// Package grants implements the receiver access gate: a PostgreSQL-backed
// grant store and the HTTP API shared by any number of API processes.
package grants

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ModeShared    = "SHARED"
	ModeExclusive = "EXCLUSIVE"

	StatusActive         = "ACTIVE"
	StatusReleased       = "RELEASED"
	StatusUpgradePending = "UPGRADE_PENDING"
)

var (
	// ErrBusy reports a conflicting active grant on the receiver. The
	// transaction that produced it is always rolled back, so a busy
	// attempt never leaves a record behind.
	ErrBusy = errors.New("busy")
	// ErrForbidden reports an owner-token mismatch. Nothing is modified.
	ErrForbidden = errors.New("forbidden")
	// ErrNotFound reports an unknown grant identifier.
	ErrNotFound = errors.New("not found")
	// ErrNotUpgradable reports an upgrade attempt against a grant that can
	// never be upgraded: it has already been released, or it is a native
	// EXCLUSIVE grant. Nothing is modified.
	ErrNotUpgradable = errors.New("not upgradable")
	// ErrAlreadyUpgrading reports that the receiver already has a different
	// grant waiting to be upgraded; at most one such waiter is allowed per
	// receiver. Nothing is modified.
	ErrAlreadyUpgrading = errors.New("already upgrading")
)

// Grant is the public view of one access grant. It never carries the owner
// token or its digest.
type Grant struct {
	ID       string `json:"grant_id"`
	Receiver string `json:"receiver"`
	Mode     string `json:"mode"`
	Status   string `json:"status"`
}

// Store persists grants in PostgreSQL. All mutating operations run inside
// a single transaction that first takes a per-receiver advisory lock, so
// the conflict check and the write are atomic across every API process
// connected to the same database.
type Store struct {
	pool *pgxpool.Pool
}

// migrateLockID serializes schema setup between concurrently starting
// API processes.
const migrateLockID int64 = 0x67616E7467617465 // "grantgate"

func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrateLockID); err != nil {
			return err
		}
		// Fresh installs get the current shape directly. The status CHECK is
		// attached by name afterwards so old and new schemas converge on the
		// same constraint below.
		if _, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS grants (
    seq          BIGSERIAL PRIMARY KEY,
    id           TEXT        NOT NULL UNIQUE,
    receiver     TEXT        NOT NULL,
    mode         TEXT        NOT NULL CHECK (mode IN ('SHARED', 'EXCLUSIVE')),
    status       TEXT        NOT NULL,
    token_digest BYTEA       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at  TIMESTAMPTZ,
    upgraded_at  TIMESTAMPTZ
)`); err != nil {
			return err
		}
		// Backfill databases created before the upgrade feature. Adding the
		// column is a no-op on current schemas; existing rows get NULL, which
		// correctly marks them as never promoted.
		if _, err := tx.Exec(ctx,
			`ALTER TABLE grants ADD COLUMN IF NOT EXISTS upgraded_at TIMESTAMPTZ`); err != nil {
			return err
		}
		// Widen the status domain to admit UPGRADE_PENDING. The original
		// column-level CHECK was auto-named grants_status_check; drop it and
		// install the widened constraint by the same name so migration is
		// idempotent across restarts and old/new binaries alike.
		if _, err := tx.Exec(ctx,
			`ALTER TABLE grants DROP CONSTRAINT IF EXISTS grants_status_check`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
ALTER TABLE grants ADD CONSTRAINT grants_status_check
CHECK (status IN ('ACTIVE', 'RELEASED', 'UPGRADE_PENDING'))`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS grants_receiver_seq ON grants (receiver, seq)`)
		return err
	})
}

// lockReceiver serializes every decision for one receiver across all API
// processes sharing the database. Callers must already know the receiver.
func lockReceiver(ctx context.Context, tx pgx.Tx, receiver string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, receiver)
	return err
}

// CreateGrant atomically checks for conflicting grants on receiver and
// inserts a new ACTIVE grant. It returns the grant plus the owner token,
// which is shown to the caller exactly once; only its SHA-256 digest is
// persisted.
//
// A UPGRADE_PENDING grant acts as an admission barrier: while an upgrade
// is queued, neither new SHARED nor new EXCLUSIVE requests are admitted.
func (s *Store) CreateGrant(ctx context.Context, receiver, mode string) (Grant, string, error) {
	var g Grant
	var token string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Serialize grant decisions per receiver across all API processes.
		if err := lockReceiver(ctx, tx, receiver); err != nil {
			return err
		}
		// A SHARED request conflicts with an active EXCLUSIVE grant and with
		// any queued upgrade; an EXCLUSIVE request conflicts with any active
		// grant at all, as well as a queued upgrade.
		var conflicts int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM grants
WHERE receiver = $1
  AND (status = 'UPGRADE_PENDING'
       OR (status = 'ACTIVE'
           AND ($2 = 'EXCLUSIVE' OR mode = 'EXCLUSIVE')))`,
			receiver, mode).Scan(&conflicts); err != nil {
			return err
		}
		if conflicts > 0 {
			return ErrBusy // rollback: no record is left behind
		}

		id, err := newID()
		if err != nil {
			return err
		}
		tok, digest, err := newToken()
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
INSERT INTO grants (id, receiver, mode, status, token_digest)
VALUES ($1, $2, $3, 'ACTIVE', $4)
RETURNING id, receiver, mode, status`,
			id, receiver, mode, digest).
			Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status); err != nil {
			return err
		}
		token = tok
		return nil
	})
	if err != nil {
		return Grant{}, "", err
	}
	return g, token, nil
}

// ListGrants returns every grant for receiver in stable insertion order.
func (s *Store) ListGrants(ctx context.Context, receiver string) ([]Grant, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id, receiver, mode, status FROM grants
WHERE receiver = $1
ORDER BY seq`, receiver)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Grant, error) {
		var g Grant
		err := row.Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status)
		return g, err
	})
}

// loadRow is the mutable state of a grant plus enough metadata to
// authenticate its owner and locate its receiver lock.
type loadRow struct {
	id         string
	receiver   string
	mode       string
	status     string
	upgradedAt *time.Time
	digest     []byte
}

func loadGrantRow(ctx context.Context, q pgx.Tx, id string, forUpdate bool) (loadRow, error) {
	var r loadRow
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	err := q.QueryRow(ctx, `
SELECT id, receiver, mode, status, upgraded_at, token_digest
FROM grants WHERE id = $1`+lock, id).
		Scan(&r.id, &r.receiver, &r.mode, &r.status, &r.upgradedAt, &r.digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return loadRow{}, ErrNotFound
	}
	if err != nil {
		return loadRow{}, err
	}
	return r, nil
}

func tokenMatches(digest []byte, token string) bool {
	sum := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(sum[:], digest) == 1
}

// UpgradeGrant atomically converts an ACTIVE SHARED grant into an exclusive
// grant without changing its owner token:
//
//   - If no other ACTIVE SHARED grant remains, the record is promoted
//     in place to ACTIVE EXCLUSIVE and returned.
//   - Otherwise it becomes UPGRADE_PENDING: the receiver then rejects all
//     new SHARED/EXCLUSIVE applications, and the last competing shared
//     grant to be released promotes it automatically (see ReleaseGrant).
//
// At most one grant per receiver may be UPGRADE_PENDING. The call is
// idempotent for the same (grant, token): replaying it while the grant is
// still waiting, or after it has been promoted, returns the current grant
// without changing the set. A wrong token yields ErrForbidden; a released
// grant, a native EXCLUSIVE grant and a second upgrade request on the same
// receiver yield stable errors and modify nothing.
func (s *Store) UpgradeGrant(ctx context.Context, id, token string) (Grant, error) {
	var g Grant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Authenticate before touching the receiver lock so a forged token
		// is rejected regardless of the record's current state.
		row, err := loadGrantRow(ctx, tx, id, false)
		if err != nil {
			return err
		}
		if !tokenMatches(row.digest, token) {
			return ErrForbidden // rollback: nothing changes
		}

		// All checks and writes for this receiver are serialized.
		if err := lockReceiver(ctx, tx, row.receiver); err != nil {
			return err
		}
		cur, err := loadGrantRow(ctx, tx, id, true)
		if err != nil {
			return err
		}

		switch {
		case cur.status == StatusReleased:
			// An upgrade cannot be (re)started on a released record.
			return ErrNotUpgradable
		case cur.mode == ModeExclusive:
			// A promoted grant (upgraded_at set) is the terminal result of a
			// prior upgrade: replay is idempotent. A native EXCLUSIVE grant
			// can never be upgraded.
			if cur.upgradedAt != nil {
				g = Grant{ID: cur.id, Receiver: cur.receiver, Mode: cur.mode, Status: cur.status}
				return nil
			}
			return ErrNotUpgradable
		case cur.status == StatusUpgradePending:
			// Repeated upgrade of the same waiting grant: idempotent wait.
			g = Grant{ID: cur.id, Receiver: cur.receiver, Mode: cur.mode, Status: cur.status}
			return nil
		}

		// cur is ACTIVE SHARED. Only one waiter per receiver.
		var otherWaiters int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM grants
WHERE receiver = $1 AND id <> $2 AND status = 'UPGRADE_PENDING'`,
			cur.receiver, cur.id).Scan(&otherWaiters); err != nil {
			return err
		}
		if otherWaiters > 0 {
			return ErrAlreadyUpgrading
		}

		var rivals int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM grants
WHERE receiver = $1 AND id <> $2 AND status = 'ACTIVE' AND mode = 'SHARED'`,
			cur.receiver, cur.id).Scan(&rivals); err != nil {
			return err
		}

		if rivals == 0 {
			// Last shared holder standing: promote in place immediately.
			if err := tx.QueryRow(ctx, `
UPDATE grants SET status = 'ACTIVE', mode = 'EXCLUSIVE', upgraded_at = now()
WHERE id = $1
RETURNING id, receiver, mode, status`,
				cur.id).Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status); err != nil {
				return err
			}
			return nil
		}

		// Competitors remain: queue as the single pending upgrade and start
		// rejecting fresh admissions until promotion or cancellation.
		if err := tx.QueryRow(ctx, `
UPDATE grants SET status = 'UPGRADE_PENDING'
WHERE id = $1
RETURNING id, receiver, mode, status`,
			cur.id).Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return g, nil
}

// ReleaseGrant transitions an ACTIVE (or UPGRADE_PENDING) grant to
// RELEASED when presented with its owner token. A wrong token yields
// ErrForbidden and leaves both the record and the receiver's active set
// untouched.
//
// When a competing SHARED grant is released, the same transaction checks
// the receiver for a queued upgrade; once no ACTIVE SHARED rival remains it
// promotes the waiting grant to ACTIVE EXCLUSIVE atomically, so no query
// can observe an intermediate state. Releasing the UPGRADE_PENDING grant
// itself cancels the upgrade (the record is simply RELEASED) and normal
// admission resumes.
func (s *Store) ReleaseGrant(ctx context.Context, id, token string) (Grant, error) {
	var g Grant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Locate and authenticate before taking the receiver lock.
		row, err := loadGrantRow(ctx, tx, id, false)
		if err != nil {
			return err
		}
		if !tokenMatches(row.digest, token) {
			return ErrForbidden // rollback: record and active set unchanged
		}

		if err := lockReceiver(ctx, tx, row.receiver); err != nil {
			return err
		}
		cur, err := loadGrantRow(ctx, tx, id, true)
		if err != nil {
			return err
		}

		// Releasing an already-released grant is an idempotent no-op.
		if cur.status == StatusReleased {
			g = Grant{ID: cur.id, Receiver: cur.receiver, Mode: cur.mode, Status: cur.status}
			return nil
		}

		wasPending := cur.status == StatusUpgradePending
		if err := tx.QueryRow(ctx, `
UPDATE grants SET status = 'RELEASED', released_at = now()
WHERE id = $1
RETURNING id, receiver, mode, status`,
			cur.id).Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status); err != nil {
			return err
		}

		// If the waiter itself left, the upgrade is cancelled and admission
		// reopens; there is nothing to promote.
		if wasPending {
			return nil
		}

		// A rival left: promote the queued upgrade exactly when it is the
		// last competitor. Both the release and promotion commit together.
		var promotedID string
		err = tx.QueryRow(ctx, `
UPDATE grants SET status = 'ACTIVE', mode = 'EXCLUSIVE', upgraded_at = now()
WHERE id = (
    SELECT w.id FROM grants w
    WHERE w.receiver = $1 AND w.status = 'UPGRADE_PENDING'
      AND NOT EXISTS (
          SELECT 1 FROM grants o
          WHERE o.receiver = $1 AND o.status = 'ACTIVE' AND o.mode = 'SHARED')
    ORDER BY w.seq
    LIMIT 1)
RETURNING id`, cur.receiver).Scan(&promotedID)
		if errors.Is(err, pgx.ErrNoRows) {
			// No waiter, or rivals still hold the receiver.
			return nil
		}
		return err
	})
	if err != nil {
		return Grant{}, err
	}
	return g, nil
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func newToken() (token string, digest []byte, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", nil, err
	}
	token = base64.RawURLEncoding.EncodeToString(b)
	sum := sha256.Sum256([]byte(token))
	return token, sum[:], nil
}
