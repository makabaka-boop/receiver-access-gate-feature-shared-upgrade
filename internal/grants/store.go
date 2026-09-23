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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ModeShared    = "SHARED"
	ModeExclusive = "EXCLUSIVE"

	StatusActive   = "ACTIVE"
	StatusReleased = "RELEASED"
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
		if _, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS grants (
    seq          BIGSERIAL PRIMARY KEY,
    id           TEXT        NOT NULL UNIQUE,
    receiver     TEXT        NOT NULL,
    mode         TEXT        NOT NULL CHECK (mode IN ('SHARED', 'EXCLUSIVE')),
    status       TEXT        NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED')),
    token_digest BYTEA       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at  TIMESTAMPTZ
)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS grants_receiver_seq ON grants (receiver, seq)`)
		return err
	})
}

// CreateGrant atomically checks for conflicting active grants on receiver
// and inserts a new ACTIVE grant. It returns the grant plus the owner
// token, which is shown to the caller exactly once; only its SHA-256
// digest is persisted.
func (s *Store) CreateGrant(ctx context.Context, receiver, mode string) (Grant, string, error) {
	var g Grant
	var token string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Serialize grant decisions per receiver across all API processes.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, receiver); err != nil {
			return err
		}
		// A SHARED request conflicts with any active EXCLUSIVE grant; an
		// EXCLUSIVE request conflicts with any active grant at all.
		var conflicts int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM grants
WHERE receiver = $1
  AND status = 'ACTIVE'
  AND ($2 = 'EXCLUSIVE' OR mode = 'EXCLUSIVE')`,
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

// ReleaseGrant transitions an ACTIVE grant to RELEASED when presented with
// its owner token. A wrong token yields ErrForbidden and leaves both the
// record and the receiver's active set untouched.
func (s *Store) ReleaseGrant(ctx context.Context, id, token string) (Grant, error) {
	var g Grant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var digest []byte
		err := tx.QueryRow(ctx, `
SELECT id, receiver, mode, status, token_digest
FROM grants WHERE id = $1
FOR UPDATE`, id).
			Scan(&g.ID, &g.Receiver, &g.Mode, &g.Status, &digest)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		sum := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(sum[:], digest) != 1 {
			return ErrForbidden // rollback: record and active set unchanged
		}
		if g.Status == StatusActive {
			if _, err := tx.Exec(ctx, `
UPDATE grants SET status = 'RELEASED', released_at = now()
WHERE id = $1`, id); err != nil {
				return err
			}
			g.Status = StatusReleased
		}
		return nil
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
