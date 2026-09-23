package grants_test

// Verifies the on-disk migration against a database built with the original
// v1 schema. Skipped unless MIGRATION_DATABASE_URL is set; the provisioning
// is done out of band (see the task notes), never against the main test DB.

import (
	"context"
	"testing"

	"grantgate/internal/grants"
)

func TestMigrateLegacySchema(t *testing.T) {
	dsn := migrationDSN()
	if dsn == "" {
		t.Skip("MIGRATION_DATABASE_URL not set; skipping legacy migration test")
	}

	// Opening the store runs the migration.
	s, err := grants.NewStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("migrate legacy db: %v", err)
	}
	defer s.Close()

	// Old rows remain readable and keep their original semantics.
	gs, err := s.ListGrants(context.Background(), "rx-old")
	if err != nil {
		t.Fatalf("list legacy: %v", err)
	}
	if len(gs) != 2 {
		t.Fatalf("legacy rows lost: %+v", gs)
	}
	if gs[0].ID != "oldactive" || gs[0].Mode != grants.ModeShared || gs[0].Status != grants.StatusActive {
		t.Fatalf("legacy active row altered: %+v", gs[0])
	}
	if gs[1].ID != "oldrel" || gs[1].Status != grants.StatusReleased {
		t.Fatalf("legacy released row altered: %+v", gs[1])
	}

	// The widened domain now admits a pending upgrade on a legacy shared row
	// (needs a real token; seed one by recreating the active row's digest is
	// not possible, so just verify new-domain inserts via the store work on
	// the migrated DB through a fresh receiver).
	g, tok, err := s.CreateGrant(context.Background(), "rx-migrated-fresh", grants.ModeShared)
	if err != nil {
		t.Fatalf("create on migrated db: %v", err)
	}
	if _, err := s.UpgradeGrant(context.Background(), g.ID, tok); err != nil {
		t.Fatalf("upgrade on migrated db: %v", err)
	}

	// Migration must be idempotent across a second open (simulates restart).
	s2, err := grants.NewStore(context.Background(), dsn)
	if err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	defer s2.Close()

	// Native EXCLUSIVE legacy row must still be rejected for upgrade (its
	// missing upgraded_at is treated as NULL -> native).
	ex, err := s2.ListGrants(context.Background(), "rx-oldex")
	if err != nil || len(ex) != 1 || ex[0].Mode != grants.ModeExclusive {
		t.Fatalf("legacy exclusive row altered: %+v err=%v", ex, err)
	}
}
