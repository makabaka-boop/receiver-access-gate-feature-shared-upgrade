package grants_test

import "os"

// migrationDSN points at a database provisioned with the legacy v1 schema;
// it is opt-in so the regular suite never mutates foreign databases.
func migrationDSN() string { return os.Getenv("MIGRATION_DATABASE_URL") }
