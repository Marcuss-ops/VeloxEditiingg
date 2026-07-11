// Package contracts / testdb_helpers.go
//
// Shared Postgres test bootstrap helper for both the artifact and
// jobs contract factories. Opens a connection through
// platform/database.Open (the canonical path), ensures the per-test
// schema exists, runs migrations under pg_advisory_lock, pins
// search_path on the open connection, and returns the *database.Handle
// + a teardown func.
//
// Order of operations is deliberate and differs from the previous
// inlined factory flow: CREATE SCHEMA now runs BEFORE
// migrations.RunMigrations so the DDL emitted by the migration files
// lands in the per-test schema, not the public fallback. The historical
// ordering (migrate, then CREATE SCHEMA, then SET search_path) worked
// only because pg's `search_path=<missing_schema>,public` falls back
// to public for the first DDL — meaning tests were operating on
// public-schema objects, which is fragile during schema inspection and
// makes per-test isolation an illusion. The new order makes the
// isolation real.
//
// Postgres test isolation lives on per-test schemas (not per-test
// databases) because CREATE DATABASE in pg requires a privilege most
// CI service accounts lack. Postgres caps identifier length at 63
// bytes; the schema-name helpers further below already bound to 32
// sanitised chars so the constructed name stays well inside the cap.
package contracts

import (
	"context"
	"testing"

	"velox-server/internal/platform/database"
	"velox-server/internal/store/migrations"
)

// openPostgresForTest opens a fresh Postgres connection via
// platform/database.Open, ensures the per-test schema exists, runs
// the embedded Postgres migrations under pg_advisory_lock, pins
// search_path, and returns the Handle + cleanup func.
//
// The DSN is expected to ALREADY carry `search_path=<schema>,public`
// (see withSearchPath) so a connect-time search_path is set;
// belt-and-braces SET search_path below supersedes the connect-time
// value once the schema is real, ensuring DDL emitted by the contract
// factories themselves (and any subsequent test query) lands in the
// per-test schema.
//
// On any failure, the helper t.Fatalfs the test with the helper's
// surfaces the t.Fatal caller — cleanup is best-effort and never
// masks the real error.
func openPostgresForTest(t *testing.T, dsn, schema string) (*database.Handle, func()) {
	t.Helper()

	ctx := context.Background()
	handle, err := database.Open(ctx, database.Config{
		Driver: database.DriverPostgres,
		URL:    dsn,
	})
	if err != nil {
		t.Fatalf("openPostgresForTest: database.Open: %v", err)
	}
	db := handle.DB

	// CREATE SCHEMA before migrations so the migration files' DDL
	// lands inside <schema> rather than the public fallback. This is
	// the ordering fix vs. the historical inlined factory flow.
	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		_ = db.Close()
		t.Fatalf("openPostgresForTest: ensure schema %q: %v", schema, err)
	}
	if _, err := db.Exec("SET search_path TO " + schema + ", public"); err != nil {
		_ = db.Close()
		t.Fatalf("openPostgresForTest: set search_path %q: %v", schema, err)
	}

	if err := migrations.RunMigrations(
		db,
		migrations.PostgresMigrationsFS(),
		"postgres",
	); err != nil {
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = db.Close()
		t.Fatalf("openPostgresForTest: run migrations: %v", err)
	}

	cleanup := func() {
		// Drop the per-test schema; CASCADE handles anything migrations
		// left behind. Best-effort — failing cleanup must not mask the
		// actual test failure.
		_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = db.Close()
	}
	return handle, cleanup
}
