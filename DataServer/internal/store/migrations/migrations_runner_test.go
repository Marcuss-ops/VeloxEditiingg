package migrations

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	// Append `_busy_timeout=5000` so concurrent goroutines don't
	// immediately trip SQLITE_BUSY when the migration-runner takes the writer lock.
	// Canonical DSN pattern for in-memory shared test DBs across
	// DataServer/internal/store/*_test.go is
	//   file:<name>?mode=memory&cache=shared&_busy_timeout=5000
	// `cache=shared` lets sibling goroutines share the same in-memory db, but
	// creates writer/reader contention: while the migration-runner holds the
	// writer lock, a concurrent reader blocks; without `_busy_timeout=5000`
	// that reader returns SQLITE_BUSY immediately instead of waiting it out.
	// Keep both flags together when copying this DSN into new tests.
	// This helper is the single source of truth for the migrations-package
	// tests.
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=5000", t.Name()))
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Always enable FK enforcement for testing
	_, _ = db.Exec("PRAGMA foreign_keys = ON")
	return db
}

// ============================================================
// RunMigrations integration tests
// ============================================================

func TestRunMigrations_FullLifecycle(t *testing.T) {
	db := openTestDB(t)

	// First run: all migrations should be applied
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}

	// Discover expected migration count
	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
	expectedCount := len(expectedMigs)

	// Verify schema_migrations has the expected number of entries
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations count: %v", err)
	}
	if count != expectedCount {
		t.Fatalf("expected %d schema_migrations entries, got %d", expectedCount, count)
	}

	// Verify each version has name, checksum, and applied_at
	rows, err := db.Query(`SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	defer rows.Close()

	migIdx := 0
	for rows.Next() {
		var v int
		var name, checksum, appliedAt string
		if err := rows.Scan(&v, &name, &checksum, &appliedAt); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		if v != expectedMigs[migIdx].Version {
			t.Errorf("row %d version: got %d, want %d", migIdx, v, expectedMigs[migIdx].Version)
		}
		if name == "" {
			t.Errorf("version %d: empty name", v)
		}
		if checksum == "" {
			t.Errorf("version %d: empty checksum", v)
		}
		if appliedAt == "" {
			t.Errorf("version %d: empty applied_at", v)
		}
		migIdx++
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	db := openTestDB(t)

	// Discover expected count
	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
	expectedCount := len(expectedMigs)

	// Run twice
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("second RunMigrations (idempotent) failed: %v", err)
	}

	// Should still have the expected number of entries
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Errorf("expected %d entries after idempotent run, got %d", expectedCount, count)
	}
}

func TestRunMigrations_ChecksumMismatch(t *testing.T) {
	db := openTestDB(t)

	// Apply migrations normally first
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}

	// Tamper with the checksum in schema_migrations
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 3`); err != nil {
		t.Fatalf("tamper checksum: %v", err)
	}

	// Second run should fail with checksum mismatch
	err := RunMigrations(db, testMigrationsFS, "testdata")
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}

// ============================================================
// applyMigration per-statement tolerance tests
// ============================================================

// TestApplyMigration_DuplicateColumnTolerated verifies the "duplicate
// column name" tolerance introduced in applyMigration for ALTER TABLE
// ADD COLUMN statements. The tolerance unblocks the Path B rollout
// path for any pre-Path-B production DB that already applied the
// legacy migrations/039_add_job_required_resource_columns.sql against
// its jobs table: when the renamed sibling
// 045_add_job_required_resource_columns.sql (on the recursive
// migrations/sqlite/ embed track) replays the ALTER TABLE ADD COLUMN
// on next boot through SQLiteMigrationsFS(), the duplicate-column
// error is treated as a no-op and the schema_migrations entry is
// recorded instead of aborting the boot.
func TestApplyMigration_DuplicateColumnTolerated(t *testing.T) {
	db := openTestDB(t)

	// applyMigration itself does NOT call EnsureSchemaTable — that's
	// the caller's responsibility (RunMigrations / EnsureApplied both
	// do it before delegating). For a direct unit test of the
	// duplicate-column tolerance pathway we must pre-establish the
	// tracking table ourselves.
	if err := EnsureSchemaTable(db); err != nil {
		t.Fatalf("ensure schema_migrations table: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE jobs (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create jobs table: %v", err)
	}

	mig := Migration{
		Version:  901,
		Name:     "test_dup_col",
		SQL:      "ALTER TABLE jobs ADD COLUMN foo TEXT NOT NULL DEFAULT '';",
		Checksum: "checksum_add_col",
	}

	if err := applyMigration(db, mig); err != nil {
		t.Fatalf("first applyMigration (add column) failed: %v", err)
	}

	// Simulate the Path B rollout: production DBs have the legacy
	// 039 record for the column additions. Force a replay by
	// clearing the schema_migrations entry; applyMigration then
	// re-executes the SQL and trips the duplicate-column tolerance.
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = ?`, mig.Version); err != nil {
		t.Fatalf("clear schema_migrations entry for replay: %v", err)
	}

	if err := applyMigration(db, mig); err != nil {
		t.Fatalf("second applyMigration should tolerate 'duplicate column name: foo', got: %v", err)
	}

	// The ALTER TABLE was silently skipped by the tolerance, so the
	// column still exists exactly once.
	var colCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name = 'foo'`,
	).Scan(&colCount); err != nil {
		t.Fatalf("pragma_table_info(jobs): %v", err)
	}
	if colCount != 1 {
		t.Errorf("expected foo column to exist exactly once after replay, got count=%d", colCount)
	}

	// schema_migrations entry is re-recorded by the second apply
	// (without the tolerance, the surrounding tx would have rolled
	// back and the entry would still be missing).
	var recordedChecksum string
	if err := db.QueryRow(
		`SELECT checksum FROM schema_migrations WHERE version = ?`,
		mig.Version,
	).Scan(&recordedChecksum); err != nil {
		t.Fatalf("schema_migrations lookup after replay: %v", err)
	}
	if recordedChecksum != mig.Checksum {
		t.Errorf("expected checksum %q after replay, got %q", mig.Checksum, recordedChecksum)
	}
}

// ============================================================
// AppliedVersions / PendingVersions tests
// ============================================================

func TestAppliedVersions(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
	expectedCount := len(expectedMigs)

	versions, err := AppliedVersions(db)
	if err != nil {
		t.Fatalf("AppliedVersions failed: %v", err)
	}

	if len(versions) != expectedCount {
		t.Fatalf("expected %d applied versions, got %d", expectedCount, len(versions))
	}

	for i, v := range versions {
		if v != expectedMigs[i].Version {
			t.Errorf("versions[%d]: got %d, want %d", i, v, expectedMigs[i].Version)
		}
	}
}

func TestPendingVersions(t *testing.T) {
	db := openTestDB(t)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
	expectedCount := len(expectedMigs)

	// ensureSchemaTable is needed before PendingVersions can query schema_migrations
	if err := ensureSchemaTable(db); err != nil {
		t.Fatalf("ensureSchemaTable failed: %v", err)
	}

	// Before applying anything, all migrations are pending
	pending, err := PendingVersions(db, testMigrationsFS, "testdata")
	if err != nil {
		t.Fatalf("PendingVersions failed: %v", err)
	}
	if len(pending) != expectedCount {
		t.Fatalf("expected %d pending migrations, got %d", expectedCount, len(pending))
	}

	// After applying, none should be pending
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}

	pending, err = PendingVersions(db, testMigrationsFS, "testdata")
	if err != nil {
		t.Fatalf("PendingVersions after apply failed: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after apply, got %d", len(pending))
	}
}

// ============================================================
// Helpers
// ============================================================

// ensureSchemaTable is a thin wrapper around migrations.EnsureSchemaTable
// exposed at package scope for tests that need to query schema_migrations
// before RunMigrations has populated it. RunMigrations itself calls
// EnsureSchemaTable internally, so this helper is only needed when a test
// specifically wants to inspect pending vs applied state on an empty DB.
func ensureSchemaTable(db *sql.DB) error {
	return EnsureSchemaTable(db)
}

func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
}
