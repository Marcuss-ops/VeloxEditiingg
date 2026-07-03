// Package migrations / migrations_down_roundtrip_test.go
//
// Round-trip test for the paired `.down.sql` migration lifecycle.
//
// Verifies the contract on the production 068_task_requirements pair:
//
//  1. UP applies the schema_migrations row for v68 plus
//     `task_requirements` + `idx_task_requirements_task_id`.
//  2. RunDown(68) DROPs the table + index AND removes the v68
//     tracking row from schema_migrations.
//  3. Re-applying UP cleanly puts the v68 pair back, re-recording
//     the tracking row with the SAME checksum (no drift).
//
// Validation invariant for the UP -> DOWN -> UP idempotency on the
// `task_requirements` subset:
//
//	snapshot(preUP) == snapshot(postDownAndUP)
//
// Mirrors the existing testdata-based migrations_test.go pattern
// (uses openTestDB + tableExists helpers). Embeds the production
// migrations/sqlite/*.sql tree into the test binary via a parallel
// `//go:embed` directive so the test exercises the real production
// SQL rather than a synthetic copy.
package migrations

import (
	"database/sql"
	"embed"
	"testing"
)

//go:embed sqlite/*.sql
var productionSQLiteFS embed.FS

// trSnapshot captures the sqlite_master state for the v68 subset:
//
//   - task_requirements table DDL (absent => tableMissing)
//   - idx_task_requirements_task_id index DDL (absent => indexMissing)
//
// The string fields store the verbatim `sql` column from
// sqlite_master so a hash-style equality diff catches drift
// in AI-generated ALTER statements, FK re-ordering, or
// IF-NOT-EXISTS dropping. An empty string means the row was
// absent in sqlite_master at the time of the snapshot.
type trSnapshot struct {
	tableSQL string
	indexSQL string
}

func (s trSnapshot) tableMissing() bool { return s.tableSQL == "" }
func (s trSnapshot) indexMissing() bool { return s.indexSQL == "" }

// snapshotTaskRequirements queries sqlite_master for the v68
// subset. An sql.ErrNoRows from QueryRow is treated as "absent";
// any other error is fatal.
func snapshotTaskRequirements(t *testing.T, db *sql.DB) trSnapshot {
	t.Helper()
	var out trSnapshot
	var sqlText string
	if err := db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name='task_requirements'`,
	).Scan(&sqlText); err == nil {
		out.tableSQL = sqlText
	} else if err != sql.ErrNoRows {
		t.Fatalf("query sqlite_master table task_requirements: %v", err)
	}
	if err := db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_task_requirements_task_id'`,
	).Scan(&sqlText); err == nil {
		out.indexSQL = sqlText
	} else if err != sql.ErrNoRows {
		t.Fatalf("query sqlite_master index idx_task_requirements_task_id: %v", err)
	}
	// Also normalize a "no row" representation: when both fields are
	// populated, also confirm schema_migrations row absence/present
	// is observable separately.
	return out
}

// schemaMigrationPresent reports whether schema_migrations has a
// row for the given version. Used to verify RunDown's DELETE
// statement actually wiped the tracking row.
func schemaMigrationPresent(t *testing.T, db *sql.DB, version int) (present bool, checksum string) {
	t.Helper()
	var cksum string
	err := db.QueryRow(
		`SELECT checksum FROM schema_migrations WHERE version = ?`, version,
	).Scan(&cksum)
	if err == sql.ErrNoRows {
		return false, ""
	}
	if err != nil {
		t.Fatalf("query schema_migrations v%d: %v", version, err)
	}
	return true, cksum
}

// TestRunDown_TaskRequirements_RoundTrip exercises the full UP ->
// DOWN -> UP cycle on version 068 against the production migrations
// tree embedded into the test binary.
//
// This is the validation contract documented in the task_requirements
// DOWN migration: applying the DOWN after the UP must return the
// schema subset to its pre-UP state (no residue), and re-applying
// the UP must produce an identical schema (idempotency).
func TestRunDown_TaskRequirements_RoundTrip(t *testing.T) {
	db := openTestDB(t)

	// ── Phase 1: UP — apply all production migrations.
	if err := RunMigrations(db, productionSQLiteFS, "sqlite"); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}
	preUp := snapshotTaskRequirements(t, db)
	prePresent, preChecksum := schemaMigrationPresent(t, db, 68)
	if preUp.tableMissing() {
		t.Fatal("post-UP: task_requirements table expected; got absent in sqlite_master")
	}
	if preUp.indexMissing() {
		t.Fatal("post-UP: idx_task_requirements_task_id expected; got absent in sqlite_master")
	}
	if !prePresent {
		t.Fatal("post-UP: schema_migrations row for v68 expected; got absent")
	}

	// ── Phase 2: DOWN — explicit operator reversal.
	if err := RunDown(db, productionSQLiteFS, "sqlite", 68); err != nil {
		t.Fatalf("RunDown(68): %v", err)
	}
	midDown := snapshotTaskRequirements(t, db)
	if !midDown.tableMissing() {
		t.Errorf("post-DOWN: task_requirements table expected absent in sqlite_master; got %q", midDown.tableSQL)
	}
	if !midDown.indexMissing() {
		t.Errorf("post-DOWN: idx_task_requirements_task_id expected absent in sqlite_master; got %q", midDown.indexSQL)
	}
	midPresent, _ := schemaMigrationPresent(t, db, 68)
	if midPresent {
		t.Errorf("post-DOWN: schema_migrations row for v68 expected absent (RunDown should DELETE it); got present")
	}

	// ── Phase 3: UP — re-apply production migrations.
	//
	// The post-DOWN schema_migrations has no row for v68, so
	// RunMigrations will treat v68 as pending and re-apply it.
	// All other versions remain tracked and are skipped on the
	// re-run (idempotent path).
	if err := RunMigrations(db, productionSQLiteFS, "sqlite"); err != nil {
		t.Fatalf("second RunMigrations: %v", err)
	}
	postReUp := snapshotTaskRequirements(t, db)
	postPresent, postChecksum := schemaMigrationPresent(t, db, 68)
	if postReUp.tableMissing() {
		t.Fatal("post-ReUP: task_requirements table expected back; got absent")
	}
	if postReUp.indexMissing() {
		t.Fatal("post-ReUP: idx_task_requirements_task_id expected back; got absent")
	}
	if !postPresent {
		t.Fatal("post-ReUP: schema_migrations row for v68 expected; got absent")
	}

	// ── INVARIANT: UP -> DOWN -> UP is idempotent on the task_requirements subset.
	if preUp.tableSQL != postReUp.tableSQL {
		t.Errorf(
			"UP -> DOWN -> UP NOT idempotent on task_requirements table\nbefore: %s\nafter:  %s",
			preUp.tableSQL, postReUp.tableSQL,
		)
	}
	if preUp.indexSQL != postReUp.indexSQL {
		t.Errorf(
			"UP -> DOWN -> UP NOT idempotent on idx_task_requirements_task_id\nbefore: %s\nafter:  %s",
			preUp.indexSQL, postReUp.indexSQL,
		)
	}

	// Bonus invariant: the schema_migrations checksum for v68 is
	// stable across the round-trip (it depends only on the file
	// content, NOT on the apply history). The Go runner computes
	// SHA256(SQL) once and uses it as both the integrity check
	// and the schema_migrations row's checksum column. If the
	// checksum changes between the two runs the file was modified.
	if preChecksum != postChecksum {
		t.Errorf(
			"v68 checksum unstable across UP -> DOWN -> UP\nfirst:  %s\nsecond: %s",
			preChecksum, postChecksum,
		)
	}

	// Bonus invariant: RunDown is idempotent on the no-op path.
	// A second RunDown(68) on the post-ReUP state must succeed
	// (statements are IF-EXISTS / IF-EXISTS), must leave the
	// schema absent, and must report v68 absent in
	// schema_migrations.
	if err := RunDown(db, productionSQLiteFS, "sqlite", 68); err != nil {
		t.Fatalf("idempotent RunDown(68) on re-applied state: %v", err)
	}
	finalDown := snapshotTaskRequirements(t, db)
	if !finalDown.tableMissing() {
		t.Errorf("idempotent RunDown: task_requirements should still be absent after second RunDown")
	}
	finalPresent, _ := schemaMigrationPresent(t, db, 68)
	if finalPresent {
		t.Errorf("idempotent RunDown: v68 schema_migrations row still missing after second delete")
	}
}
