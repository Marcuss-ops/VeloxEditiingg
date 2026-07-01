// Package migrations / sqlite/063_task_specs_required_outputs_test.go
//
// Per-migration unit tests for 063_task_specs_required_outputs.sql.
// Coverage:
//   - The required_outputs_json column is added to task_specs after
//     applying 063.
//   - Pre-existing task_specs rows get the DEFAULT '[]' value
//     (round-trip byte-identical).
//   - New rows can store non-empty JSON arrays.
//   - The production applyMigration replay path tolerates the
//     "duplicate column name" surface (Path-B recovery scenario).
//   - PRAGMA integrity_check returns "ok".
package migrations

import (
	"testing"

	// Required for //go:embed parsing.
	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/063_task_specs_required_outputs.sql
var sqliteSQL063 string

// TestMigration063_ColumnAdded asserts the new column lands on a
// representative task_specs shape. The production schema for
// task_specs is built by migration 040; this test creates a
// representative shape so the ALTER TABLE has a target table.
func TestMigration063_ColumnAdded(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL063)

	if !columnExists(t, db, "task_specs", "required_outputs_json") {
		t.Fatal("required_outputs_json column missing after migration 063")
	}
}

// TestMigration063_DefaultOnExistingRows asserts the column DEFAULT
// '[]' kicks in for pre-existing rows. The plan's "existing
// Fabrizio_clips-style specs untouched" acceptance criterion maps
// to this property: a byte-identical round-trip means the new
// column adds zero surface to existing rows.
func TestMigration063_DefaultOnExistingRows(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs: %v", err)
	}
	now := "2024-01-01T00:00:00Z"
	// Insert two rows BEFORE applying 063, so the ALTER TABLE
	// migration's DEFAULT '[]' must kick in for both existing rows.
	for _, id := range []string{"ts-old-1", "ts-old-2"} {
		if _, err := db.Exec(
			`INSERT INTO task_specs (task_id, executor_id, payload_json, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?)`,
			id, "exec.x", "{}", now, now,
		); err != nil {
			t.Fatalf("insert pre-063 task_specs: %v", err)
		}
	}

	applyMigrationSQL(t, db, sqliteSQL063)

	rows, err := db.Query(
		`SELECT task_id, required_outputs_json FROM task_specs ORDER BY task_id`,
	)
	if err != nil {
		t.Fatalf("select task_specs after migration: %v", err)
	}
	defer rows.Close()

	seen := make(map[string]string)
	for rows.Next() {
		var id, val string
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatalf("scan task_spec row: %v", err)
		}
		seen[id] = val
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration err: %v", err)
	}
	for _, id := range []string{"ts-old-1", "ts-old-2"} {
		if seen[id] != "[]" {
			t.Errorf("existing task_specs row %q required_outputs_json: got %q, want []", id, seen[id])
		}
	}
}

// TestMigration063_AcceptsNonEmpty asserts new rows can store a
// non-empty JSON array. The Phase 2+ creatorflow.RenderPlan code
// path will author these payloads; the test guards the column
// shape against a future schema change that would re-introduce a
// length or format constraint at the SQL layer.
func TestMigration063_AcceptsNonEmpty(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL063)

	now := "2024-01-01T00:00:00Z"
	payload := `[{"kind":"final_video","mime_type":"video/mp4","min_count":1,"max_count":1}]`
	if _, err := db.Exec(
		`INSERT INTO task_specs
		    (task_id, executor_id, payload_json, required_outputs_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"ts-new", "exec.x", "{}", payload, now, now,
	); err != nil {
		t.Fatalf("insert task_specs with non-empty required_outputs_json: %v", err)
	}

	var got string
	if err := db.QueryRow(
		`SELECT required_outputs_json FROM task_specs WHERE task_id = ?`, "ts-new",
	).Scan(&got); err != nil {
		t.Fatalf("select new row: %v", err)
	}
	if got != payload {
		t.Errorf("new row required_outputs_json: got %q, want %q", got, payload)
	}
}

// TestMigration063_AddColumnTolerance drives the production
// applyMigration path directly so the test exercises the
// "duplicate column name" tolerance at migrations.go rather than
// just the raw SQLite driver error string. Models the Path-B
// recovery scenario: a partial boot committed the ALTER TABLE but
// not the schema_migrations INSERT, and the next boot must re-run
// the migration cleanly.
func TestMigration063_AddColumnTolerance(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs: %v", err)
	}

	// Production applyMigration path: compute the same SHA256 hex
	// for the SQL content that production produces on discover.
	checksum := checksumHex(sqliteSQL063)
	mig := Migration{
		Version:  63,
		Name:     "task_specs_required_outputs",
		SQL:      sqliteSQL063,
		Checksum: checksum,
	}

	if err := EnsureSchemaTable(db); err != nil {
		t.Fatalf("ensure schema_migrations table: %v", err)
	}
	if err := applyMigration(db, mig); err != nil {
		t.Fatalf("first applyMigration (fresh DB, no replay): %v", err)
	}

	// Path-B scenario: a partial boot committed the ALTER TABLE but
	// not the schema_migrations INSERT. Force-clear the row and re-run;
	// the production tolerance must swallow the duplicate-column error
	// and re-record schema_migrations.
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = 63`); err != nil {
		t.Fatalf("clear schema_migrations row for replay: %v", err)
	}
	if err := applyMigration(db, mig); err != nil {
		t.Fatalf("replay applyMigration should tolerate 'duplicate column name', got: %v", err)
	}

	// Column still exists exactly once.
	var colCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('task_specs') WHERE name='required_outputs_json'`,
	).Scan(&colCount); err != nil {
		t.Fatalf("pragma_table_info(task_specs): %v", err)
	}
	if colCount != 1 {
		t.Errorf("expected exactly 1 column 'required_outputs_json' after replay, got %d", colCount)
	}

	// schema_migrations row is re-recorded by the second apply. Without
	// the duplicate-column tolerance, the surrounding tx would have
	// rolled back and the entry would still be missing.
	var recordedChecksum string
	if err := db.QueryRow(
		`SELECT checksum FROM schema_migrations WHERE version = 63`,
	).Scan(&recordedChecksum); err != nil {
		t.Fatalf("schema_migrations lookup after replay: %v", err)
	}
	if recordedChecksum != checksum {
		t.Errorf("expected checksum %q after replay, got %q", checksum, recordedChecksum)
	}
}

// TestMigration063_IntegrityOK runs PRAGMA integrity_check after the
// ALTER TABLE applies. Any schema-level corruption (orphaned pages,
// missing btree entries, mismatched rowids) surfaces here.
func TestMigration063_IntegrityOK(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL063)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}
