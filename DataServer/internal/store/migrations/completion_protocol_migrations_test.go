package migrations

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	// Required for the //go:embed directives below. Variables are
	// typed as string (single-file embeds), so the embed package is
	// referenced only at compile time via the directive parser.
	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

// Embed the raw SQL content of the three new migration files. The
// production boot path picks them up automatically through the
// //go:embed sqlite/*.sql pattern in runner.go; tests load the same
// files directly via //go:embed rather than the production discovery
// path so the migration tests do not pay the cost of running ALL 63
// migrations on every invocation.
//
//go:embed sqlite/061_attempt_commits.sql
var sql061AttemptCommits string

//go:embed sqlite/062_task_output_declarations.sql
var sql062TaskOutputDeclarations string

//go:embed sqlite/063_task_specs_required_outputs.sql
var sql063TaskSpecsRequiredOutputs string

// applyMigrationSQL runs the contents of a migration-file-style SQL
// string on a fresh in-memory DB. Mirrors the production runner's
// splitStatements() + per-statement execution pattern so tests catch
// the same statement-level error semantics. Returns the first
// non-nil error so a single test failure is attributable to the
// specific statement that broke.
func applyMigrationSQL(t *testing.T, db *sql.DB, content string) {
	t.Helper()
	for _, stmt := range splitStatements(content) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("apply statement %q: %v", trimForLog(stmt), err)
		}
	}
}

// trimForLog trims a SQL statement down for inclusion in log/assert
// messages; the trimmed string is 80 chars max so multi-statement
// splits do not flood the failure output.
func trimForLog(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 80 {
		s = s[:80] + "..."
	}
	return s
}

// ================================================================
// Integration: 061 + 062 + 063 applied in order on a fresh DB
// ================================================================

func TestCompletionProtocol_ApplyAllThreeInOrderOnFreshDB(t *testing.T) {
	db := openTestDB(t)

	// Each migration is applied directly via applyMigrationSQL. The
	// production RunMigrations path covers the same statements via the
	// //go:embed sqlite/*.sql directive in runner.go, so a green here
	// proves the per-statement semantics match production.
	applyMigrationSQL(t, db, sql061AttemptCommits)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	// Migration 063 ADDs a column to task_specs; on a fresh DB the
	// table doesn't exist yet, so we pre-create a representative shape.
	if _, err := db.Exec(`
		CREATE TABLE task_specs (
			task_id TEXT PRIMARY KEY,
			executor_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("pre-create task_specs in fresh-DB integration: %v", err)
	}
	applyMigrationSQL(t, db, sql063TaskSpecsRequiredOutputs)

	// All three tables now exist.
	for _, table := range []string{
		"attempt_commits",
		"task_output_declarations",
		"task_specs",
	} {
		if !tableExists(t, db, table) {
			t.Errorf("fresh-DB apply: %s missing", table)
		}
	}

	// task_specs has the new column.
	if !columnExists(t, db, "task_specs", "required_outputs_json") {
		t.Error("task_specs.required_outputs_json missing after apply-all")
	}

	// Full integrity check across the schema.
	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}

	// End-to-end: insert an attempt_commits row, link a declaration
	// to it via commit_id, then verify the FK-light join matches. We
	// do NOT add a foreign key constraint on task_output_declarations
	// .commit_id intentionally: the master enforces the relationship
	// at the application layer, and adding FKs here would break the
	// tolerant ALTER TABLE apply path for legacy imports — so just
	// assert the columns are joined-able.
	now := "2024-01-01T00:00:00Z"
	if _, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"c-integ", "t-integ", "a-integ", "j-integ", "w-integ", "l-integ",
		1, "DECLARED", 1, "h-integ", now, now, now, now,
	); err != nil {
		t.Fatalf("insert attempt_commits in integration: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO task_output_declarations (
			declaration_id, commit_id, task_id, attempt_id,
			output_kind, logical_name, mime_type,
			expected_size_bytes, expected_sha256,
			status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"d-integ", "c-integ", "t-integ", "a-integ",
		"final_video", "out.mp4", "video/mp4",
		100, "sha-integ", "DECLARED", now, now,
	); err != nil {
		t.Fatalf("insert task_output_declarations in integration: %v", err)
	}

	var declCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_output_declarations WHERE commit_id = ?`,
		"c-integ",
	).Scan(&declCount); err != nil {
		t.Fatalf("count joined declarations: %v", err)
	}
	if declCount != 1 {
		t.Errorf("declarations for commit c-integ: got %d, want 1", declCount)
	}

	var commitStatus string
	if err := db.QueryRow(
		`SELECT status FROM attempt_commits WHERE commit_id = ?`,
		"c-integ",
	).Scan(&commitStatus); err != nil {
		t.Fatalf("select joined commit status: %v", err)
	}
	if commitStatus != "DECLARED" {
		t.Errorf("commit status: got %q, want DECLARED", commitStatus)
	}
}

// ================================================================
// Production-path integration test
// ================================================================

// TestCompletionProtocol_Production_RunMigrations_FreshDB drives the
// PRODUCTION boot path: SQLiteMigrationsFS() (the embed.FS exposed
// by runner.go via //go:embed sqlite/*.sql) → discoverMigrations →
// applyMigration loop. Running on a fresh in-memory DB exercises:
//
//   - the file glob properly picks up 061/062/063 alongside the
//     historical 001..060 set;
//   - the filename parser correctly extracts version + name pairs
//     ("061_attempt_commits" → Version=61, Name="attempt_commits");
//   - the multi-statement SPLITTER in migrations.go handles the new
//     CREATE TABLE + CREATE INDEX pairs and the trailing ALTER TABLE
//     ADD COLUMN statement without false-positive errors;
//   - the ALTER TABLE "duplicate column" tolerance is irrelevant on
//     first-boot (no replay) but the per-statement loop is faithful.
//
// This is the one test that proves the migration files behave the
// same way under the production runner that they do in their
// embed-via-//go:embed form. The other tests in this file exercise
// only the raw SQL semantics in isolation.
func TestCompletionProtocol_Production_RunMigrations_FreshDB(t *testing.T) {
	db := openTestDB(t)

	if err := RunMigrations(db, SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("RunMigrations on the production embed FS: %v", err)
	}

	// The two new CREATE TABLE statements landed.
	for _, table := range []string{"attempt_commits", "task_output_declarations"} {
		if !tableExists(t, db, table) {
			t.Errorf("production RunMigrations: table %s missing", table)
		}
	}

	// task_specs gained required_outputs_json. We deliberately do not
	// pre-create task_specs in this test — the production schema for
	// task_specs is built by migration 040, so the column only exists
	// after 041..062 have also applied cleanly.
	if !columnExists(t, db, "task_specs", "required_outputs_json") {
		t.Error("production RunMigrations: task_specs.required_outputs_json missing")
	}

	// Full schema integrity.
	var integ string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integ); err != nil {
		t.Fatalf("PRAGMA integrity_check after production RunMigrations: %v", err)
	}
	if integ != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", integ)
	}

	// Each migration file → exactly one schema_migrations row. Count
	// everything discoverable from the production embed FS so the
	// assertion stays correct as the migration set grows (a future
	// migration 064 simply adds one row without breaking the test).
	discovered, err := discoverMigrations(SQLiteMigrationsFS(), "sqlite")
	if err != nil {
		t.Fatalf("discoverMigrations: %v", err)
	}
	var migCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migCount); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if migCount != len(discovered) {
		t.Errorf("expected %d schema_migrations entries (one per discovered migration), got %d",
			len(discovered), migCount)
	}

	// The three new schema_migrations rows are present and named
	// according to the parser convention (filename after the version
	// underscore, sans ".sql").
	expectedNames := map[int]string{
		61: "attempt_commits",
		62: "task_output_declarations",
		63: "task_specs_required_outputs",
	}
	for v, wantName := range expectedNames {
		var gotName string
		if err := db.QueryRow(
			`SELECT name FROM schema_migrations WHERE version = ?`, v,
		).Scan(&gotName); err != nil {
			t.Errorf("schema_migrations lookup version %d: %v", v, err)
			continue
		}
		if gotName != wantName {
			t.Errorf("schema_migrations name for version %d: got %q, want %q", v, gotName, wantName)
		}
	}

	// No version was skipped silently AND no version has duplicate
	// schema_migrations rows. Single GROUP BY HAVING query replaces
	// a per-version round-trip loop — at 58+ migrations this keeps
	// the production-path coverage cheap.
	rows, err := db.Query(
		`SELECT version, COUNT(*) FROM schema_migrations
		 GROUP BY version HAVING COUNT(*) != 1
		 ORDER BY version`,
	)
	if err != nil {
		t.Fatalf("group-by having on schema_migrations: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		var n int
		if err := rows.Scan(&v, &n); err != nil {
			t.Fatalf("scan group-by having row: %v", err)
		}
		t.Errorf("schema_migrations has %d rows for version %d, want exactly 1", n, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
}

// ================================================================
// Helpers
// ================================================================

// indexExists returns true when sqlite_master has an index entry of
// the given name. Distinct from tableExists only by the type filter.
func indexExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("check index %s: %v", name, err)
	}
	return count > 0
}

// checksumHex returns the SHA256 hex string of the SQL content. This
// matches the format produced by discoverMigrations at
// migrations.go:118 so the production applyMigration path treats the
// replay as the same migration.
func checksumHex(content string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
}
func TestCompletionProtocolInvariants(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := RunMigrations(db, SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	queries := []string{
		"SELECT j.job_id FROM jobs j WHERE j.status='SUCCEEDED' AND NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.job_id=j.job_id AND a.status='READY');",
		"SELECT t.task_id FROM tasks t WHERE t.status='SUCCEEDED' AND EXISTS (SELECT 1 FROM task_output_declarations d LEFT JOIN artifacts a ON a.id=d.artifact_id AND a.status='READY' WHERE d.task_id=t.task_id AND d.required=1 AND a.id IS NULL);",
		"SELECT job_id, output_kind, COUNT(*) FROM artifacts WHERE status='READY' GROUP BY job_id, output_kind HAVING COUNT(*)>1;",
		"SELECT d.delivery_id FROM job_deliveries d JOIN artifacts a ON a.id=d.artifact_id WHERE a.status!='READY';",
	}

	for i, q := range queries {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatalf("query %d failed: %v", i+1, err)
		}
		defer rows.Close()

		if rows.Next() {
			t.Errorf("invariant query %d returned rows, expected 0", i+1)
		}
		if err := rows.Err(); err != nil {
			t.Errorf("invariant query %d error: %v", i+1, err)
		}
	}

	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity check failed: %v", err)
	}
	if integrity != "ok" {
		t.Errorf("integrity check: expected 'ok', got '%s'", integrity)
	}
}
