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
// Migration 061: attempt_commits
// ================================================================

func TestMigration061_AttemptCommits_TableExists(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	if !tableExists(t, db, "attempt_commits") {
		t.Fatal("expected attempt_commits table to exist after migration 061")
	}
}

func TestMigration061_AttemptCommits_AllColumns(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	expected := []string{
		"commit_id",
		"task_id",
		"attempt_id",
		"job_id",
		"worker_id",
		"lease_id",
		"task_revision",
		"status",
		"required_output_count",
		"ready_output_count",
		"commit_token_hash",
		"commit_deadline_at",
		"last_progress_at",
		"committed_at",
		"rejected_code",
		"rejected_message",
		"created_at",
		"updated_at",
	}
	for _, col := range expected {
		if !columnExists(t, db, "attempt_commits", col) {
			t.Errorf("attempt_commits column %q missing after migration 061", col)
		}
	}
}

func TestMigration061_AttemptCommits_Indexes(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	expected := []string{
		"idx_attempt_commits_status",
		"idx_attempt_commits_deadline",
	}
	for _, idx := range expected {
		if !indexExists(t, db, idx) {
			t.Errorf("attempt_commits index %q missing after migration 061", idx)
		}
	}
}

func TestMigration061_AttemptCommits_IntegrityOK(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}

func TestMigration061_AttemptCommits_DefaultReadyOutputCount(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	now := "2024-01-01T00:00:00Z"
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"c-1", "t-1", "a-1", "j-1", "w-1", "l-1",
		3, "DECLARED", 1,
		"hash-x", now, now, now, now,
	)
	if err != nil {
		t.Fatalf("insert attempt_commits: %v", err)
	}

	var readyCount int
	if err := db.QueryRow(
		`SELECT ready_output_count FROM attempt_commits WHERE commit_id = ?`, "c-1",
	).Scan(&readyCount); err != nil {
		t.Fatalf("read ready_output_count: %v", err)
	}
	if readyCount != 0 {
		t.Errorf("ready_output_count default: got %d, want 0", readyCount)
	}
}

func TestMigration061_AttemptCommits_UniqueOnTaskAndAttempt(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	now := "2024-01-01T00:00:00Z"
	insertRow := func(commitID string) error {
		_, err := db.Exec(
			`INSERT INTO attempt_commits (
				commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
				task_revision, status, required_output_count,
				commit_token_hash, commit_deadline_at, last_progress_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			commitID, "t-same", "a-same", "j-1", "w-1", "l-1",
			1, "DECLARED", 1,
			"hash-x", now, now, now, now,
		)
		return err
	}

	if err := insertRow("c-first"); err != nil {
		t.Fatalf("first insert attempt_commits: %v", err)
	}
	if err := insertRow("c-second"); err == nil {
		t.Fatal("expected UNIQUE violation on (task_id, attempt_id), got nil")
	} else if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("second insert error should mention UNIQUE violation, got: %v", err)
	}
}

func TestMigration061_AttemptCommits_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql061AttemptCommits)

	now := "2024-01-01T00:00:00Z"
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			committed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"c-rt", "t-rt", "a-rt", "j-rt", "w-rt", "l-rt",
		5, "VERIFYING", 2,
		"hash-rt", now, now, now, now, now,
	)
	if err != nil {
		t.Fatalf("insert attempt_commits round-trip: %v", err)
	}

	var (
		status         string
		commitToken    string
		committedAt    sql.NullString
		requiredCount  int
		taskRevision   int
	)
	err = db.QueryRow(
		`SELECT status, commit_token_hash, committed_at,
		        required_output_count, task_revision
		 FROM attempt_commits WHERE commit_id = ?`, "c-rt",
	).Scan(&status, &commitToken, &committedAt, &requiredCount, &taskRevision)
	if err != nil {
		t.Fatalf("select attempt_commits row: %v", err)
	}
	if status != "VERIFYING" {
		t.Errorf("status: got %q, want VERIFYING", status)
	}
	if commitToken != "hash-rt" {
		t.Errorf("commit_token_hash: got %q, want hash-rt", commitToken)
	}
	if !committedAt.Valid || committedAt.String != now {
		t.Errorf("committed_at: got %v, want %q", committedAt, now)
	}
	if requiredCount != 2 {
		t.Errorf("required_output_count: got %d, want 2", requiredCount)
	}
	if taskRevision != 5 {
		t.Errorf("task_revision: got %d, want 5", taskRevision)
	}

	// Update path: bump ready_output_count, mark committed_at is already
	// set, and verify the row still passes UNIQUE/read integrity.
	if _, err := db.Exec(
		`UPDATE attempt_commits
		    SET status = ?, ready_output_count = ?, updated_at = ?
		  WHERE commit_id = ?`,
		"COMMITTED", 2, "2024-01-01T00:01:00Z", "c-rt",
	); err != nil {
		t.Fatalf("update attempt_commits: %v", err)
	}

	var newStatus string
	if err := db.QueryRow(
		`SELECT status FROM attempt_commits WHERE commit_id = ?`, "c-rt",
	).Scan(&newStatus); err != nil {
		t.Fatalf("re-select after update: %v", err)
	}
	if newStatus != "COMMITTED" {
		t.Errorf("status after update: got %q, want COMMITTED", newStatus)
	}

	// Hard delete and re-insert should be allowed (UNIQUE removed with
	// the row). Model the "previous attempt's commit row removed, new
	// attempt for same task_id" recovery flow Phase 2 will exercise.
	if _, err := db.Exec(`DELETE FROM attempt_commits WHERE commit_id = ?`, "c-rt"); err != nil {
		t.Fatalf("delete attempt_commits: %v", err)
	}
	if err := insertForTaskAndAttempt(db, "c-rt-after-delete", "t-rt", "a-rt", now); err != nil {
		t.Errorf("re-INSERT after DELETE for same (task_id, attempt_id): %v", err)
	}
}

// insertForTaskAndAttempt inserts a fresh attempt_commits row with the
// given triple. Helper for the round-trip test's delete+reinsert case.
func insertForTaskAndAttempt(db *sql.DB, commitID, taskID, attemptID, now string) error {
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		commitID, taskID, attemptID, "j-rt", "w-rt", "l-rt",
		6, "DECLARED", 1,
		"hash-rt-2", now, now, now, now,
	)
	return err
}

// ================================================================
// Migration 062: task_output_declarations
// ================================================================

func TestMigration062_TaskOutputDeclarations_TableExists(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	if !tableExists(t, db, "task_output_declarations") {
		t.Fatal("expected task_output_declarations table to exist after migration 062")
	}
}

func TestMigration062_TaskOutputDeclarations_AllColumns(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	expected := []string{
		"declaration_id",
		"commit_id",
		"task_id",
		"attempt_id",
		"output_kind",
		"logical_name",
		"mime_type",
		"expected_size_bytes",
		"expected_sha256",
		"worker_spool_key",
		"status",
		"upload_id",
		"artifact_id",
		"created_at",
		"updated_at",
	}
	for _, col := range expected {
		if !columnExists(t, db, "task_output_declarations", col) {
			t.Errorf("task_output_declarations column %q missing after migration 062", col)
		}
	}
}

func TestMigration062_TaskOutputDeclarations_Indexes(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	if !indexExists(t, db, "idx_task_output_declarations_commit") {
		t.Errorf("task_output_declarations index idx_task_output_declarations_commit missing after migration 062")
	}
}

func TestMigration062_TaskOutputDeclarations_IntegrityOK(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}

func TestMigration062_TaskOutputDeclarations_UniqueQuadTuple(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	now := "2024-01-01T00:00:00Z"
	insertDecl := func(declID, kind, logicalName string) error {
		_, err := db.Exec(
			`INSERT INTO task_output_declarations (
				declaration_id, commit_id, task_id, attempt_id,
				output_kind, logical_name, mime_type,
				expected_size_bytes, expected_sha256,
				status, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			declID, "c-1", "t-1", "a-1", kind, logicalName,
			"video/mp4", 1024, "sha-x", "DECLARED", now, now,
		)
		return err
	}

	// First INSERT — succeeds.
	if err := insertDecl("d-first", "final_video", "out.mp4"); err != nil {
		t.Fatalf("first declaration insert: %v", err)
	}

	// Same task+attempt+kind+logical_name → UNIQUE violation.
	if err := insertDecl("d-second", "final_video", "out.mp4"); err == nil {
		t.Fatal("expected UNIQUE violation on (task_id, attempt_id, output_kind, logical_name), got nil")
	} else if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("second insert error should mention UNIQUE violation, got: %v", err)
	}

	// Differ on output_kind → allowed.
	if err := insertDecl("d-third", "thumbnail", "out.mp4"); err != nil {
		t.Errorf("declaration with different output_kind should be allowed, got: %v", err)
	}

	// Differ on logical_name → allowed.
	if err := insertDecl("d-fourth", "final_video", "preview.mp4"); err != nil {
		t.Errorf("declaration with different logical_name should be allowed, got: %v", err)
	}
}

func TestMigration062_TaskOutputDeclarations_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sql062TaskOutputDeclarations)

	now := "2024-01-01T00:00:00Z"
	_, err := db.Exec(
		`INSERT INTO task_output_declarations (
			declaration_id, commit_id, task_id, attempt_id,
			output_kind, logical_name, mime_type,
			expected_size_bytes, expected_sha256,
			worker_spool_key, status,
			upload_id, artifact_id,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"d-rt", "c-rt", "t-rt", "a-rt",
		"final_video", "out.mp4", "video/mp4",
		2048, "sha-rt",
		"spool-key-x", "UPLOADING",
		"u-rt", "art-rt",
		now, now,
	)
	if err != nil {
		t.Fatalf("insert task_output_declarations: %v", err)
	}

	var (
		kind       string
		logical    string
		mime       string
		size       int64
		sha        string
		spoolKey   sql.NullString
		status     string
		uploadID   sql.NullString
		artifactID sql.NullString
	)
	err = db.QueryRow(
		`SELECT output_kind, logical_name, mime_type,
		        expected_size_bytes, expected_sha256,
		        worker_spool_key, status,
		        upload_id, artifact_id
		 FROM task_output_declarations WHERE declaration_id = ?`, "d-rt",
	).Scan(&kind, &logical, &mime, &size, &sha, &spoolKey, &status, &uploadID, &artifactID)
	if err != nil {
		t.Fatalf("select task_output_declarations row: %v", err)
	}
	if kind != "final_video" || logical != "out.mp4" || mime != "video/mp4" {
		t.Errorf("output_kind/logical_name/mime_type mismatch: %q/%q/%q", kind, logical, mime)
	}
	if size != 2048 {
		t.Errorf("expected_size_bytes: got %d, want 2048", size)
	}
	if sha != "sha-rt" {
		t.Errorf("expected_sha256: got %q, want sha-rt", sha)
	}
	if !spoolKey.Valid || spoolKey.String != "spool-key-x" {
		t.Errorf("worker_spool_key: got %v, want spool-key-x", spoolKey)
	}
	if status != "UPLOADING" {
		t.Errorf("status: got %q, want UPLOADING", status)
	}
	if !uploadID.Valid || uploadID.String != "u-rt" {
		t.Errorf("upload_id: got %v, want u-rt", uploadID)
	}
	if !artifactID.Valid || artifactID.String != "art-rt" {
		t.Errorf("artifact_id: got %v, want art-rt", artifactID)
	}
}

// ================================================================
// Migration 063: task_specs ADD COLUMN required_outputs_json
// ================================================================

func TestMigration063_RequiredOutputs_ColumnAdded(t *testing.T) {
	db := openTestDB(t)
	// Pre-create task_specs with a minimal schema representative of
	// the production shape after migration 040 (column list is
	// irrelevant — we only need the table to exist for ALTER TABLE
	// to apply ADD COLUMN).
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
	applyMigrationSQL(t, db, sql063TaskSpecsRequiredOutputs)

	if !columnExists(t, db, "task_specs", "required_outputs_json") {
		t.Fatal("required_outputs_json column missing after migration 063")
	}
}

func TestMigration063_RequiredOutputs_DefaultOnExistingRows(t *testing.T) {
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

	applyMigrationSQL(t, db, sql063TaskSpecsRequiredOutputs)

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

func TestMigration063_RequiredOutputs_AcceptsNonEmpty(t *testing.T) {
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
	applyMigrationSQL(t, db, sql063TaskSpecsRequiredOutputs)

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

func TestMigration063_RequiredOutputs_AddColumnTolerance(t *testing.T) {
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

	// Drive the production applyMigration path directly so the test
	// genuinely exercises the "duplicate column name" tolerance at
	// migrations.go:178 rather than just the raw SQLite driver
	// error string. We compute the same SHA256 hex for the SQL
	// content that production produces on discover.
	checksum := checksumHex(sql063TaskSpecsRequiredOutputs)
	mig := Migration{
		Version:  63,
		Name:     "task_specs_required_outputs",
		SQL:      sql063TaskSpecsRequiredOutputs,
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

func TestMigration063_RequiredOutputs_IntegrityOK(t *testing.T) {
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
	applyMigrationSQL(t, db, sql063TaskSpecsRequiredOutputs)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
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
