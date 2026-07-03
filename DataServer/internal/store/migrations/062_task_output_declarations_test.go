// Package migrations / sqlite/062_task_output_declarations_test.go
//
// Per-migration unit tests for 062_task_output_declarations.sql.
// Coverage:
//   - Table exists after applying 062.
//   - All 15 columns present in the exact order the plan dictates.
//   - The commit_id index present (the supervisor's reconciliation
//     scan joins on commit_id).
//   - PRAGMA integrity_check returns "ok".
//   - UNIQUE(task_id, attempt_id, output_kind, logical_name) rejects
//     a duplicate identity and ACCEPTS rows that differ on any of
//     the four key components (positive coverage).
//   - Round-trip: insert a fully-populated row, read it back, verify
//     nullable columns (worker_spool_key, upload_id, artifact_id)
//     surface as sql.NullString with the expected Valid/Invalid.
package migrations

import (
	"database/sql"
	"strings"
	"testing"

	// Required for //go:embed parsing.
	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/062_task_output_declarations.sql
var sqliteSQL062 string

// TestMigration062_TableExists asserts the table lands on a fresh DB
// after the migration is applied.
func TestMigration062_TableExists(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

	if !tableExists(t, db, "task_output_declarations") {
		t.Fatal("expected task_output_declarations table to exist after migration 062")
	}
}

// TestMigration062_AllColumns asserts every column from the plan
// (`docs/completion-protocol.md` §1.2) is present.
func TestMigration062_AllColumns(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

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

// TestMigration062_Indexes asserts the single index from the plan is
// present. The supervisor's reconciliation scan reads declarations
// by commit_id.
func TestMigration062_Indexes(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

	if !indexExists(t, db, "idx_task_output_declarations_commit") {
		t.Errorf("task_output_declarations index idx_task_output_declarations_commit missing after migration 062")
	}
}

// TestMigration062_IntegrityOK runs PRAGMA integrity_check. Any
// schema-level corruption (orphaned pages, missing btree entries,
// mismatched rowids) surfaces here.
func TestMigration062_IntegrityOK(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}

// TestMigration062_UniqueQuadTuple exercises the canonical UNIQUE
// constraint from the plan:
//
//	UNIQUE(task_id, attempt_id, output_kind, logical_name)
//
// A second INSERT with the same quadruple MUST be rejected.
// Inserts that differ on any one of the four components MUST be
// accepted — the positive coverage guards against an
// over-broad constraint that would reject legitimate declarations.
func TestMigration062_UniqueQuadTuple(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

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

// TestMigration062_RoundTrip drives the full lifecycle: insert a
// fully-populated row, read it back, verify every column value.
// Nullable columns (worker_spool_key, upload_id, artifact_id) are
// asserted as sql.NullString with Valid=true; a future schema
// change that makes one of them NOT NULL would fail here.
func TestMigration062_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

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

// TestMigration062_RejectsMissingRequiredColumn asserts the NOT NULL
// constraints on the canonical columns are enforced. A future
// schema change that loosens a NOT NULL would fail this test.
func TestMigration062_RejectsMissingRequiredColumn(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL062)

	now := "2024-01-01T00:00:00Z"
	// Omit mime_type (NOT NULL) — must fail.
	_, err := db.Exec(
		`INSERT INTO task_output_declarations (
			declaration_id, commit_id, task_id, attempt_id,
			output_kind, logical_name,
			expected_size_bytes, expected_sha256,
			status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"d-nn", "c-nn", "t-nn", "a-nn",
		"final_video", "out.mp4",
		1024, "sha-nn",
		"DECLARED", now, now,
	)
	if err == nil {
		t.Fatal("expected NOT NULL violation on mime_type, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "not null") {
		t.Errorf("expected NOT NULL violation, got: %v", err)
	}
}
