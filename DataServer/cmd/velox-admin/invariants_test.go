package main

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/invariants"
)

func openWritableAuditDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	for _, stmt := range []string{
		`CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL)`,
		`CREATE TABLE tasks (task_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE task_attempts (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE artifacts (id TEXT PRIMARY KEY, status TEXT NOT NULL, storage_key TEXT, local_path TEXT)`,
		`CREATE TABLE artifact_uploads (upload_id TEXT PRIMARY KEY, status TEXT NOT NULL, completed_at TEXT)`,
		`CREATE TABLE job_deliveries (delivery_id TEXT PRIMARY KEY, status TEXT NOT NULL, remote_id TEXT)`,
		`CREATE TABLE worker_sessions (session_id TEXT PRIMARY KEY, worker_id TEXT NOT NULL, session_type TEXT NOT NULL, status TEXT NOT NULL, revoked INTEGER NOT NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func TestRunInvariantAuditRequiresDatabase(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := run([]string{"audit-invariants"}, &out, &errOut); err == nil || !strings.Contains(err.Error(), "--db is required") {
		t.Fatalf("err=%v stderr=%q", err, errOut.String())
	}
}

func TestRunInvariantAuditReadOnlyReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.db")
	if fixture, err := invariants.OpenReadOnly(path); err == nil {
		fixture.Close()
		t.Fatal("opening missing database unexpectedly succeeded")
	}
	writable, err := openWritableAuditDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := run([]string{"audit-invariants", "--db", path}, &out, &errOut); err != nil {
		t.Fatalf("run audit: %v stderr=%q output=%q", err, errOut.String(), out.String())
	}
	if !strings.Contains(out.String(), `"mode": "read-only"`) || !strings.Contains(out.String(), `"ok": true`) {
		t.Fatalf("output=%q", out.String())
	}
}
