package invariants

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openAuditFixture(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "audit.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
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
			t.Fatal(err)
		}
	}
	return db, path
}

func TestAuditDetectsIllegalLifecycleRowsReadOnly(t *testing.T) {
	db, path := openAuditFixture(t)
	if _, err := db.Exec(`INSERT INTO jobs(job_id,status) VALUES ('job-illegal','SUCCEEDED')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks(task_id,job_id,status) VALUES ('task-illegal','job-illegal','RUNNING')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifacts(id,status,storage_key,local_path) VALUES ('artifact-illegal','READY','','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO job_deliveries(delivery_id,status,remote_id) VALUES ('delivery-illegal','SUCCEEDED','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads(upload_id,status,completed_at) VALUES ('upload-illegal','COMPLETED','')`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	report, err := Audit(context.Background(), ro, path, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if report.OK || len(report.Findings) < 4 {
		t.Fatalf("report=%+v, want multiple findings", report)
	}
	joined := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		joined = append(joined, finding.Invariant+":"+finding.ResourceID)
	}
	text := strings.Join(joined, "|")
	for _, want := range []string{"job_task_convergence:job-illegal", "artifact_ready_blob:artifact-illegal", "delivery_remote_id:delivery-illegal", "upload_terminal_fields:upload-illegal"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing finding %q in %s", want, text)
		}
	}
	if _, err := ro.Exec(`DELETE FROM jobs`); err == nil {
		t.Fatal("read-only audit connection permitted DELETE")
	}
}

func TestAuditCleanFixtureIsOK(t *testing.T) {
	db, path := openAuditFixture(t)
	_ = db.Close()
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	report, err := Audit(context.Background(), ro, path, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || len(report.Findings) != 0 {
		t.Fatalf("report=%+v, want clean", report)
	}
}
