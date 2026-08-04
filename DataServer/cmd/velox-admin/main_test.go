package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
)

func TestRunDriveDuplicateCleanupRequiresExplicitMode(t *testing.T) {
	for _, args := range [][]string{
		{"cleanup-drive-duplicates", "--db", "/tmp/velox.db", "--manifest", "/tmp/manifest.json"},
		{"cleanup-drive-duplicates", "--db", "/tmp/velox.db", "--manifest", "/tmp/manifest.json", "--dry-run", "--apply"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("args=%v err=%v stderr=%q", args, err, stderr.String())
		}
	}
}

func TestRunStaleExecutionReconcileRequiresExplicitMode(t *testing.T) {
	for _, args := range [][]string{
		{"reconcile-stale-executions", "--db", "/tmp/velox.db"},
		{"reconcile-stale-executions", "--db", "/tmp/velox.db", "--dry-run", "--apply"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Fatalf("args=%v err=%v stderr=%q", args, err, stderr.String())
		}
	}
}

func TestRunStaleExecutionReconcileDryRunProducesReport(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "admin.db")
	// Opening the store applies the production schema; the command then
	// reopens the same database and must produce a valid dry-run report.
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"reconcile-stale-executions", "--db", dbPath, "--dry-run", "--actor", "cli-test"}, &stdout, &stderr); err != nil {
		t.Fatalf("run dry-run: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"mode": "dry-run"`) {
		t.Fatalf("report=%q", stdout.String())
	}
	_ = os.Remove(dbPath)
}

func TestRunStaleExecutionReconcileApplyPersistsTransitionAndAudit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "admin-apply.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.DB().Exec(`
		INSERT INTO tasks (task_id, job_id, status, revision, attempt_count, created_at, updated_at)
		VALUES ('cli-orphan-task', 'cli-missing-job', 'READY', 1, 0, ?, ?)`, now, now); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := run([]string{"reconcile-stale-executions", "--db", dbPath, "--apply", "--actor", "cli-test"}, &stdout, &stderr); err != nil {
		t.Fatalf("run apply: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"mode": "apply"`) || !strings.Contains(stdout.String(), `"applied"`) {
		t.Fatalf("report=%q", stdout.String())
	}

	check, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var status string
	if err := check.DB().QueryRow(`SELECT status FROM tasks WHERE task_id='cli-orphan-task'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("task status=%q, want CANCELLED", status)
	}
	var audits int
	if err := check.DB().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED' AND actor_id='cli-test'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count=%d, want 1", audits)
	}
}

func TestRunHelpListsDuplicateCleanupCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"--help"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "cleanup-drive-duplicates") {
		t.Fatalf("help=%q", stderr.String())
	}
}
