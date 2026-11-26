package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReconcileLegacyVideoDeadLetters(t *testing.T) {
	db, err := NewSQLiteStore(filepath.Join(t.TempDir(), "legacy-dlq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	seed := func(taskID, executor, taskStatus, attemptStatus, jobStatus, dlqStatus string, retryable int) {
		t.Helper()
		if _, err := db.DB().ExecContext(ctx, `INSERT INTO jobs(job_id,status,created_at,updated_at,migrated_at) VALUES(?,?,?,?,?)`, "job-"+taskID, jobStatus, "now", "now", "now"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.DB().ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,created_at,updated_at) VALUES(?,?,?,?,?,?)`, taskID, "job-"+taskID, executor, taskStatus, "now", "now"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.DB().ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,status,created_at,updated_at,job_id) VALUES(?,?,?,?,?,?)`, "attempt-"+taskID, taskID, attemptStatus, "now", "now", "job-"+taskID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.DB().ExecContext(ctx, `INSERT INTO dead_letter_tasks(id,job_id,task_id,last_attempt_id,failure_class,retryable,first_failed_at,last_failed_at,status) VALUES(?,?,?,?,?,?,?,?,?)`, "dlq-"+taskID, "job-"+taskID, taskID, "attempt-"+taskID, "TASK_FAILURE", retryable, "now", "now", dlqStatus); err != nil {
			t.Fatal(err)
		}
	}
	seed("resolved", "video.prepare.window.v1", "SUCCEEDED", "FAILED", "SUCCEEDED", "OPEN", 0)
	seed("cancelled", "video.assemble.slice.v1", "FAILED", "FAILED", "FAILED", "OPEN", 0)
	seed("unrelated", "video.assemble.copy.v1", "FAILED", "FAILED", "FAILED", "OPEN", 0)

	dryRun, err := db.ReconcileLegacyVideoDeadLetters(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Open != 2 || dryRun.Resolved != 1 || dryRun.Cancelled != 1 || dryRun.Applied {
		t.Fatalf("unexpected dry-run summary: %+v", dryRun)
	}
	var open int
	if err := db.DB().QueryRowContext(ctx, `SELECT count(*) FROM dead_letter_tasks WHERE status='OPEN'`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 3 {
		t.Fatalf("dry-run changed dead letters: open=%d, want 3", open)
	}

	applied, err := db.ReconcileLegacyVideoDeadLetters(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Open != 2 || applied.Resolved != 1 || applied.Cancelled != 1 || !applied.Applied {
		t.Fatalf("unexpected applied summary: %+v", applied)
	}
	var resolved, cancelled int
	if err := db.DB().QueryRowContext(ctx, `SELECT count(*) FROM dead_letter_tasks WHERE status='RESOLVED'`).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT count(*) FROM dead_letter_tasks WHERE status='CANCELLED'`).Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if resolved != 1 || cancelled != 1 {
		t.Fatalf("unexpected terminal statuses: resolved=%d cancelled=%d", resolved, cancelled)
	}
	again, err := db.ReconcileLegacyVideoDeadLetters(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if again.Open != 0 || again.Resolved != 0 || again.Cancelled != 0 {
		t.Fatalf("reconciliation is not idempotent: %+v", again)
	}
}

func TestReconcileLegacyVideoDeadLettersRejectsRetryableRowsAtomically(t *testing.T) {
	db, err := NewSQLiteStore(filepath.Join(t.TempDir(), "legacy-dlq-live.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO jobs(job_id,status,created_at,updated_at,migrated_at) VALUES('job','FAILED','now','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,created_at,updated_at) VALUES('task','job','video.prepare.window.v1','FAILED','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,status,created_at,updated_at,job_id) VALUES('attempt','task','FAILED','now','now','job')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO dead_letter_tasks(id,job_id,task_id,last_attempt_id,failure_class,retryable,first_failed_at,last_failed_at,status) VALUES('dlq','job','task','attempt','TASK_FAILURE',1,'now','now','OPEN')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileLegacyVideoDeadLetters(ctx, true); err == nil {
		t.Fatal("expected retryable row to abort reconciliation")
	}
	var status string
	if err := db.DB().QueryRowContext(ctx, `SELECT status FROM dead_letter_tasks WHERE id='dlq'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "OPEN" {
		t.Fatalf("reconciliation partially changed a row: status=%s", status)
	}
}
