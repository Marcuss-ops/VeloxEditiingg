package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/stalereconcile"
)

func TestStaleExecutionReconciler_OrphanAttemptIsCancelledAndConvergent(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO task_attempts (
		id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		report_version, created_at, updated_at
	) VALUES ('attempt-orphan', 'missing-task', 'missing-job', 1, 'worker-orphan', 'lease-orphan', 'RUNNING', 0, ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}

	reconciler := newStaleExecutionReconcilerForTest(store)
	first, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != 1 || first.Applied[0].Category != stalereconcile.StaleOrphanAttempt {
		t.Fatalf("unexpected first report: %+v", first)
	}

	var status, errorCode string
	if err := db.QueryRow(`SELECT status, error_code FROM task_attempts WHERE id='attempt-orphan'`).Scan(&status, &errorCode); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" || errorCode != "ORPHAN_ATTEMPT" {
		t.Fatalf("attempt status/error_code=%s/%s, want CANCELLED/ORPHAN_ATTEMPT", status, errorCode)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE resource_type='task_attempt' AND resource_id='attempt-orphan'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count=%d, want 1", audits)
	}

	second, err := reconciler.Reconcile(context.Background(), now.Add(time.Second), 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Findings) != 0 || len(second.Applied) != 0 {
		t.Fatalf("orphan attempt did not converge: %+v", second)
	}
}

func TestStaleExecutionReconciler_TerminalJobAttemptIsCancelled(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,max_retries,created_at,updated_at,migrated_at,revision) VALUES ('job-terminal-active-task','FAILED',3,?,?,?,0)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (task_id,job_id,status,revision,attempt_count,created_at,updated_at) VALUES ('task-terminal-job','job-terminal-active-task','RUNNING',1,1,?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (
		id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		report_version, created_at, updated_at
	) VALUES ('attempt-terminal-job', 'task-terminal-job', 'job-terminal-active-task', 1, 'worker-terminal-job', 'lease-terminal-job', 'RUNNING', 0, ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}

	reconciler := newStaleExecutionReconcilerForTest(store)
	report, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Applied) != 1 || report.Applied[0].Category != stalereconcile.StaleOrphanTask {
		t.Fatalf("terminal-job task/attempt reconciliation was not convergent: %+v", report)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM task_attempts WHERE id='attempt-terminal-job'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("attempt status=%q, want CANCELLED", status)
	}
}

func TestStaleExecutionReconciler_TerminalParentAttemptIsCancelled(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,max_retries,created_at,updated_at,migrated_at,revision) VALUES ('job-terminal-attempt','SUCCEEDED',3,?,?,?,0)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (task_id,job_id,status,revision,attempt_count,created_at,updated_at) VALUES ('task-terminal-attempt','job-terminal-attempt','SUCCEEDED',1,1,?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (
		id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		report_version, created_at, updated_at
	) VALUES ('attempt-terminal-parent', 'task-terminal-attempt', 'job-terminal-attempt', 1, 'worker-terminal', 'lease-terminal', 'PENDING', 0, ?, ?)`, old, old); err != nil {
		t.Fatal(err)
	}

	reconciler := newStaleExecutionReconcilerForTest(store)
	report, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator-test")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Applied) != 1 || report.Applied[0].Category != stalereconcile.StaleOrphanAttempt {
		t.Fatalf("unexpected report: %+v", report)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM task_attempts WHERE id='attempt-terminal-parent'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" {
		t.Fatalf("attempt status=%q, want CANCELLED", status)
	}
}
