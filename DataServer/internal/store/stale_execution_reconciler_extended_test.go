package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func insertAttemptCommitFixture(t *testing.T, db *sql.DB, commitID, jobID, taskID, attemptID, status, deadline string) {
	t.Helper()
	stamp := deadline
	_, err := db.Exec(`INSERT INTO attempt_commits (
		commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
		task_revision, status, required_output_count, commit_token_hash,
		commit_deadline_at, last_progress_at, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		commitID, taskID, attemptID, jobID, "worker-fixture", "lease-fixture",
		1, status, 1, "hash-fixture", deadline, stamp, stamp, stamp)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStaleExecutionReconciler_CommittedArtifactDrift(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,max_retries,created_at,updated_at,migrated_at,revision) VALUES ('job-drift','RUNNING',3,?,?,?,0)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (
		task_id, job_id, status, revision, attempt_count, attempt_id, attempt_number,
		worker_id, lease_id, created_at, updated_at
	) VALUES ('task-drift','job-drift','RUNNING',1,1,'attempt-drift',1,'worker-fixture','lease-fixture',?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (
		id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		report_version, created_at, updated_at
	) VALUES ('attempt-drift','task-drift','job-drift',1,'worker-fixture','lease-fixture','RUNNING',0,?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	insertAttemptCommitFixture(t, db, "commit-drift", "job-drift", "task-drift", "attempt-drift", "COMMITTED", old)
	if _, err := db.Exec(`INSERT INTO artifacts (id,job_id,type,status,size_bytes,created_at) VALUES ('art-drift','job-drift','video','READY',100,?)`, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_output_declarations (
		declaration_id,commit_id,task_id,attempt_id,output_kind,logical_name,mime_type,
		expected_size_bytes,expected_sha256,status,artifact_id,created_at,updated_at
	) VALUES ('decl-drift','commit-drift','task-drift','attempt-drift','final_video','out.mp4','video/mp4',100,'sha','READY','art-drift',?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != 1 || first.Applied[0].Category != StaleCommittedArtifact {
		t.Fatalf("unexpected first report: %+v", first)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-drift'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("job status=%q, want SUCCEEDED", status)
	}
	var taskStatus, attemptStatus string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='task-drift'`).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM task_attempts WHERE id='attempt-drift'`).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "SUCCEEDED" || attemptStatus != "SUCCEEDED" {
		t.Fatalf("task/attempt statuses=%s/%s, want SUCCEEDED/SUCCEEDED", taskStatus, attemptStatus)
	}
	var outboxCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_id='reconcile_commit_commit-drift'`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("recovery outbox count=%d, want 1", outboxCount)
	}
	second, err := reconciler.Reconcile(context.Background(), now.Add(time.Second), 100, true, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || len(second.Findings) != 0 {
		t.Fatalf("drift was not convergent: %+v", second)
	}
	var outboxAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_id='reconcile_commit_commit-drift'`).Scan(&outboxAfter); err != nil {
		t.Fatal(err)
	}
	if outboxAfter != outboxCount {
		t.Fatalf("outbox count grew on replay: %d -> %d", outboxCount, outboxAfter)
	}
}

func TestStaleExecutionReconciler_DoesNotPromoteTaskWithoutMatchingAttempt(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id,status,max_retries,created_at,updated_at,migrated_at,revision) VALUES ('job-attempt-guard','RUNNING',3,?,?,?,0)`, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (
		task_id, job_id, status, revision, attempt_count, attempt_id, attempt_number,
		worker_id, lease_id, created_at, updated_at
	) VALUES ('task-attempt-guard','job-attempt-guard','RUNNING',1,1,'attempt-attempt-guard',1,'worker-fixture','lease-fixture',?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (
		id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		report_version, created_at, updated_at
	) VALUES ('attempt-attempt-guard','task-attempt-guard','job-attempt-guard',1,'worker-fixture','lease-fixture','FAILED',0,?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	insertAttemptCommitFixture(t, db, "commit-attempt-guard", "job-attempt-guard", "task-attempt-guard", "attempt-attempt-guard", "COMMITTED", old)
	if _, err := db.Exec(`INSERT INTO artifacts (id,job_id,type,status,size_bytes,created_at) VALUES ('art-attempt-guard','job-attempt-guard','video','READY',100,?)`, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_output_declarations (
		declaration_id,commit_id,task_id,attempt_id,output_kind,logical_name,mime_type,
		expected_size_bytes,expected_sha256,status,artifact_id,created_at,updated_at
	) VALUES ('decl-attempt-guard','commit-attempt-guard','task-attempt-guard','attempt-attempt-guard','final_video','out.mp4','video/mp4',100,'sha','READY','art-attempt-guard',?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Applied) != 0 {
		t.Fatalf("reconciler promoted a task without a matching active attempt: %+v", report)
	}
	var taskStatus, jobStatus string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='task-attempt-guard'`).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-attempt-guard'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "RUNNING" || jobStatus != "RUNNING" {
		t.Fatalf("task/job statuses=%s/%s; guard must preserve RUNNING/RUNNING", taskStatus, jobStatus)
	}
}

func TestStaleExecutionReconciler_UnconfirmedSpool(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-time.Minute).Format(time.RFC3339Nano)
	insertAttemptCommitFixture(t, db, "commit-spool", "job-spool", "task-spool", "attempt-spool", "DECLARED", old)
	if _, err := db.Exec(`INSERT INTO task_output_declarations (
		declaration_id,commit_id,task_id,attempt_id,output_kind,logical_name,mime_type,
		expected_size_bytes,expected_sha256,worker_spool_key,status,created_at,updated_at
	) VALUES ('decl-spool','commit-spool','task-spool','attempt-spool','final_video','out.mp4','video/mp4',100,'sha','spool-key','DECLARED',?,?)`, old, old); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewStaleExecutionReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reconciler.Reconcile(context.Background(), now, 100, true, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != 1 || first.Applied[0].Category != StaleUnconfirmedSpool {
		t.Fatalf("unexpected first report: %+v", first)
	}
	var declarationStatus, commitStatus string
	if err := db.QueryRow(`SELECT status FROM task_output_declarations WHERE declaration_id='decl-spool'`).Scan(&declarationStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM attempt_commits WHERE commit_id='commit-spool'`).Scan(&commitStatus); err != nil {
		t.Fatal(err)
	}
	if declarationStatus != "REJECTED" || commitStatus != "EXPIRED" {
		t.Fatalf("statuses=%s/%s, want REJECTED/EXPIRED", declarationStatus, commitStatus)
	}
	second, err := reconciler.Reconcile(context.Background(), now.Add(time.Second), 100, true, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || len(second.Findings) != 0 {
		t.Fatalf("spool reconciliation was not convergent: %+v", second)
	}
}
