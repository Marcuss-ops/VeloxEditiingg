package store

import (
	"context"
	"testing"
	"time"
)

func TestCancelTerminalizesActiveTasksAndAttemptsAtomically(t *testing.T) {
	store, db := openStaleReconcilerTestDB(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT INTO jobs (job_id,status,max_retries,revision,created_at,updated_at,migrated_at)
		VALUES ('job-cancel-active','RUNNING',3,0,?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO tasks (task_id,job_id,status,revision,attempt_count,worker_id,lease_id,lease_expires_at,created_at,updated_at)
		VALUES ('task-cancel-active','job-cancel-active','LEASED',4,1,'worker-1','lease-1',?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO task_attempts (id,task_id,job_id,attempt_number,worker_id,lease_id,status,report_version,created_at,updated_at)
		VALUES ('attempt-cancel-active','task-cancel-active','job-cancel-active',1,'worker-1','lease-1','RUNNING',0,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}

	repo := NewSQLiteJobRepository(store)
	if err := repo.Cancel(context.Background(), "job-cancel-active", "restart cleanup", -1); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	var taskStatus, workerID, leaseID string
	if err := db.QueryRow(`SELECT status,worker_id,lease_id FROM tasks WHERE task_id='task-cancel-active'`).Scan(&taskStatus, &workerID, &leaseID); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "CANCELLED" || workerID != "" || leaseID != "" {
		t.Fatalf("task after cancel = status=%q worker=%q lease=%q", taskStatus, workerID, leaseID)
	}

	var attemptStatus, errorCode string
	if err := db.QueryRow(`SELECT status,error_code FROM task_attempts WHERE id='attempt-cancel-active'`).Scan(&attemptStatus, &errorCode); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "CANCELLED" || errorCode != "TASK_CANCELLED" {
		t.Fatalf("attempt after cancel = status=%q error_code=%q", attemptStatus, errorCode)
	}
}
