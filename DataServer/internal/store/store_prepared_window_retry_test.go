package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRetryFailedPreparedWindowAfterWorkerUpdate(t *testing.T) {
	ctx := context.Background()
	db, err := NewSQLiteStore(filepath.Join(t.TempDir(), "prepared-window-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateFleetOperationsTableIfNotExists(); err != nil {
		t.Fatal(err)
	}
	conn := db.DB()
	if _, err := conn.ExecContext(ctx, `INSERT INTO jobs(job_id,status,updated_at,migrated_at) VALUES('job','RUNNING','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,created_at,updated_at)
		VALUES('task','job','video.prepare.window.v1','FAILED','now','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO task_specs(task_id,executor_id,payload_json,created_at)
		VALUES('task','video.prepare.window.v1','{}','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,status,error_code,error_message,created_at,updated_at,completed_at,job_id)
		VALUES('attempt','task',1,'worker-a','FAILED','WINDOW_CERTIFICATION_FAILED','short output','now','now','2026-11-09T11:00:00Z','job')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetryFailedPreparedWindows(ctx, "job"); err == nil {
		t.Fatal("certification failure was retried without a worker image update")
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO fleet_operations(operation_id,worker_id,op,requested_by,reason,status,queued_at,finished_at,payload)
		VALUES('update','worker-a','update','operator','fixed worker image','SUCCEEDED','2026-11-09T11:00:01Z','2026-11-09T11:00:02Z','{}')`); err != nil {
		t.Fatal(err)
	}
	ids, err := db.RetryFailedPreparedWindows(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "task" {
		t.Fatalf("requeued task IDs = %v, want [task]", ids)
	}
	var status string
	if err := conn.QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id='task'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "READY" {
		t.Fatalf("task status = %s, want READY", status)
	}
}
