package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

func TestIngestRetryablePreparationClosesAttemptAndRequeuesAtomically(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "ingest-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, err = s.db.Exec(`
		INSERT INTO jobs(job_id,status,max_retries,created_at,updated_at,migrated_at) VALUES('job','RUNNING',1,'x','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,attempt_count,depends_on,created_at,updated_at) VALUES('prep','job','video.prepare.window.v1','RUNNING','w','l','a',1,1,'[]','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,depends_on,created_at,updated_at) VALUES('assembly','job','video.assemble.copy.v1','READY','["prep"]','x','x');
		INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('assembly','video.assemble.copy.v1','{"runtime_finalize_status":"WAITING_PREPARATION"}','x');
		INSERT INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,lease_id,status,created_at,updated_at) VALUES('a','prep','job',1,'w','l','RUNNING','x','x');`)
	if err != nil {
		t.Fatal(err)
	}
	cmd := taskgraph.IngestResultCommand{TaskID: "prep", JobID: "job", AttemptID: "a", WorkerID: "w", LeaseID: "l", TaskStatus: taskgraph.StatusFailed, AttemptStatus: taskattempts.AttemptStatusFailed, ErrorCode: contract.VideoPrepareWindowRetryableErrorCode, ErrorMsg: "temporary network failure"}
	if err := NewSQLiteTaskRepository(s).IngestTaskResultAtomic(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	var taskStatus, attemptStatus, parentStatus, jobStatus string
	var deadLetters int
	err = s.db.QueryRow(`SELECT t.status,a.status,p.status,j.status,(SELECT count(*) FROM dead_letter_tasks WHERE task_id='prep') FROM tasks t JOIN task_attempts a ON a.id='a' JOIN tasks p ON p.task_id='assembly' JOIN jobs j ON j.job_id=t.job_id WHERE t.task_id='prep'`).Scan(&taskStatus, &attemptStatus, &parentStatus, &jobStatus, &deadLetters)
	if err != nil {
		t.Fatal(err)
	}
	if taskStatus != "PENDING" || attemptStatus != "FAILED" || parentStatus != "READY" || jobStatus != "RUNNING" || deadLetters != 0 {
		t.Fatalf("task=%s attempt=%s parent=%s job=%s deadletters=%d", taskStatus, attemptStatus, parentStatus, jobStatus, deadLetters)
	}
}

func TestPreparedWindowRetryPreservesAttemptAndHonorsBackoff(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	_, err = s.db.Exec(`
		INSERT INTO jobs(job_id,status,max_retries,created_at,updated_at,migrated_at) VALUES('job','RUNNING',1,'2026-01-01','2026-01-01','2026-01-01');
		INSERT INTO tasks(task_id,job_id,executor_id,executor_version,status,worker_id,lease_id,attempt_id,attempt_count,attempt_number,revision,depends_on,created_at,updated_at)
		VALUES('prep','job','video.prepare.window.v1',1,'FAILED','worker','lease','attempt-1',1,1,2,'[]','2026-01-01','2026-01-01');
		INSERT INTO tasks(task_id,job_id,executor_id,executor_version,status,worker_id,lease_id,revision,depends_on,created_at,updated_at)
		VALUES('assembly','job','video.assemble.copy.v1',1,'READY','','',1,'["prep"]','2026-01-01','2026-01-01');
		INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('prep','video.prepare.window.v1','{}','2026-01-01');
		INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('assembly','video.assemble.copy.v1','{"runtime_finalize_status":"WAITING_PREPARATION","runtime_assets_pending":true}','2026-01-01');
		INSERT INTO task_attempts(id,task_id,job_id,attempt_number,worker_id,lease_id,status,error_code,error_message,created_at,updated_at) VALUES('attempt-1','prep','job',1,'worker','lease','FAILED','UPLOAD_FAILED','prepared-window registry upload: HTTP 401','2026-01-01','2026-01-01');
		UPDATE tasks SET project_id='',render_plan_id='',created_at='2026-01-01T00:00:00Z',updated_at='2026-01-01T00:00:00Z';
		INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES('worker-2','worker-2','worker','{}','2026-01-01T00:00:00Z');
		INSERT INTO worker_sessions(session_id,worker_id,token_hash,created_at,expires_at,session_type,status,revoked) VALUES('session-2','worker-2','hash','2026-01-01T00:00:00Z','2030-01-01T00:00:00Z','control','ACTIVE',0);
		INSERT INTO worker_runtime_snapshots(snapshot_id,worker_id,session_id) VALUES('snapshot-2','worker-2','session-2');`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	repo := NewSQLiteTaskRepository(s)
	retrying, err := repo.ReconcileFailedRuntimePreparations(ctx, 10)
	if err != nil || retrying != 1 {
		t.Fatalf("reconciled=%d err=%v", retrying, err)
	}
	lifecycle, err := taskgraph.NewLifecycleService(repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.TickReadiness(ctx, 20); err != nil {
		t.Fatal(err)
	}
	task, err := repo.Get(ctx, "prep")
	if err != nil || task.Status != taskgraph.StatusPending || task.ReadyAt == nil || !task.ReadyAt.After(now) {
		t.Fatalf("during backoff task=%+v err=%v", task, err)
	}
	if _, err := s.db.Exec(`UPDATE tasks SET ready_at=? WHERE task_id='prep'`, now.Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.TickReadiness(ctx, 20); err != nil {
		t.Fatal(err)
	}
	claimed, attempt, err := repo.ClaimNextWithAttemptAtomic(ctx, "worker-2", "lease-2")
	if err != nil || claimed == nil || attempt == nil {
		t.Fatalf("claim task=%+v attempt=%+v err=%v", claimed, attempt, err)
	}
	if claimed.ID != "prep" || attempt.AttemptNumber != 2 || attempt.ID == "attempt-1" {
		t.Fatalf("retry identity task=%+v attempt=%+v", claimed, attempt)
	}
	var oldStatus string
	if err := s.db.QueryRow(`SELECT status FROM task_attempts WHERE id='attempt-1'`).Scan(&oldStatus); err != nil || oldStatus != "FAILED" {
		t.Fatalf("prior attempt status=%s err=%v", oldStatus, err)
	}
}

func TestPreparedWindowRetryIsBoundedAndRequiresKnownFailure(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		msg     string
	}{
		{2, "prepared-window registry upload: HTTP 401"},
		{1, "WINDOW_EXTRADATA_IDENTITY_MISMATCH"},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "retry.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			_, err = s.db.Exec(`INSERT INTO jobs(job_id,status,max_retries,created_at,updated_at,migrated_at) VALUES('job','RUNNING',1,'x','x','x');
			INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,attempt_count,depends_on,created_at,updated_at) VALUES('prep','job','video.prepare.window.v1','FAILED','w','l','a',?,?,'[]','x','x');
			INSERT INTO tasks(task_id,job_id,executor_id,status,depends_on,created_at,updated_at) VALUES('assembly','job','video.assemble.copy.v1','READY','["prep"]','x','x');
			INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('assembly','video.assemble.copy.v1','{"runtime_finalize_status":"WAITING_PREPARATION"}','x');`, tc.attempt, tc.attempt)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			got, err := schedulePreparedWindowRetry(context.Background(), tx, taskgraph.IngestResultCommand{TaskID: "prep", JobID: "job", AttemptID: "a", WorkerID: "w", LeaseID: "l", TaskStatus: taskgraph.StatusFailed, ErrorMsg: tc.msg}, time.Now())
			if err != nil || got {
				t.Fatalf("retrying=%v err=%v", got, err)
			}
		})
	}
}
