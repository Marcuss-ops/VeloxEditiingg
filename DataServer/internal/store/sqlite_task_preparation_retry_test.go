package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

func TestRetiredWindowPreparationDoesNotScheduleRuntimeRetry(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "retired-window-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, err = s.db.Exec(`INSERT INTO jobs(job_id,status,max_retries,created_at,updated_at,migrated_at) VALUES('job','RUNNING',2,'x','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,attempt_count,depends_on,created_at,updated_at) VALUES('prep','job','video.prepare.window.v1','FAILED','w','l','a',1,1,'[]','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,depends_on,created_at,updated_at) VALUES('assembly','job','video.assemble.copy.v1','READY','["prep"]','x','x');
		INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('assembly','video.assemble.copy.v1','{"runtime_finalize_status":"WAITING_PREPARATION"}','x');`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	got, err := scheduleRuntimePreparationRetry(context.Background(), tx, taskgraph.IngestResultCommand{
		TaskID: "prep", JobID: "job", AttemptID: "a", WorkerID: "w", LeaseID: "l",
		TaskStatus: taskgraph.StatusFailed, ErrorCode: contract.VideoPrepareWindowRetryableErrorCode,
		ErrorMsg: "temporary network failure",
	}, time.Now())
	if err != nil || got {
		t.Fatalf("retired preparation retry=%t err=%v, want no retry", got, err)
	}
}

func TestSourceCertificationTransientFailureUsesFencedRetry(t *testing.T) {
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "source-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, err = s.db.Exec(`INSERT INTO jobs(job_id,status,max_retries,created_at,updated_at,migrated_at) VALUES('job','RUNNING',1,'x','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,attempt_count,depends_on,created_at,updated_at) VALUES('cert','job',?,'FAILED','w','l','a',1,1,'[]','x','x');
		INSERT INTO tasks(task_id,job_id,executor_id,status,depends_on,created_at,updated_at) VALUES('assembly','job','video.assemble.copy.v1','READY','["cert"]','x','x');
		INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('assembly','video.assemble.copy.v1','{"runtime_finalize_status":"WAITING_PREPARATION"}','x');`, contract.VideoSourceCertificationExecutorID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	got, err := scheduleRuntimePreparationRetry(context.Background(), tx, taskgraph.IngestResultCommand{
		TaskID: "cert", JobID: "job", AttemptID: "a", WorkerID: "w", LeaseID: "l",
		TaskStatus: taskgraph.StatusFailed, ErrorCode: contract.VideoSourceCertificationProbeFailedErrorCode,
	}, time.Now())
	if err != nil || !got {
		t.Fatalf("source certification retry=%t err=%v, want retry", got, err)
	}
}

func TestSourceCertificationIdentityFailuresAreNotRetried(t *testing.T) {
	for _, code := range []string{contract.VideoSourceCertificationInvalidErrorCode, contract.VideoSourceCertificationIncompatibleErrorCode, contract.VideoSourceCertificationIdentityMismatchErrorCode} {
		if retryableRuntimeDependencyFailure(code, "") {
			t.Errorf("permanent source certification failure %s was classified retryable", code)
		}
	}
}
