package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/outbox"
	"velox-shared/taskcontract"
)

func TestRuntimeFinalizationRecoveryRecomputesSpecHash(t *testing.T) {
	for _, hasPreparations := range []bool{false, true} {
		t.Run(fmt.Sprint(hasPreparations), func(t *testing.T) {
			repo, db := runtimeFailureFixture(t, "READY", "SUCCEEDED", "RUNNING", false)
			if hasPreparations {
				if _, err := db.Exec(`UPDATE task_specs SET payload_json=json_set(payload_json,'$.runtime_prepare_task_ids',json('["prep"]')) WHERE task_id='assembly'`); err != nil {
					t.Fatal(err)
				}
			}
			if err := repo.RecoverInterruptedRuntimeFinalizations(context.Background()); err != nil {
				t.Fatal(err)
			}
			var raw, hash string
			if err := db.QueryRow(`SELECT payload_json,spec_hash FROM task_specs WHERE task_id='assembly'`).Scan(&raw, &hash); err != nil {
				t.Fatal(err)
			}
			var payload map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &payload); err != nil {
				t.Fatal(err)
			}
			state := "QUEUED"
			if hasPreparations {
				state = "WAITING_PREPARATION"
			}
			want, err := (&taskcontract.TaskSpec{Version: 1, JobID: "job-1", ExecutorID: "video.assemble.copy.v1", Payload: payload}).SpecHash()
			if err != nil {
				t.Fatal(err)
			}
			if payload["runtime_finalize_status"] != state || hash != want {
				t.Fatalf("state=%v hash=%s want=%s", payload["runtime_finalize_status"], hash, want)
			}
		})
	}
}

type runtimeFailureOutbox struct{ fail bool }

func (o runtimeFailureOutbox) Insert(ctx context.Context, tx outbox.Executor, p outbox.InsertParams) (string, error) {
	if o.fail {
		return "", errors.New("outbox unavailable")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO test_outbox(event_type,payload) VALUES (?,?)`, p.EventType, string(p.Payload))
	return "event-1", err
}

func runtimeFailureFixture(t *testing.T, parentStatus, childStatus, finalizeStatus string, failOutbox bool) (*SQLiteTaskRepository, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "finalization.db")+"?_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
		CREATE TABLE jobs(job_id TEXT PRIMARY KEY,status TEXT,revision INTEGER,updated_at TEXT,error_message TEXT,failed_at TEXT,failed_by TEXT,max_retries INTEGER DEFAULT 3);
		CREATE TABLE tasks(task_id TEXT PRIMARY KEY,job_id TEXT,executor_id TEXT,status TEXT,worker_id TEXT,lease_id TEXT,attempt_id TEXT,attempt_number INTEGER,ready_at TEXT,started_at TEXT,completed_at TEXT,lease_expires_at TEXT,revision INTEGER,updated_at TEXT,depends_on TEXT,winning_attempt_terminal_pending INTEGER DEFAULT 0,winning_attempt_id TEXT,winning_attempt_committed_at TEXT);
		CREATE TABLE task_specs(task_id TEXT PRIMARY KEY,spec_version INTEGER,spec_hash TEXT,payload_json TEXT,executor_id TEXT);
		CREATE TABLE task_attempts(id TEXT PRIMARY KEY,task_id TEXT,attempt_number INTEGER,error_code TEXT,error_message TEXT,worker_id TEXT,lease_id TEXT,completed_at TEXT,created_at TEXT);
		CREATE TABLE fleet_operations(operation_id TEXT PRIMARY KEY,worker_id TEXT,op TEXT,status TEXT,finished_at TEXT);
		CREATE TABLE job_history(job_id TEXT,status TEXT,worker_id TEXT,message TEXT,raw_json TEXT,event_ts TEXT);
		CREATE TABLE job_events(timestamp TEXT,job_id TEXT,event TEXT,raw_json TEXT);
		CREATE TABLE test_outbox(event_type TEXT,payload TEXT);
		INSERT INTO jobs(job_id,status,revision,max_retries) VALUES('job-1','RUNNING',1,3);
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,revision,updated_at,completed_at,depends_on) VALUES('assembly','job-1','video.assemble.copy.v1',?,'','','',0,1,'2026-01-01','', '["prep"]');
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,revision,updated_at,completed_at,depends_on) VALUES('prep','job-1','video.prepare.window.v1',?,'worker-1','lease-1','attempt-1',1,1,'2026-01-01','2026-01-01', '[]');
		INSERT INTO tasks(task_id,job_id,executor_id,status,worker_id,lease_id,attempt_id,attempt_number,revision,updated_at,depends_on) VALUES('other','job-1','video.prepare.window.v1','READY','','','',0,1,'2026-01-01', '[]');
		INSERT INTO task_specs(task_id,spec_version,spec_hash,payload_json,executor_id) VALUES('assembly',1,'old',json_object('runtime_finalize_status',?,'runtime_assets_pending',json('true')),'video.assemble.copy.v1');
		INSERT INTO task_specs(task_id,spec_version,spec_hash,payload_json,executor_id) VALUES('prep',1,'prep-hash','{}','video.prepare.window.v1');
		INSERT INTO task_specs(task_id,spec_version,spec_hash,payload_json,executor_id) VALUES('other',1,'other-hash','{}','video.prepare.window.v1');
		INSERT INTO task_attempts(id,task_id,attempt_number,error_code,error_message,worker_id,lease_id,completed_at,created_at) VALUES('attempt-1','prep',1,'WINDOW_ENCODE_FAILED','bad source','worker-1','lease-1','2026-01-01','2026-01-01');`, parentStatus, childStatus, finalizeStatus)
	if err != nil {
		t.Fatal(err)
	}
	s := &SQLiteStore{db: db}
	s.SetOutbox(runtimeFailureOutbox{fail: failOutbox})
	return NewSQLiteTaskRepository(s), db
}

func TestReconcileFailedRuntimePreparationRetriesExtradataMismatchAfterWorkerUpdate(t *testing.T) {
	repo, db := runtimeFailureFixture(t, "READY", "FAILED", "WAITING_PREPARATION", false)
	if _, err := db.Exec(`INSERT INTO fleet_operations(operation_id,worker_id,op,status,finished_at) VALUES('update-1','worker-1','update','SUCCEEDED','2026-01-02')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE task_attempts SET error_code='WINDOW_EXTRADATA_IDENTITY_MISMATCH',error_message='old worker SAR',completed_at='2026-01-01' WHERE id='attempt-1'`); err != nil {
		t.Fatal(err)
	}
	n, err := repo.ReconcileFailedRuntimePreparations(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("reconcile count=%d err=%v", n, err)
	}
	var jobStatus, parentStatus, childStatus, finalizeStatus string
	if err := db.QueryRow(`SELECT j.status,p.status,c.status,json_extract(s.payload_json,'$.runtime_finalize_status')
		FROM jobs j JOIN tasks p ON p.job_id=j.job_id JOIN tasks c ON c.task_id='prep'
		JOIN task_specs s ON s.task_id=p.task_id WHERE j.job_id='job-1' AND p.task_id='assembly'`).
		Scan(&jobStatus, &parentStatus, &childStatus, &finalizeStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "RUNNING" || parentStatus != "READY" || childStatus != "PENDING" || finalizeStatus != "WAITING_PREPARATION" {
		t.Fatalf("unexpected retry state job=%s parent=%s child=%s finalize=%s", jobStatus, parentStatus, childStatus, finalizeStatus)
	}
}

func TestReconcileFailedRuntimePreparationClosesJobAtomically(t *testing.T) {
	for _, parentStatus := range []string{"PENDING", "READY", "CANCELLED"} {
		t.Run(parentStatus, func(t *testing.T) {
			repo, db := runtimeFailureFixture(t, parentStatus, "FAILED", "WAITING_PREPARATION", false)
			n, err := repo.ReconcileFailedRuntimePreparations(context.Background(), 10)
			if err != nil || n != 1 {
				t.Fatalf("reconcile count=%d err=%v", n, err)
			}
			var job, task, state, code, message, hash string
			var pending bool
			err = db.QueryRow(`SELECT j.status,t.status,json_extract(s.payload_json,'$.runtime_finalize_status'),json_extract(s.payload_json,'$.runtime_finalize_error_code'),j.error_message,s.spec_hash,json_extract(s.payload_json,'$.runtime_assets_pending') FROM jobs j JOIN tasks t ON t.job_id=j.job_id JOIN task_specs s ON s.task_id=t.task_id WHERE t.task_id='assembly'`).Scan(&job, &task, &state, &code, &message, &hash, &pending)
			if err != nil {
				t.Fatal(err)
			}
			if job != "FAILED" || task != "FAILED" || state != "FAILED" || code != "PREPARATION_TASK_FAILED" || !pending || hash == "old" || !strings.Contains(message, "WINDOW_ENCODE_FAILED") {
				t.Fatalf("unexpected job/task/state/code/pending/hash/message: %s %s %s %s %v %s %s", job, task, state, code, pending, hash, message)
			}
			var events, history, outboxCount int
			if err := db.QueryRow(`SELECT (SELECT count(*) FROM job_events),(SELECT count(*) FROM job_history),(SELECT count(*) FROM test_outbox)`).Scan(&events, &history, &outboxCount); err != nil {
				t.Fatal(err)
			}
			if events != 1 || history != 1 || outboxCount != 1 {
				t.Fatalf("events/history/outbox=%d/%d/%d", events, history, outboxCount)
			}
			var other string
			if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='other'`).Scan(&other); err != nil || other != "CANCELLED" {
				t.Fatalf("unleased sibling=%s err=%v", other, err)
			}
			n, err = repo.ReconcileFailedRuntimePreparations(context.Background(), 10)
			if err != nil || n != 0 {
				t.Fatalf("replay count=%d err=%v", n, err)
			}
		})
	}
}

func TestRuntimeFailureRechecksDependencyAndRollsBackOutboxFailure(t *testing.T) {
	t.Run("requeued", func(t *testing.T) {
		repo, db := runtimeFailureFixture(t, "READY", "READY", "WAITING_PREPARATION", false)
		changed, err := repo.failRuntimeFinalization(context.Background(), "assembly", "PREPARATION_TASK_FAILED", "", true)
		if err != nil || changed {
			t.Fatalf("changed=%v err=%v", changed, err)
		}
		var status string
		if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-1'`).Scan(&status); err != nil || status != "RUNNING" {
			t.Fatalf("job=%s err=%v", status, err)
		}
	})
	t.Run("outbox", func(t *testing.T) {
		repo, db := runtimeFailureFixture(t, "READY", "FAILED", "WAITING_PREPARATION", true)
		if _, err := repo.ReconcileFailedRuntimePreparations(context.Background(), 10); err == nil {
			t.Fatal("expected outbox failure")
		}
		var job, task, state string
		if err := db.QueryRow(`SELECT j.status,t.status,json_extract(s.payload_json,'$.runtime_finalize_status') FROM jobs j JOIN tasks t ON t.job_id=j.job_id JOIN task_specs s ON s.task_id=t.task_id WHERE t.task_id='assembly'`).Scan(&job, &task, &state); err != nil {
			t.Fatal(err)
		}
		if job != "RUNNING" || task != "READY" || state != "WAITING_PREPARATION" {
			t.Fatalf("partial transition: %s %s %s", job, task, state)
		}
	})
}

func TestRuntimeFinalizerFailureClosesJob(t *testing.T) {
	repo, db := runtimeFailureFixture(t, "READY", "SUCCEEDED", "RUNNING", false)
	if err := repo.FailRuntimeFinalization(context.Background(), "assembly", "RUNTIME_FINALIZE_FAILED", "invalid plan"); err != nil {
		t.Fatal(err)
	}
	var status, code string
	if err := db.QueryRow(`SELECT j.status,json_extract(s.payload_json,'$.runtime_finalize_error_code') FROM jobs j JOIN tasks t ON t.job_id=j.job_id JOIN task_specs s ON s.task_id=t.task_id WHERE t.task_id='assembly'`).Scan(&status, &code); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || code != "RUNTIME_FINALIZE_FAILED" {
		t.Fatalf("job=%s code=%s", status, code)
	}
}

func TestRuntimeFinalizerTransientRetryIsDurableAndBounded(t *testing.T) {
	repo, db := runtimeFailureFixture(t, "READY", "SUCCEEDED", "RUNNING", false)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		scheduled, err := repo.ScheduleRuntimeFinalizationRetry(ctx, "assembly", "network timeout")
		if err != nil || !scheduled {
			t.Fatalf("retry %d: scheduled=%v err=%v", i, scheduled, err)
		}
		var state string
		var count int
		if err := db.QueryRow(`SELECT json_extract(payload_json,'$.runtime_finalize_status'),json_extract(payload_json,'$.runtime_finalize_retry_count') FROM task_specs WHERE task_id='assembly'`).Scan(&state, &count); err != nil {
			t.Fatal(err)
		}
		if state != "RETRY_WAIT" || count != i {
			t.Fatalf("state=%s count=%d", state, count)
		}
		claimed, err := repo.ClaimRuntimeFinalization(ctx, "assembly")
		if err != nil || claimed {
			t.Fatalf("claimed before backoff=%v err=%v", claimed, err)
		}
		if _, err := db.Exec(`UPDATE task_specs SET payload_json=json_set(payload_json,'$.runtime_finalize_retry_at','2000-01-01T00:00:00Z') WHERE task_id='assembly'`); err != nil {
			t.Fatal(err)
		}
		candidates, err := repo.ListRuntimeFinalizationCandidates(ctx, 10)
		if err != nil || len(candidates) != 1 || candidates[0].TaskID != "assembly" {
			t.Fatalf("candidates=%+v err=%v", candidates, err)
		}
		claimed, err = repo.ClaimRuntimeFinalization(ctx, "assembly")
		if err != nil || !claimed {
			t.Fatalf("claim after backoff=%v err=%v", claimed, err)
		}
	}
	scheduled, err := repo.ScheduleRuntimeFinalizationRetry(ctx, "assembly", "network timeout")
	if err != nil || scheduled {
		t.Fatalf("exhausted scheduled=%v err=%v", scheduled, err)
	}
	if err := repo.FailRuntimeFinalization(ctx, "assembly", "RUNTIME_FINALIZE_FAILED", "network timeout"); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-1'`).Scan(&status); err != nil || status != "FAILED" {
		t.Fatalf("job=%s err=%v", status, err)
	}
}
