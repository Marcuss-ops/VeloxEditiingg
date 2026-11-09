package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
)

func openDoctorCLIDB(t *testing.T) (*store.SQLiteStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doctor-db.sqlite")
	db, err := store.NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func insertDoctorJob(t *testing.T, db *store.SQLiteStore, jobID, status string, updated time.Time) {
	t.Helper()
	if _, err := db.DB().Exec(`
		INSERT INTO jobs(job_id,status,created_at,updated_at,migrated_at)
		VALUES(?,?,?,?,?)`,
		jobID, status, updated.UTC().Format(time.RFC3339), updated.UTC().Format(time.RFC3339), updated.UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert job: %v", err)
	}
}

func insertDoctorTask(t *testing.T, db *store.SQLiteStore, jobID, taskID, executorID, status string, updated time.Time) {
	t.Helper()
	if _, err := db.DB().Exec(`
		INSERT INTO tasks(task_id,job_id,status,priority,revision,attempt_count,executor_id,executor_version,worker_id,lease_id,created_at,updated_at)
		VALUES(?,?,?,0,1,0,?,1,'','',?,?)`,
		taskID, jobID, status, executorID,
		updated.Add(-time.Minute).UTC().Format(time.RFC3339), updated.UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert task: %v", err)
	}
}

func insertDoctorSpec(t *testing.T, db *store.SQLiteStore, taskID, executorID, payload string, createdAt time.Time) {
	t.Helper()
	if _, err := db.DB().Exec(`
		INSERT INTO task_specs(task_id,spec_version,spec_hash,executor_id,payload_json,created_at)
		VALUES(?,1,'hash',?,?,?)`,
		taskID, executorID, payload, createdAt.UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert spec: %v", err)
	}
}

func insertDoctorReservation(t *testing.T, db *store.SQLiteStore, taskID, jobID string, expires time.Time) {
	t.Helper()
	if _, err := db.DB().Exec(`
		INSERT INTO future_task_reservations(task_id,job_id,worker_id,reservation_id,task_revision,distance,state,expires_at,created_at,updated_at)
		VALUES(?,?,'w-1',?,1,1,'PREPARING',?,?,?)`,
		taskID, jobID, "res-"+taskID,
		expires.UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert reservation: %v", err)
	}
}

func insertDoctorWorker(t *testing.T, db *store.SQLiteStore) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.DB().Exec(`
		INSERT INTO workers(worker_id,worker_name,status,last_heartbeat,schedulable,drain,worker_group,raw_json,migrated_at,connection_status,session_active,active_task_count,task_slots,last_heartbeat_at)
		VALUES('w-1','worker one','CONNECTED',?,1,0,'default','{}',?, 'CONNECTED',1,0,4,?)`,
		now, now, now); err != nil {
		t.Fatalf("insert worker: %v", err)
	}
}

func TestRunDoctorStalledPreparationGate(t *testing.T) {
	db, path := openDoctorCLIDB(t)
	now := time.Now().UTC().Add(-5 * time.Minute)
	insertDoctorJob(t, db, "job-prep", "RUNNING", now)
	insertDoctorTask(t, db, "job-prep", "task-prep", "video.assemble.slice.v1", "READY", now)
	insertDoctorSpec(t, db, "task-prep", "video.assemble.slice.v1",
		`{"runtime_assets_pending":true,"runtime_finalize_status":"WAITING_PREPARATION"}`, now)
	insertDoctorReservation(t, db, "task-prep", "job-prep", time.Now().UTC().Add(10*time.Minute))
	insertDoctorWorker(t, db)

	var stdout, stderr bytes.Buffer
	if err := run([]string{"doctor", "--db", path, "--job", "job-prep", "--json"}, &stdout, &stderr); err != nil {
		t.Fatalf("run doctor: %v stderr=%q", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{`"verdict": "STALLED"`, `"cause": "PREPARATION_GATE_PENDING"`, `"ready_tasks": 1`, `"workers_available": 1`} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %s:\n%s", want, out)
		}
	}
}

func TestRunDoctorExpiredReservation(t *testing.T) {
	db, path := openDoctorCLIDB(t)
	now := time.Now().UTC().Add(-5 * time.Minute)
	insertDoctorJob(t, db, "job-expired", "RUNNING", now)
	insertDoctorTask(t, db, "job-expired", "task-expired", "video.assemble.slice.v1", "READY", now)
	insertDoctorSpec(t, db, "task-expired", "video.assemble.slice.v1", `{"runtime_assets_pending":true}`, now)
	insertDoctorReservation(t, db, "task-expired", "job-expired", time.Now().UTC().Add(-time.Minute))
	insertDoctorWorker(t, db)

	var stdout, stderr bytes.Buffer
	if err := run([]string{"doctor", "--db", path, "--job", "job-expired", "--json"}, &stdout, &stderr); err != nil {
		t.Fatalf("run doctor: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"cause": "RESERVATION_EXPIRED"`) {
		t.Fatalf("output=%s", stdout.String())
	}
}

func TestRunDoctorCompleteJob(t *testing.T) {
	db, path := openDoctorCLIDB(t)
	now := time.Now().UTC()
	insertDoctorJob(t, db, "job-done", "SUCCEEDED", now)
	insertDoctorTask(t, db, "job-done", "task-done", "video.assemble.copy.v1", "SUCCEEDED", now)

	var stdout, stderr bytes.Buffer
	if err := run([]string{"doctor", "--db", path, "--job", "job-done"}, &stdout, &stderr); err != nil {
		t.Fatalf("run doctor: %v stderr=%q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "VERDICT COMPLETE") || !strings.Contains(stdout.String(), "cause: none") {
		t.Fatalf("output=%s", stdout.String())
	}
}

func TestRunDoctorInsufficientEvidence(t *testing.T) {
	db, path := openDoctorCLIDB(t)
	insertDoctorJob(t, db, "job-empty", "PENDING", time.Now().UTC())

	var stdout, stderr bytes.Buffer
	if err := run([]string{"doctor", "--db", path, "--job", "job-empty", "--json"}, &stdout, &stderr); err != nil {
		t.Fatalf("run doctor: %v stderr=%q", err, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"verdict": "UNKNOWN"`) || !strings.Contains(out, `"cause": "INSUFFICIENT_EVIDENCE"`) || !strings.Contains(out, `"insufficient_evidence": true`) {
		t.Fatalf("output=%s", out)
	}
}

func TestRunDoctorRequiresDatabaseAndJob(t *testing.T) {
	for _, args := range [][]string{
		{"doctor"},
		{"doctor", "--db", "/tmp/velox.db"},
	} {
		var stdout, stderr bytes.Buffer
		err := run(args, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "required") {
			t.Fatalf("args=%v err=%v stderr=%q", args, err, stderr.String())
		}
	}
}
