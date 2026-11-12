package jobdoctor

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func seedDoctorFailureFixture(t *testing.T, db *sql.DB, jobStatus, taskStatus, executor, payload string) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE jobs(job_id TEXT PRIMARY KEY,status TEXT,last_error TEXT,error_message TEXT);
		CREATE TABLE tasks(task_id TEXT PRIMARY KEY,job_id TEXT,executor_id TEXT,executor_version INTEGER,status TEXT,worker_id TEXT,lease_id TEXT,revision INTEGER,attempt_count INTEGER,created_at TEXT,updated_at TEXT);
		CREATE TABLE task_specs(task_id TEXT,payload_json TEXT);
		CREATE TABLE task_attempts(id TEXT,task_id TEXT,attempt_number INTEGER,status TEXT,worker_id TEXT,lease_id TEXT,updated_at TEXT,error_code TEXT,error_message TEXT);
		INSERT INTO jobs VALUES('job',?,'','job lifecycle error');
		INSERT INTO tasks VALUES('task','job',?,1,?,'worker','lease',1,1,'2026-11-09T11:00:00Z','2026-11-09T11:00:00Z');
		INSERT INTO task_specs VALUES('task',?);
		INSERT INTO task_attempts VALUES('attempt','task',1,'FAILED','worker','lease','2026-11-09T11:00:00Z','WINDOW_ENCODE_FAILED','bad source');`, jobStatus, executor, taskStatus, payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiagnoseTerminalAndStaleFinalizationFailure(t *testing.T) {
	for _, status := range []string{"FAILED", "RUNNING"} {
		t.Run(status, func(t *testing.T) {
			db := openDoctorTestDB(t)
			seedDoctorFailureFixture(t, db, status, "FAILED", "video.prepare.window.v1", `{"runtime_assets_pending":true,"runtime_finalize_status":"FAILED","runtime_finalize_error_code":"PREPARATION_TASK_FAILED","runtime_finalize_error":"bad source"}`)
			d, err := Diagnose(context.Background(), db, "job", time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC), DefaultThresholds())
			if err != nil {
				t.Fatal(err)
			}
			want := VerdictStalled
			if status == "FAILED" {
				want = VerdictBlocked
			}
			if d.Verdict != want || d.Cause != CauseRuntimeFinalizeFailed || !strings.Contains(d.CauseDetail, "PREPARATION_TASK_FAILED") || len(d.FailedTasks) != 1 || d.FailedTasks[0].ErrorCode != "WINDOW_ENCODE_FAILED" {
				t.Fatalf("diagnosis=%+v", d)
			}
		})
	}
}

func TestDiagnoseDoesNotMislabelOtherTaskFailure(t *testing.T) {
	db := openDoctorTestDB(t)
	seedDoctorFailureFixture(t, db, "RUNNING", "FAILED", "video.assemble.copy.v1", `{}`)
	d, err := Diagnose(context.Background(), db, "job", time.Now(), DefaultThresholds())
	if err != nil {
		t.Fatal(err)
	}
	if d.Cause != CauseTaskFailed {
		t.Fatalf("cause=%s want=%s", d.Cause, CauseTaskFailed)
	}
}

func TestDiagnoseFutureFinalizationRetryIsAdvancing(t *testing.T) {
	db := openDoctorTestDB(t)
	seedDoctorFailureFixture(t, db, "RUNNING", "READY", "video.assemble.copy.v1", `{"runtime_assets_pending":true,"runtime_finalize_status":"RETRY_WAIT","runtime_finalize_retry_at":"2026-11-09T12:00:05Z"}`)
	d, err := Diagnose(context.Background(), db, "job", time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC), DefaultThresholds())
	if err != nil {
		t.Fatal(err)
	}
	if d.Verdict != VerdictAdvancing || !strings.Contains(d.CauseDetail, "retry") {
		t.Fatalf("diagnosis=%+v", d)
	}
}
