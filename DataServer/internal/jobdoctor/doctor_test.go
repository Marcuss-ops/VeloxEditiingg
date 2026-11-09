package jobdoctor

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openDoctorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+t.TempDir()+"/doctor.db?mode=rwc&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDiagnoseReportsFenceConflictWithoutOptionalTables(t *testing.T) {
	db := openDoctorTestDB(t)
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT, last_error TEXT)`,
		`CREATE TABLE tasks (task_id TEXT PRIMARY KEY, job_id TEXT, executor_id TEXT, executor_version INTEGER, status TEXT, worker_id TEXT, lease_id TEXT, revision INTEGER, attempt_count INTEGER, created_at TEXT, updated_at TEXT)`,
		`INSERT INTO jobs(job_id,status) VALUES('job-fence','RUNNING')`,
		`INSERT INTO tasks(task_id,job_id,executor_id,executor_version,status,worker_id,lease_id,revision,attempt_count,created_at,updated_at)
		 VALUES('task-fence','job-fence','video.assemble.copy.v1',1,'RUNNING','w-1','l-1',1,1,'2026-11-09T11:59:00Z','2026-11-09T11:59:30Z')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}

	diagnosis, err := Diagnose(context.Background(), db, "job-fence", now, DefaultThresholds())
	if err != nil {
		t.Fatalf("Diagnose: %v", err)
	}
	if diagnosis.Verdict != VerdictStalled || diagnosis.Cause != CauseFenceConflict {
		t.Fatalf("verdict=%s cause=%s, want STALLED/FENCE_CONFLICT", diagnosis.Verdict, diagnosis.Cause)
	}
	if len(diagnosis.Warnings) == 0 {
		t.Fatal("missing optional tables must surface as warnings")
	}
	foundAttemptWarning := false
	for _, warning := range diagnosis.Warnings {
		if strings.Contains(warning, "task_attempts") {
			foundAttemptWarning = true
		}
	}
	if !foundAttemptWarning {
		t.Fatalf("warnings=%v, want a task_attempts warning", diagnosis.Warnings)
	}
}

func TestDiagnoseExpiredReservationOutranksNoProgress(t *testing.T) {
	db := openDoctorTestDB(t)
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT, last_error TEXT)`,
		`CREATE TABLE tasks (task_id TEXT PRIMARY KEY, job_id TEXT, executor_id TEXT, executor_version INTEGER, status TEXT, worker_id TEXT, lease_id TEXT, revision INTEGER, attempt_count INTEGER, created_at TEXT, updated_at TEXT)`,
		`CREATE TABLE future_task_reservations (task_id TEXT PRIMARY KEY, job_id TEXT, worker_id TEXT, reservation_id TEXT, task_revision INTEGER, distance INTEGER, state TEXT, expires_at TEXT)`,
		`INSERT INTO jobs(job_id,status) VALUES('job-expired','RUNNING')`,
		`INSERT INTO tasks(task_id,job_id,executor_id,executor_version,status,worker_id,lease_id,revision,attempt_count,created_at,updated_at)
		 VALUES('task-expired','job-expired','video.assemble.slice.v1',1,'READY','','',1,0,'2026-11-09T11:00:00Z','2026-11-09T11:00:00Z')`,
		`INSERT INTO future_task_reservations(task_id,job_id,worker_id,reservation_id,task_revision,distance,state,expires_at)
		 VALUES('task-expired','job-expired','w-1','res-1',1,1,'PREPARING','2026-11-09T11:30:00Z')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}

	diagnosis, err := Diagnose(context.Background(), db, "job-expired", now, DefaultThresholds())
	if err != nil {
		t.Fatalf("Diagnose: %v", err)
	}
	if diagnosis.Verdict != VerdictStalled || diagnosis.Cause != CauseReservationExpired {
		t.Fatalf("verdict=%s cause=%s, want STALLED/RESERVATION_EXPIRED", diagnosis.Verdict, diagnosis.Cause)
	}
}

func TestDiagnoseMissingJob(t *testing.T) {
	db := openDoctorTestDB(t)
	if _, err := db.Exec(`CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT, last_error TEXT)`); err != nil {
		t.Fatal(err)
	}
	_, err := Diagnose(context.Background(), db, "missing", time.Now().UTC(), DefaultThresholds())
	if err != ErrJobNotFound {
		t.Fatalf("err=%v, want ErrJobNotFound", err)
	}
}
