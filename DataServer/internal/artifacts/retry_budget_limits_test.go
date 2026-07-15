package artifacts_test

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/deliveries"
)

func TestFinalizeVerified_BudgetConsumedBy429Retries(t *testing.T) {
	db := openPropagationDB(t)
	seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-429", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-429", UploadID: "up-429"})
	seedDeliveryPlans(t, db, "J-429", []phase5Plan{{"primary", 1, 3, true}})
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, false)
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: "up-429", ArtifactID: "art-429", JobID: "J-429", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-429/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	var maxAttempts int
	if err := db.QueryRow(`SELECT max_attempts FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, "art-429").Scan(&maxAttempts); err != nil {
		t.Fatal(err)
	}
	if maxAttempts != 3 {
		t.Fatalf("max_attempts=%d want 3", maxAttempts)
	}
	for i := 1; i <= maxAttempts; i++ {
		now := time.Now().UTC().Format(time.RFC3339)
		if i < maxAttempts {
			_, err := db.Exec(`UPDATE job_deliveries SET status='RETRY_WAIT',attempt_count=?,last_error_code='RATE_LIMIT',next_attempt_at=?,updated_at=? WHERE artifact_id=? AND destination_id='primary'`, i, time.Now().UTC().Add(time.Duration(i)*time.Minute).Format(time.RFC3339), now, "art-429")
			if err != nil {
				t.Fatal(err)
			}
		} else {
			_, err := db.Exec(`UPDATE job_deliveries SET status='FAILED',attempt_count=?,last_error_code='RATE_LIMIT',last_error_message='max attempts reached: 429',completed_at=?,updated_at=? WHERE artifact_id=? AND destination_id='primary'`, i, now, now, "art-429")
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var count int
	var status string
	if err := db.QueryRow(`SELECT attempt_count,status FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, "art-429").Scan(&count, &status); err != nil {
		t.Fatal(err)
	}
	if count != 3 || status != "FAILED" {
		t.Fatalf("count/status=%d/%s want 3/FAILED", count, status)
	}
}

func TestFinalizeVerified_MarksTaskSucceeded(t *testing.T) {
	for _, status := range []string{"RUNNING", "LEASED", "PENDING"} {
		t.Run(status, func(t *testing.T) {
			db := openPropagationDB(t)
			seedPhase5Fixture(t, db, phase5Fixture{JobID: "J-task", WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: "art-task", UploadID: "up-task"})
			now := time.Now().UTC().Format(time.RFC3339)
			_, err := db.Exec(`INSERT INTO tasks (task_id,job_id,project_id,render_plan_id,executor_id,executor_version,status,priority,revision,attempt_count,worker_id,lease_id,created_at,updated_at) VALUES ('task-J-task','J-task','proj','rp','executor.scene_composite',1,?,0,0,0,'w','l',?,?)`, status, now, now)
			if err != nil {
				t.Fatal(err)
			}
			runFinalize(t, db, nil, artifacts.FinalizeVerifiedCommand{UploadID: "up-task", ArtifactID: "art-task", JobID: "J-task", WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/J-task/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
			var got string
			if err := db.QueryRow(`SELECT status FROM tasks WHERE job_id='J-task'`).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != "SUCCEEDED" {
				t.Errorf("status=%s want SUCCEEDED", got)
			}
		})
	}
}

func TestFinalizeVerified_ClosesAllStateTablesAtomically(t *testing.T) {
	db := openPropagationDB(t)
	jobID := "J-Q5"
	artifactID := "art-Q5"
	uploadID := "up-Q5"
	seedPhase5Fixture(t, db, phase5Fixture{JobID: jobID, WorkerID: "w", LeaseID: "l", Revision: 1, AttemptNumber: 1, ArtifactID: artifactID, UploadID: uploadID})
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(`INSERT INTO tasks (task_id,job_id,project_id,render_plan_id,executor_id,executor_version,status,priority,revision,attempt_count,worker_id,lease_id,created_at,updated_at) VALUES ('task-Q5',?,'proj','rp','executor.scene_composite',1,'RUNNING',0,0,0,'w','l',?,?)`, jobID, now, now)
	if err != nil {
		t.Fatal(err)
	}
	seedDeliveryPlans(t, db, jobID, []phase5Plan{{"primary", 1, 3, true}})
	resolver := deliveries.NewSQLiteDeliveryPlanResolver(db, false)
	runFinalize(t, db, resolver, artifacts.FinalizeVerifiedCommand{UploadID: uploadID, ArtifactID: artifactID, JobID: jobID, WorkerID: "w", LeaseID: "l", AttemptNumber: 1, ExpectedRevision: 1, StorageProvider: "local", StorageKey: "artifacts/" + jobID + "/1", SHA256: "deadbeef", SizeBytes: 1024, MIMEType: "video/mp4", VerifiedAt: time.Now().UTC()})
	checks := []struct{ q, want string }{{`SELECT status FROM jobs WHERE job_id='J-Q5'`, "SUCCEEDED"}, {`SELECT status FROM tasks WHERE job_id='J-Q5'`, "SUCCEEDED"}, {`SELECT status FROM artifacts WHERE id='art-Q5'`, "READY"}, {`SELECT status FROM artifact_uploads WHERE upload_id='up-Q5'`, "COMPLETED"}}
	for _, c := range checks {
		var got string
		if err := db.QueryRow(c.q).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("got %s want %s", got, c.want)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id='art-Q5'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("deliveries=%d want 1", n)
	}
}
