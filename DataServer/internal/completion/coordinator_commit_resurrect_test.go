package completion

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestCommitAttempt_ResurrectsFailedAttemptFromResumeLoopRace pins the
// master-side race fix: a durable-outbox TaskResult "failed" that lands
// BEFORE the resume-loop commit must not block that commit. The commit
// resurrects the FAILED attempt + task + job in ONE transaction when the
// attempt_commits row proves all required outputs are READY.
func TestCommitAttempt_ResurrectsFailedAttemptFromResumeLoopRace(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-rs", "attempt-rs")
	jobID := "job-rs"
	seedCompleteUploadFixture(t, db, "up-rs", "art-rs", jobID, strings.Repeat("a", 64))

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: jobID, OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	commitID := scheduleRowReady(t, db, fence, "art-rs")

	// Simulate the durable-outbox TaskResult "failed" landing first: the
	// ingest would have flipped task + attempt + job to FAILED while the
	// attempt_commits row stays DECLARED/RECEIVED with ready >= required.
	if _, err := db.Exec(`UPDATE tasks SET status='FAILED' WHERE task_id='task-rs'`); err != nil {
		t.Fatalf("inject task FAILED: %v", err)
	}
	if _, err := db.Exec(`UPDATE task_attempts SET status='FAILED', error_code='UPLOAD_INTERRUPTED', error_message='transport closed' WHERE id='attempt-rs'`); err != nil {
		t.Fatalf("inject attempt FAILED: %v", err)
	}
	if _, err := db.Exec(`UPDATE jobs SET status='FAILED' WHERE job_id='job-rs'`); err != nil {
		t.Fatalf("inject job FAILED: %v", err)
	}

	if _, err := c.CommitAttempt(context.Background(), commitID); err != nil {
		t.Fatalf("CommitAttempt must resurrect FAILED attempt/task/job: %v", err)
	}

	// All three lifecycles resurrected to SUCCEEDED in one transaction.
	assertStatus(t, db, `SELECT status FROM task_attempts WHERE id='attempt-rs'`, "SUCCEEDED", "attempt")
	assertStatus(t, db, `SELECT status FROM tasks WHERE task_id='task-rs'`, "SUCCEEDED", "task")
	assertStatus(t, db, `SELECT status FROM jobs WHERE job_id='job-rs'`, "SUCCEEDED", "job")

	// The stale failure reason must be cleared on resurrection.
	var ec, em string
	if err := db.QueryRow(`SELECT COALESCE(error_code,''), COALESCE(error_message,'') FROM task_attempts WHERE id='attempt-rs'`).Scan(&ec, &em); err != nil {
		t.Fatalf("read attempt error fields: %v", err)
	}
	if ec != "" || em != "" {
		t.Errorf("resurrected attempt still carries error_code=%q error_message=%q; want empty", ec, em)
	}

	if row := readAttemptCommitRow(t, db, fence); row.Status != "COMMITTED" {
		t.Errorf("attempt_commits.status = %q, want COMMITTED", row.Status)
	}
}

// TestCommitAttempt_DoesNotResurrectTimedOutAttempt guards the boundary of
// the resurrection: a master/operator-driven TIMED_OUT attempt is never
// resurrected by a late commit, even with a READY attempt_commits row.
func TestCommitAttempt_DoesNotResurrectTimedOutAttempt(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-rs-tmo", "attempt-rs-tmo")
	jobID := "job-rs-tmo"
	seedCompleteUploadFixture(t, db, "up-rs-tmo", "art-rs-tmo", jobID, strings.Repeat("a", 64))

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: jobID, OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	commitID := scheduleRowReady(t, db, fence, "art-rs-tmo")

	if _, err := db.Exec(`UPDATE task_attempts SET status='TIMED_OUT' WHERE id='attempt-rs-tmo'`); err != nil {
		t.Fatalf("inject attempt TIMED_OUT: %v", err)
	}

	if _, err := c.CommitAttempt(context.Background(), commitID); err == nil {
		t.Fatalf("CommitAttempt must NOT resurrect a TIMED_OUT attempt; got nil err")
	}
	assertStatus(t, db, `SELECT status FROM task_attempts WHERE id='attempt-rs-tmo'`, "TIMED_OUT", "attempt")
}

func assertStatus(t *testing.T, db *sql.DB, query, want, label string) {
	t.Helper()
	var got string
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("read %s status: %v", label, err)
	}
	if got != want {
		t.Errorf("%s status = %q, want %q", label, got, want)
	}
}
