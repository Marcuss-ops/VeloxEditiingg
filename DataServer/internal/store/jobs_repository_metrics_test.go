package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/jobs"
)

// TestQueueSnapshotCountsPendingRunningAndOldestAge locks the job-queue
// projection behind velox_jobs_pending / velox_jobs_running /
// velox_jobs_oldest_pending_age_seconds: pending counts PENDING only,
// running counts the in-flight states (LEASED + RUNNING), and the
// oldest age is measured against the caller's clock from the oldest
// PENDING row — a depth-only gauge cannot express the "1 job waiting 7
// minutes" incident this age exists for.
func TestQueueSnapshotCountsPendingRunningAndOldestAge(t *testing.T) {
	s, repo := openTransitionTestDB(t)
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		id, status, createdAt string
	}{
		{"p-old", "PENDING", now.Add(-7 * time.Minute).Format(time.RFC3339)},
		{"p-new", "PENDING", now.Add(-30 * time.Second).Format(time.RFC3339)},
		{"leased", "LEASED", now.Add(-2 * time.Minute).Format(time.RFC3339)},
		{"running", "RUNNING", now.Add(-time.Minute).Format(time.RFC3339)},
		{"done", "SUCCEEDED", now.Add(-time.Hour).Format(time.RFC3339)},
		{"dead", "FAILED", now.Add(-time.Hour).Format(time.RFC3339)},
	}
	for _, row := range rows {
		if _, err := s.db.Exec(
			`INSERT INTO jobs(job_id,status,revision,max_retries,created_at,updated_at) VALUES(?,?,0,0,?,?)`,
			row.id, row.status, row.createdAt, row.createdAt,
		); err != nil {
			t.Fatal(err)
		}
	}

	snapshot, err := repo.QueueSnapshot(context.Background(), now)
	if err != nil {
		t.Fatalf("QueueSnapshot: %v", err)
	}
	if snapshot.Pending != 2 {
		t.Errorf("Pending = %d, want 2", snapshot.Pending)
	}
	if snapshot.Running != 2 {
		t.Errorf("Running = %d, want 2 (LEASED + RUNNING)", snapshot.Running)
	}
	if want := 7 * time.Minute; snapshot.OldestPendingAge != want {
		t.Errorf("OldestPendingAge = %s, want %s", snapshot.OldestPendingAge, want)
	}
}

// TestQueueSnapshotEmptyQueueIsZero locks the honest-empty semantics:
// no pending job ⇒ age 0 (not a stale carry-over from a previous
// reading, since gauges are set-to-current-value every tick).
func TestQueueSnapshotEmptyQueueIsZero(t *testing.T) {
	s, repo := openTransitionTestDB(t)
	if _, err := s.db.Exec(
		`INSERT INTO jobs(job_id,status,revision,max_retries,created_at,updated_at) VALUES('only-running','RUNNING',0,0,?,?)`,
		time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		t.Fatal(err)
	}
	snapshot, err := repo.QueueSnapshot(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatalf("QueueSnapshot: %v", err)
	}
	if snapshot.Pending != 0 || snapshot.Running != 1 || snapshot.OldestPendingAge != 0 {
		t.Fatalf("snapshot = %+v, want no pending job and zero age", snapshot)
	}
}

// TestRecentTerminalJobsUsesCompletionWatermarkAndOrder locks the input
// of the e2e latency histogram: only terminal jobs at/after the
// watermark come back, oldest first, with the three lifecycle
// timestamps the phase attribution needs (created → started →
// completed).
func TestRecentTerminalJobsUsesCompletionWatermarkAndOrder(t *testing.T) {
	s, repo := openTransitionTestDB(t)
	created := time.Date(2026, 9, 28, 11, 0, 0, 0, time.UTC)
	started := created.Add(time.Minute)
	succeeded := started.Add(4 * time.Minute)

	insert := func(id, status string, c, st, done time.Time) {
		t.Helper()
		if _, err := s.db.Exec(
			`INSERT INTO jobs(job_id,status,revision,max_retries,created_at,started_at,completed_at,updated_at) VALUES(?,?,0,0,?,?,?,?)`,
			id, status, c.Format(time.RFC3339), st.Format(time.RFC3339), done.Format(time.RFC3339), done.Format(time.RFC3339),
		); err != nil {
			t.Fatal(err)
		}
	}
	insert("first", "SUCCEEDED", created, started, succeeded)
	insert("second", "FAILED", created, started, succeeded.Add(time.Minute))
	insert("cancelled", "CANCELLED", created, started, succeeded.Add(2*time.Minute))
	insert("old", "SUCCEEDED", created, started, succeeded.Add(-time.Hour))
	insert("still-running", "RUNNING", created, started, time.Time{})

	records, err := repo.RecentTerminalJobs(context.Background(), succeeded.Add(-time.Second), 10)
	if err != nil {
		t.Fatalf("RecentTerminalJobs: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("records = %d (%+v), want 3 terminal jobs inside the window", len(records), records)
	}
	if records[0].ID != "first" || records[1].ID != "second" || records[2].ID != "cancelled" {
		t.Fatalf("order = %s, %s, %s; want oldest-completion first", records[0].ID, records[1].ID, records[2].ID)
	}
	rec := records[0]
	if rec.Status != jobs.StatusSucceeded {
		t.Errorf("status = %q, want SUCCEEDED", rec.Status)
	}
	if !rec.CreatedAt.Equal(created) || !rec.StartedAt.Equal(started) || !rec.CompletedAt.Equal(succeeded) {
		t.Errorf("timestamps = %v → %v → %v, want %v → %v → %v",
			rec.CreatedAt, rec.StartedAt, rec.CompletedAt, created, started, succeeded)
	}
}
