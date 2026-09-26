package store

import (
	"context"
	"testing"
	"time"
)

func TestPruneJobEventsRetention(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/job-events-retention.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	old := now.Add(-31 * 24 * time.Hour).Format(time.RFC3339Nano)
	recent := now.Add(-time.Hour).Format(time.RFC3339Nano)
	for _, job := range []struct{ id, status string }{
		{"job-terminal-old", "SUCCEEDED"},
		{"job-active-old", "RUNNING"},
		{"job-terminal-recent", "FAILED"},
	} {
		if _, err := s.db.Exec(`INSERT INTO jobs (job_id,status,created_at,updated_at,migrated_at) VALUES (?,?,?,?,?)`, job.id, job.status, old, old, old); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range []struct{ jobID, timestamp string }{
		{"job-terminal-old", old},
		{"job-active-old", old},
		{"job-terminal-recent", recent},
		{"deleted-job", old},
	} {
		if _, err := s.db.Exec(`INSERT INTO job_events (timestamp,job_id,event,raw_json) VALUES (?,?,'test','{}')`, event.timestamp, event.jobID); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pruneJobEvents(context.Background(), tx, 30, now); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM job_events`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("remaining job_events=%d, want 2 (active-old and terminal-recent)", remaining)
	}
}
