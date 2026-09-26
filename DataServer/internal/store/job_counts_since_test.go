package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/jobs"
)

func TestJobCountsSinceUsesCompletionWindowAndExcludesCancelled(t *testing.T) {
	s, repo := openTransitionTestDB(t)
	cutoff := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	rows := []struct{ id, status, completed string }{
		{"recent-success", "SUCCEEDED", "2026-09-26T10:00:00Z"},
		{"recent-failure", "FAILED", "2026-09-26T11:00:00Z"},
		{"old-success", "SUCCEEDED", "2026-09-24T11:59:59Z"},
		{"cancelled", "CANCELLED", "2026-09-26T11:30:00Z"},
	}
	for _, row := range rows {
		if _, err := s.db.Exec(`INSERT INTO jobs(job_id,status,revision,max_retries,created_at,updated_at,completed_at) VALUES(?,?,0,0,?,?,?)`, row.id, row.status, row.completed, row.completed, row.completed); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := repo.CountsSince(context.Background(), cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if counts[jobs.StatusSucceeded] != 1 || counts[jobs.StatusFailed] != 1 {
		t.Fatalf("24h counts = %#v, want one success and one failure", counts)
	}
	if counts[jobs.StatusCancelled] != 0 {
		t.Fatalf("cancelled count = %d, want excluded", counts[jobs.StatusCancelled])
	}
}
