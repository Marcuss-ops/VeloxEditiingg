// Package store / jobs_repository_metrics.go
//
// Read-only job queries backing the metrics supervisor's job-queue and
// end-to-end-latency projections (jobs.QueueSnapshot /
// jobs.CompletionRecord). They live on baseJobRepository next to the
// other Reader methods so the canonical jobs repository remains the
// single access path to the jobs table.

package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/jobs"
)

// QueueSnapshot returns the instantaneous queue projection: how many
// jobs are waiting (PENDING), how many are in flight (LEASED +
// RUNNING), and how old the oldest waiting job is as of `now`.
//
// `now` is a parameter (not time.Now()) so tests are deterministic and
// the caller owns the clock domain: created_at is Master-local RFC3339
// and the age must be computed against the same clock.
func (b *baseJobRepository) QueueSnapshot(ctx context.Context, now time.Time) (jobs.QueueSnapshot, error) {
	var pending, running int64
	var oldestPending sql.NullString
	err := b.db.QueryRowContext(ctx, `
		SELECT
			SUM(CASE WHEN UPPER(COALESCE(status,'')) = 'PENDING' THEN 1 ELSE 0 END),
			SUM(CASE WHEN UPPER(COALESCE(status,'')) IN ('LEASED','RUNNING') THEN 1 ELSE 0 END),
			MIN(CASE WHEN UPPER(COALESCE(status,'')) = 'PENDING' THEN created_at END)
		FROM jobs`).Scan(&pending, &running, &oldestPending)
	if err != nil {
		return jobs.QueueSnapshot{}, fmt.Errorf("job queue snapshot: %w", err)
	}
	snap := jobs.QueueSnapshot{Pending: pending, Running: running}
	if oldestPending.Valid {
		if created := parseTimeOrZero(oldestPending.String); !created.IsZero() && now.After(created) {
			snap.OldestPendingAge = now.Sub(created)
		}
	}
	return snap, nil
}

// RecentTerminalJobs returns jobs that reached a terminal state
// (SUCCEEDED, FAILED, CANCELLED) at or after `since`, oldest first,
// capped at `limit`. The completion watermark mirrors CountsSince:
// completed_at when present, else updated_at, else created_at.
//
// ORDER BY is the completion watermark ASC so a backlog drains in
// chronological order within a tick (same rationale as
// SQLiteLabelResolver.RecentAttemptIDs).
func (b *baseJobRepository) RecentTerminalJobs(ctx context.Context, since time.Time, limit int) ([]jobs.CompletionRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := b.db.QueryContext(ctx, `
		SELECT job_id,
		       UPPER(COALESCE(status,'')),
		       COALESCE(created_at,''),
		       COALESCE(started_at,''),
		       COALESCE(completed_at,'')
		FROM jobs
		WHERE UPPER(COALESCE(status,'')) IN ('SUCCEEDED','FAILED','CANCELLED')
		  AND COALESCE(NULLIF(completed_at,''), NULLIF(updated_at,''), created_at) >= ?
		ORDER BY COALESCE(NULLIF(completed_at,''), NULLIF(updated_at,''), created_at) ASC
		LIMIT ?`,
		since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("recent terminal jobs: %w", err)
	}
	defer rows.Close()
	var out []jobs.CompletionRecord
	for rows.Next() {
		var (
			id, status, createdAt, startedAt, completedAt string
		)
		if err := rows.Scan(&id, &status, &createdAt, &startedAt, &completedAt); err != nil {
			return nil, fmt.Errorf("recent terminal jobs scan: %w", err)
		}
		out = append(out, jobs.CompletionRecord{
			ID:          id,
			Status:      jobs.JobStatus(status),
			CreatedAt:   parseTimeOrZero(createdAt),
			StartedAt:   parseTimeOrZero(startedAt),
			CompletedAt: parseTimeOrZero(completedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recent terminal jobs iterate: %w", err)
	}
	return out, nil
}
