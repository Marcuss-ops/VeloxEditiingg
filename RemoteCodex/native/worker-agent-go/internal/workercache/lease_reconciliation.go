package workercache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// LeaseReleaseReconciliation is one durable lease-release task. The row is
// retained until the corresponding lease release succeeds (or the asset row
// is already gone), so a worker restart cannot strand protection indefinitely.
type LeaseReleaseReconciliation struct {
	AssetKey      string
	JobID         string
	AttemptCount  int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// EnqueueLeaseRelease durably records a failed lease release. The composite
// primary key makes repeated enqueue attempts idempotent and preserves the
// existing retry schedule rather than resetting backoff.
func (c *Cache) EnqueueLeaseRelease(ctx context.Context, assetKey, jobID string, now time.Time) error {
	if c == nil || c.db == nil {
		return errors.New("workercache.EnqueueLeaseRelease: cache is nil")
	}
	if assetKey == "" || jobID == "" {
		return errors.New("workercache.EnqueueLeaseRelease: asset_key and job_id are required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	stamp := now.UTC().Format(time.RFC3339Nano)
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO pending_lease_releases
		    (asset_key, job_id, attempt_count, next_attempt_at, last_error, created_at, updated_at)
		VALUES (?, ?, 0, ?, '', ?, ?)
		ON CONFLICT(asset_key, job_id) DO NOTHING`, assetKey, jobID, stamp, stamp, stamp)
	if err != nil {
		return fmt.Errorf("workercache.EnqueueLeaseRelease(%q,%q): %w", assetKey, jobID, err)
	}
	return nil
}

// ListDueLeaseReleases returns a bounded, oldest-first batch ready for
// reconciliation. It is safe to call after a restart; all retry metadata is
// persisted in the cache database. Production owns one reconciler loop per
// Worker session, and runSession waits for that loop before reconnecting, so
// the queue has a single consumer; callers that create additional consumers
// must provide their own claim/fencing policy.
func (c *Cache) ListDueLeaseReleases(ctx context.Context, now time.Time, limit int) ([]LeaseReleaseReconciliation, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("workercache.ListDueLeaseReleases: cache is nil")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := c.db.QueryContext(ctx, `
		SELECT asset_key, job_id, attempt_count, next_attempt_at, last_error, created_at, updated_at
		  FROM pending_lease_releases
		 WHERE next_attempt_at <= ?
		 ORDER BY next_attempt_at ASC, created_at ASC
		 LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("workercache.ListDueLeaseReleases: %w", err)
	}
	defer rows.Close()

	entries := make([]LeaseReleaseReconciliation, 0)
	for rows.Next() {
		var entry LeaseReleaseReconciliation
		var nextAt, createdAt, updatedAt string
		if err := rows.Scan(&entry.AssetKey, &entry.JobID, &entry.AttemptCount, &nextAt, &entry.LastError, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("workercache.ListDueLeaseReleases: scan: %w", err)
		}
		if entry.NextAttemptAt, err = parseRFC3339Nano(nextAt); err != nil {
			return nil, fmt.Errorf("workercache.ListDueLeaseReleases: next_attempt_at: %w", err)
		}
		if entry.CreatedAt, err = parseRFC3339Nano(createdAt); err != nil {
			return nil, fmt.Errorf("workercache.ListDueLeaseReleases: created_at: %w", err)
		}
		if entry.UpdatedAt, err = parseRFC3339Nano(updatedAt); err != nil {
			return nil, fmt.Errorf("workercache.ListDueLeaseReleases: updated_at: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workercache.ListDueLeaseReleases: rows: %w", err)
	}
	return entries, nil
}

// MarkLeaseReleaseRetry advances durable retry metadata after a failed
// reconciliation attempt. A future worker session will honor NextAttemptAt.
func (c *Cache) MarkLeaseReleaseRetry(ctx context.Context, assetKey, jobID, lastError string, attemptCount int, nextAttemptAt, now time.Time) error {
	if c == nil || c.db == nil {
		return errors.New("workercache.MarkLeaseReleaseRetry: cache is nil")
	}
	if assetKey == "" || jobID == "" {
		return errors.New("workercache.MarkLeaseReleaseRetry: asset_key and job_id are required")
	}
	if attemptCount < 0 {
		attemptCount = 0
	}
	if nextAttemptAt.IsZero() {
		nextAttemptAt = time.Now().UTC()
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result, err := c.db.ExecContext(ctx, `
		UPDATE pending_lease_releases
		   SET attempt_count = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
		 WHERE asset_key = ? AND job_id = ?`,
		attemptCount, nextAttemptAt.UTC().Format(time.RFC3339Nano), lastError, now.UTC().Format(time.RFC3339Nano), assetKey, jobID)
	if err != nil {
		return fmt.Errorf("workercache.MarkLeaseReleaseRetry(%q,%q): %w", assetKey, jobID, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.MarkLeaseReleaseRetry(%q,%q): rows affected: %w", assetKey, jobID, err)
	}
	if n != 1 {
		return fmt.Errorf("%w: lease reconciliation asset_key=%s job_id=%s", ErrNotFound, assetKey, jobID)
	}
	return nil
}

// DeleteLeaseRelease removes a reconciliation row after Release succeeds or
// after the cache confirms that the asset row is already absent. It is
// idempotent so duplicate workers/retries cannot turn successful cleanup into
// an error.
func (c *Cache) DeleteLeaseRelease(ctx context.Context, assetKey, jobID string) error {
	if c == nil || c.db == nil {
		return errors.New("workercache.DeleteLeaseRelease: cache is nil")
	}
	if assetKey == "" || jobID == "" {
		return errors.New("workercache.DeleteLeaseRelease: asset_key and job_id are required")
	}
	if _, err := c.db.ExecContext(ctx, `DELETE FROM pending_lease_releases WHERE asset_key = ? AND job_id = ?`, assetKey, jobID); err != nil {
		return fmt.Errorf("workercache.DeleteLeaseRelease(%q,%q): %w", assetKey, jobID, err)
	}
	return nil
}

// PendingLeaseReleaseCount is a small observability/test helper. It reports
// durable rows, including rows waiting for a future retry time.
func (c *Cache) PendingLeaseReleaseCount(ctx context.Context) (int, error) {
	if c == nil || c.db == nil {
		return 0, errors.New("workercache.PendingLeaseReleaseCount: cache is nil")
	}
	var count int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM pending_lease_releases`).Scan(&count); err != nil {
		return 0, fmt.Errorf("workercache.PendingLeaseReleaseCount: %w", err)
	}
	return count, nil
}
