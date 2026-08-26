package workercache

import (
	"context"
	"fmt"
	"time"
)

// ErrNotFound when no cached asset row matches.
func (c *Cache) Acquire(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Acquire: jobID is required")
	}
	res, err := c.db.ExecContext(ctx, `INSERT OR IGNORE INTO cached_asset_leases (asset_key, job_id, acquired_at) SELECT asset_key, ?, ? FROM cached_assets WHERE asset_key = ?`, jobID, time.Now().UTC().Format(time.RFC3339Nano), assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): lease insert: %w", assetKey, jobID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("workercache.Acquire(%q, %q): rows affected: %w", assetKey, jobID, err)
	} else if n == 0 {
		var found int
		if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE asset_key = ?`, assetKey).Scan(&found); err != nil {
			return fmt.Errorf("workercache.Acquire(%q, %q): probe: %w", assetKey, jobID, err)
		}
		if found == 0 {
			return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
		}
	}
	return nil
}

// RenewLease is a cache-protection heartbeat, not a TTL extension: the
// authoritative cached_asset_leases relation has no expiry column and keeps
// the asset protected while present. It bumps last_used_at only when the
// (asset, job) lease relation still exists. Unlike MarkUsed, this fenced update cannot refresh an
// unleased asset, so a lost lease is visible to the caller and the render's
// renewal loop can report it instead of silently claiming success.
// Returns ErrNotFound when the asset row is missing and ErrLeaseNotFound when
// the asset exists but this job no longer owns a lease for it.
func (c *Cache) RenewLease(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.RenewLease: jobID is required")
	}
	res, err := c.db.ExecContext(ctx, `
UPDATE cached_assets
   SET last_used_at = ?
 WHERE asset_key = ?
   AND EXISTS (
       SELECT 1 FROM cached_asset_leases
        WHERE asset_key = ? AND job_id = ?
   )`, time.Now().UTC().Format(time.RFC3339Nano), assetKey, assetKey, jobID)
	if err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): %w", assetKey, jobID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): rows affected: %w", assetKey, jobID, err)
	} else if n == 1 {
		return nil
	}
	var assetExists int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM cached_assets WHERE asset_key = ?`, assetKey).Scan(&assetExists); err != nil {
		return fmt.Errorf("workercache.RenewLease(%q, %q): probe: %w", assetKey, jobID, err)
	}
	if assetExists == 0 {
		return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
	}
	return fmt.Errorf("%w: asset_key=%s job_id=%s", ErrLeaseNotFound, assetKey, jobID)
}

// Release removes only the (asset, job) lease relation and bumps
// last_used_at. Releasing another job's lease is a benign no-op.
// Returns ErrNotFound when the asset row itself is missing.
func (c *Cache) Release(ctx context.Context, assetKey, jobID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if jobID == "" {
		return fmt.Errorf("workercache.Release: jobID is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := c.db.ExecContext(ctx, `DELETE FROM cached_asset_leases WHERE asset_key = ? AND job_id = ?`, assetKey, jobID); err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): lease delete: %w", assetKey, jobID, err)
	}
	res, err := c.db.ExecContext(ctx, `UPDATE cached_assets SET last_used_at = ? WHERE asset_key = ?`, now, assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Release(%q, %q): %w", assetKey, jobID, err)
	}
	return mustHaveAffected(res, assetKey, "Release")
}

// Reserve protects an asset for an imminent job until expiresAt. Reservations
// are durable and participate in the same cleanup protection barrier as leases.
func (c *Cache) Reserve(ctx context.Context, assetKey, reservationID string, expiresAt time.Time) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if reservationID == "" || expiresAt.IsZero() {
		return fmt.Errorf("workercache.Reserve: reservation ID and expiry are required")
	}
	res, err := c.db.ExecContext(ctx, `INSERT OR REPLACE INTO cached_asset_reservations (asset_key, reservation_id, expires_at) SELECT asset_key, ?, ? FROM cached_assets WHERE asset_key = ?`, reservationID, expiresAt.UTC().Format(time.RFC3339Nano), assetKey)
	if err != nil {
		return fmt.Errorf("workercache.Reserve(%q, %q): %w", assetKey, reservationID, err)
	}
	return mustHaveAffected(res, assetKey, "Reserve")
}

// ReleaseReservation removes one future-job reservation. It is idempotent
// when the cache row still exists.
func (c *Cache) ReleaseReservation(ctx context.Context, assetKey, reservationID string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	if _, err := c.db.ExecContext(ctx, `DELETE FROM cached_asset_reservations WHERE asset_key = ? AND reservation_id = ?`, assetKey, reservationID); err != nil {
		return fmt.Errorf("workercache.ReleaseReservation(%q, %q): %w", assetKey, reservationID, err)
	}
	return nil
}

// DeleteIfUnleased atomically removes an unleased and unreserved cache row.
// The predicates close the List→Delete race when another job acquires a lease
// or future-job reservation while cleanup is scanning. Cleanup should prefer
// EvictIfUnleased so physical removal and index deletion share a write fence.
func (c *Cache) DeleteIfUnleased(ctx context.Context, assetKey string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx, `DELETE FROM cached_assets WHERE asset_key = ? AND NOT EXISTS (SELECT 1 FROM cached_asset_leases WHERE asset_key = ?) AND NOT EXISTS (SELECT 1 FROM cached_asset_reservations WHERE asset_key = ? AND julianday(expires_at) > julianday('now'))`, assetKey, assetKey, assetKey)
	if err != nil {
		return fmt.Errorf("workercache.DeleteIfUnleased(%q): %w", assetKey, err)
	}
	if err := mustHaveAffected(res, assetKey, "DeleteIfUnleased"); err != nil {
		return err
	}
	c.deleteOrphanedBlobs(ctx)
	return nil
}

// deleteOrphanedBlobs removes blob rows no longer referenced by any asset.
// It is a best-effort hygiene pass: orphaned blobs are invisible to the
// asset-keyed read model, so a failed sweep is harmless and retried on a
// later delete/eviction.
func (c *Cache) deleteOrphanedBlobs(ctx context.Context) {
	_, _ = c.db.ExecContext(ctx,
		`DELETE FROM cached_blobs WHERE NOT EXISTS (SELECT 1 FROM cached_assets a WHERE a.content_hash = cached_blobs.content_hash)`)
}

// Delete removes the row. Returns ErrNotFound when no row matches.
// Cleanup uses EvictIfUnleased instead so physical removal and index deletion
// remain one fenced operation.
func (c *Cache) Delete(ctx context.Context, assetKey string) error {
	if assetKey == "" {
		return ErrEmptyID
	}
	res, err := c.db.ExecContext(ctx,
		`DELETE FROM cached_assets WHERE asset_key = ?`,
		assetKey,
	)
	if err != nil {
		return fmt.Errorf("workercache.Delete(%q): %w", assetKey, err)
	}
	if err := mustHaveAffected(res, assetKey, "Delete"); err != nil {
		return err
	}
	c.deleteOrphanedBlobs(ctx)
	return nil
}

// List returns all rows ordered by asset_key (deterministic for
// tests + supervisor scans).
func (c *Cache) List(ctx context.Context) ([]Entry, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT `+selectCols+selectFrom+` ORDER BY a.asset_key ASC`)
	if err != nil {
		return nil, fmt.Errorf("workercache.List: %w", err)
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// Size returns the current number of indexed entries and their recorded byte
// total. It is a read-only snapshot for low-cardinality cache gauges.
func (c *Cache) Size(ctx context.Context) (entries int, bytes int64, err error) {
	if c == nil || c.db == nil {
		return 0, 0, fmt.Errorf("workercache.Size: nil cache")
	}
	if err := c.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(1) FROM cached_assets), COALESCE((SELECT SUM(b.size_bytes) FROM cached_blobs b WHERE EXISTS (SELECT 1 FROM cached_assets a WHERE a.content_hash = b.content_hash)), 0)`).Scan(&entries, &bytes); err != nil {
		return 0, 0, fmt.Errorf("workercache.Size: %w", err)
	}
	return entries, bytes, nil
}

// ReadyKeys returns the canonical asset keys currently materialized on disk.
// It is a read-only, bounded heartbeat projection used by master placement;
// callers must not treat it as a lease or as proof that a file cannot be
// evicted before the task acquires its lease.
func (c *Cache) ReadyKeys(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT a.asset_key FROM cached_assets a JOIN cached_blobs b ON b.content_hash = a.content_hash WHERE b.download_complete = 1 ORDER BY a.asset_key`)
	if err != nil {
		return nil, fmt.Errorf("workercache.ReadyKeys: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("workercache.ReadyKeys scan: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
