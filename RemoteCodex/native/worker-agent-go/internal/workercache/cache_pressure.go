// Package workercache — pressure-based LRU eviction (cache pressure
// controller).
//
// The legacy Cleanup pass removes every unleased / unreserved /
// unprotected / grace-expired asset on each tick regardless of actual disk
// need. That policy can evict still-warm blobs on a worker that has plenty
// of free NVMe. The pressure controller is the replacement policy:
//
//	disk < HIGH watermark  → evict NOTHING
//	disk >= HIGH watermark → evict LRU blobs until usage drops to LOW
//
// HIGH/LOW give hysteresis (e.g. 80 / 72) so the cleaner never thrashes on
// the 80.1 ↔ 79.9 boundary. Eviction is blob-scoped: the physical bytes
// belong to cached_blobs and are removed only when no remaining asset
// references the blob with a lease, an unexpired reservation, or a key in
// the master's protected snapshot. The asset_key → content_hash mappings are
// intentionally retained (cheap metadata) so a later resolve knows the bytes
// must be re-downloaded.
package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"velox-shared/assetref"
)

// ErrBlobProtected is returned when a blob is still referenced by at least one
// asset holding an active lease, an unexpired reservation, or a key present in
// the caller's protected snapshot. It is also returned by scrub invalidation
// when a job/reservation wins the selection race. It is a policy outcome, not
// a hard failure: the caller counts it or skips the blob.
var ErrBlobProtected = errors.New("workercache: blob is protected")

// BlobCandidate is a blob eligible for LRU pressure eviction at query time:
// complete, and not referenced by any leased or unreserved-expired asset.
// Snapshot protection is checked again under the eviction write fence.
type BlobCandidate struct {
	ContentHash assetref.ContentHash
	LocalPath   string
	SizeBytes   int64
	LastUsedAt  time.Time
}

// OldestEvictableBlobs returns up to limit complete blobs ordered by
// last_used_at ascending (LRU first). A blob is excluded when ANY asset
// referencing it holds an active lease or an unexpired reservation — the
// blob-level equivalent of the asset-level protection barrier. Snapshot
// protection is NOT part of this projection (it is a Go-side map), so
// callers must still pass the protected set to EvictBlobIfUnprotected,
// which re-checks it under the write fence.
func (c *Cache) OldestEvictableBlobs(ctx context.Context, limit int) ([]BlobCandidate, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("workercache.OldestEvictableBlobs: limit must be positive")
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT b.content_hash, b.local_path, b.size_bytes, b.last_used_at
  FROM cached_blobs b
 WHERE b.download_complete = 1
   AND NOT EXISTS (
       SELECT 1 FROM cached_assets a
        WHERE a.content_hash = b.content_hash
          AND (
              EXISTS (SELECT 1 FROM cached_asset_leases l WHERE l.asset_key = a.asset_key)
              OR EXISTS (SELECT 1 FROM cached_asset_reservations r
                          WHERE r.asset_key = a.asset_key
                            AND julianday(r.expires_at) > julianday('now'))
          )
   )
 ORDER BY b.last_used_at ASC
 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("workercache.OldestEvictableBlobs: %w", err)
	}
	defer rows.Close()
	var out []BlobCandidate
	for rows.Next() {
		var (
			cand   BlobCandidate
			hash   string
			usedAt string
		)
		if err := rows.Scan(&hash, &cand.LocalPath, &cand.SizeBytes, &usedAt); err != nil {
			return nil, fmt.Errorf("workercache.OldestEvictableBlobs: scan: %w", err)
		}
		cand.ContentHash = assetref.ContentHash(hash)
		if cand.LastUsedAt, err = parseRFC3339Nano(usedAt); err != nil {
			return nil, fmt.Errorf("workercache.OldestEvictableBlobs: last_used_at: %w", err)
		}
		out = append(out, cand)
	}
	return out, rows.Err()
}

// EvictBlobIfUnprotected removes a blob's physical file and cached_blobs row
// under one SQLite write fence, but only when the blob is complete and no
// remaining asset references it with a lease, an unexpired reservation, or a
// key present in protected. Referencing asset_key → content_hash mappings are
// retained so a future resolve of any of them sees a MISS and re-downloads
// the bytes. Returns ErrBlobProtected when protected, ErrNotFound when the
// blob is already gone.
func (c *Cache) EvictBlobIfUnprotected(ctx context.Context, contentHash assetref.ContentHash, protected map[string]struct{}) error {
	if contentHash == "" {
		return ErrInvalidContentHash
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): connection: %w", contentHash, err)
	}
	defer conn.Close()
	rollback := func(cause error) error {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return cause
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): begin: %w", contentHash, err)
	}

	var (
		blobPath string
		dlInt    int
	)
	err = conn.QueryRowContext(ctx,
		`SELECT local_path, download_complete FROM cached_blobs WHERE content_hash = ?`,
		string(contentHash)).Scan(&blobPath, &dlInt)
	if errors.Is(err, sql.ErrNoRows) {
		return rollback(fmt.Errorf("%w: content_hash=%s", ErrNotFound, contentHash))
	}
	if err != nil {
		return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): probe: %w", contentHash, err))
	}
	if dlInt == 0 {
		return rollback(fmt.Errorf("%w: download_in_flight content_hash=%s", ErrBlobProtected, contentHash))
	}

	// Re-check the protection barrier under the fence: any referencing asset
	// that is leased, reserved (unexpired), or snapshot-protected keeps the
	// physical bytes. This closes the List→Evict race where a lease lands
	// after the candidate query but before the removal.
	refRows, err := conn.QueryContext(ctx,
		`SELECT a.asset_key FROM cached_assets a WHERE a.content_hash = ?`,
		string(contentHash))
	if err != nil {
		return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): refs: %w", contentHash, err))
	}
	var refs []string
	for refRows.Next() {
		var key string
		if err := refRows.Scan(&key); err != nil {
			refRows.Close()
			return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): refs scan: %w", contentHash, err))
		}
		refs = append(refs, key)
	}
	refRows.Close()
	if err := refRows.Err(); err != nil {
		return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): refs err: %w", contentHash, err))
	}
	for _, key := range refs {
		if _, keep := protected[key]; keep {
			return rollback(fmt.Errorf("%w: snapshot asset_key=%s content_hash=%s", ErrBlobProtected, key, contentHash))
		}
	}
	var protectedRefs int
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(1) FROM cached_assets a
 WHERE a.content_hash = ?
   AND (
       EXISTS (SELECT 1 FROM cached_asset_leases l WHERE l.asset_key = a.asset_key)
       OR EXISTS (SELECT 1 FROM cached_asset_reservations r
                   WHERE r.asset_key = a.asset_key
                     AND julianday(r.expires_at) > julianday('now'))
   )`, string(contentHash)).Scan(&protectedRefs); err != nil {
		return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): protection recheck: %w", contentHash, err))
	}
	if protectedRefs > 0 {
		return rollback(fmt.Errorf("%w: leased_or_reserved content_hash=%s", ErrBlobProtected, contentHash))
	}

	if blobPath != "" {
		if err := c.fs.Remove(blobPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): physical remove: %w", contentHash, err))
		}
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM cached_blobs WHERE content_hash = ?`, string(contentHash)); err != nil {
		return rollback(fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): delete blob: %w", contentHash, err))
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("workercache.EvictBlobIfUnprotected(%q): commit: %w", contentHash, err)
	}
	return nil
}

// PressureEvictionConfig carries the hysteresis and batch tuning for the
// pressure controller. The values mirror WorkerConfig's
// CacheHighWatermarkPercent / CacheLowWatermarkPercent /
// CacheEvictionBatchSize (which are validated there); the controller itself
// still validates so it can be driven directly in tests.
type PressureEvictionConfig struct {
	HighWatermarkPercent int
	LowWatermarkPercent  int
	BatchSize            int
}

// PressureEvictionStats summarises one pressure pass.
type PressureEvictionStats struct {
	// UsagePercent is the disk-usage percentage observed at entry.
	UsagePercent int
	// Attempted is the number of candidate blobs the loop tried to evict.
	Attempted int
	// Removed is the number of blobs physically evicted.
	Removed int
	// RemovedBytes is the sum of SizeBytes over evicted blobs.
	RemovedBytes int64
	// Protected is the number of candidates skipped because a lease,
	// reservation, or snapshot reference landed before the fence.
	Protected  int
	DurationMS int64
}

// EvictUnderPressure runs one pressure-eviction pass. It returns without
// evicting anything when usage is below HighWatermarkPercent. Above the high
// watermark it removes LRU blobs in batches until usage drops to
// LowWatermarkPercent or no evictable blobs remain. usage is the injectable
// disk-usage probe (a closure so production can call statfs on the cache
// root and tests can drive a deterministic sequence).
func EvictUnderPressure(ctx context.Context, c *Cache, cfg PressureEvictionConfig, usage func() int, protected map[string]struct{}) (PressureEvictionStats, error) {
	started := time.Now()
	var stats PressureEvictionStats
	// EvictUnderPressure is PURE: it returns stats and leaves observability to
	// the caller (the CleanupLoop's OnTick is the single Prometheus boundary).
	// Recording here would double-count the same pass when the loop's OnTick
	// also publishes the eviction counters.
	defer func() { stats.DurationMS = time.Since(started).Milliseconds() }()

	if c == nil {
		return stats, fmt.Errorf("workercache.EvictUnderPressure: nil cache")
	}
	if usage == nil {
		return stats, fmt.Errorf("workercache.EvictUnderPressure: nil usage probe")
	}
	high := cfg.HighWatermarkPercent
	low := cfg.LowWatermarkPercent
	batch := cfg.BatchSize
	if high <= 0 || low <= 0 {
		return stats, fmt.Errorf("workercache.EvictUnderPressure: watermarks must be positive")
	}
	if low >= high {
		return stats, fmt.Errorf("workercache.EvictUnderPressure: low watermark must be strictly below high")
	}
	if batch <= 0 {
		return stats, fmt.Errorf("workercache.EvictUnderPressure: batch size must be positive")
	}

	stats.UsagePercent = usage()
	if stats.UsagePercent < high {
		return stats, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		candidates, err := c.OldestEvictableBlobs(ctx, batch)
		if err != nil {
			return stats, err
		}
		if len(candidates) == 0 {
			return stats, nil
		}
		progress := false
		for _, cand := range candidates {
			stats.Attempted++
			switch err := c.EvictBlobIfUnprotected(ctx, cand.ContentHash, protected); {
			case err == nil:
				stats.Removed++
				stats.RemovedBytes += cand.SizeBytes
				progress = true
			case errors.Is(err, ErrBlobProtected):
				stats.Protected++
			case errors.Is(err, ErrNotFound):
				// Concurrent eviction already removed it: not an error.
			default:
				return stats, err
			}
		}
		if !progress {
			// Every candidate was protected or already gone; another pass
			// would return the same set, so stop rather than spin.
			return stats, nil
		}
		stats.UsagePercent = usage()
		if stats.UsagePercent <= low {
			return stats, nil
		}
	}
}
