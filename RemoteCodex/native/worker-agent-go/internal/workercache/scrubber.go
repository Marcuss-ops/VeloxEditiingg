// Package workercache — background integrity scrubber.
//
// The scrubber re-verifies a small, throttled fraction of the cache per pass
// so bit-rot and out-of-band corruption are found without slowing jobs. It
// re-computes SHA-256 over each candidate blob's physical file and compares
// it to the blob's recorded content_hash:
//
//   - match      → bump verified_at (the blob moves to the back of the queue,
//     so coverage is round-robin, oldest-verified first);
//   - mismatch   → invalidate: remove the physical file and the cached_blobs
//     row while RETAINING the asset_key → content_hash mappings,
//     so a later resolve of any referencing asset sees a MISS and
//     re-downloads (the same mapping-retention rule as pressure
//     eviction);
//   - missing    → same invalidation (a stale blob is not a cache hit);
//   - read error → skip (transient I/O must never destroy a good blob).
//
// Throttling: each pass re-reads at most ScrubConfig.BytesPerPass bytes and
// touches at most ScrubConfig.MaxBlobsPerPass blobs, ordered by verified_at
// ascending. A single blob is always processed even when it exceeds the byte
// budget (otherwise the largest — and most valuable to verify — files would
// starve forever). Blobs referenced by an active lease or unexpired
// reservation are excluded at selection time so the scrubber never competes
// for NVMe with a running render.
package workercache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"velox-shared/assetref"
)

// ErrBlobInFlight is returned by InvalidateCorruptBlob when a blob is still
// mid-download (download_complete = 0). The scrubber never selects such a
// blob, so this is a defensive fence against a concurrent promotion racing
// the invalidation.
var ErrBlobInFlight = errors.New("workercache: blob download is in flight")

// ScrubConfig throttles one scrub pass. BytesPerPass is the soft ceiling on
// total re-hashed bytes; MaxBlobsPerPass is the hard ceiling on the number of
// blobs touched. Both mirror WorkerConfig's CacheScrub* tuning (validated
// there); the scrubber still validates so it can be driven directly in tests.
type ScrubConfig struct {
	BytesPerPass    int64
	MaxBlobsPerPass int
}

// ScrubStats summarises one scrub pass. Scanned counts blobs whose bytes were
// re-read (success, mismatch, or missing-file); ScannedBytes is their total
// size. Corrupt/CorruptBytes count blobs that failed verification and were
// invalidated. DurationMS is the wall-clock cost of the pass.
type ScrubStats struct {
	Scanned      int
	ScannedBytes int64
	Corrupt      int
	CorruptBytes int64
	DurationMS   int64
}

// IntegrityScrubber re-verifies cached blobs against their recorded
// content_hash. HashFile is injectable for tests; production uses the
// default streaming SHA-256.
type IntegrityScrubber struct {
	HashFile func(path string) (assetref.ContentHash, error)
}

// NewIntegrityScrubber returns a scrubber with the default streaming SHA-256
// hasher.
func NewIntegrityScrubber() *IntegrityScrubber {
	return &IntegrityScrubber{HashFile: defaultScrubHashFile}
}

func (s *IntegrityScrubber) hashFile() func(string) (assetref.ContentHash, error) {
	if s != nil && s.HashFile != nil {
		return s.HashFile
	}
	return defaultScrubHashFile
}

// defaultScrubHashFile computes the lowercase hex SHA-256 of a file by
// streaming it (never loading the whole asset into memory).
func defaultScrubHashFile(path string) (assetref.ContentHash, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return assetref.ContentHash(hex.EncodeToString(h.Sum(nil))), nil
}

// NextScrubBlobs returns up to limit verifiable blobs ordered by verified_at
// ascending (oldest verification first). Legacy blobs (a synthetic
// legacy:<asset> key with no verified digest) are excluded because there is
// no SHA-256 to compare against, and blobs referenced by a leased or
// unexpired-reserved asset are excluded so the scrubber never competes for
// I/O with an active job.
func (c *Cache) NextScrubBlobs(ctx context.Context, limit int) ([]Blob, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("workercache.NextScrubBlobs: limit must be positive")
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT b.content_hash, b.local_path, b.size_bytes, b.download_complete,
       b.created_at, b.last_used_at, b.verified_at
  FROM cached_blobs b
 WHERE b.download_complete = 1
   AND b.content_hash NOT LIKE 'legacy:%'
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
 ORDER BY b.verified_at ASC
 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("workercache.NextScrubBlobs: %w", err)
	}
	defer rows.Close()
	var out []Blob
	for rows.Next() {
		var (
			b         Blob
			dlInt     int
			createdS  string
			usedS     string
			verifiedS string
		)
		if err := rows.Scan(&b.ContentHash, &b.LocalPath, &b.SizeBytes, &dlInt,
			&createdS, &usedS, &verifiedS); err != nil {
			return nil, fmt.Errorf("workercache.NextScrubBlobs: scan: %w", err)
		}
		b.DownloadComplete = dlInt != 0
		if b.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
			return nil, fmt.Errorf("workercache.NextScrubBlobs: created_at: %w", err)
		}
		if b.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
			return nil, fmt.Errorf("workercache.NextScrubBlobs: last_used_at: %w", err)
		}
		if b.VerifiedAt, err = parseRFC3339Nano(verifiedS); err != nil {
			return nil, fmt.Errorf("workercache.NextScrubBlobs: verified_at: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// MarkBlobVerified records a successful integrity re-verification by bumping
// verified_at. It deliberately does NOT touch last_used_at: the scrub pass is
// a health check, not an access, so it must not make a cold blob look hot to
// the LRU pressure controller. Returns ErrNotFound when the blob is gone.
func (c *Cache) MarkBlobVerified(ctx context.Context, contentHash assetref.ContentHash) error {
	if contentHash == "" {
		return ErrInvalidContentHash
	}
	res, err := c.db.ExecContext(ctx,
		`UPDATE cached_blobs SET verified_at = ? WHERE content_hash = ?`,
		time.Now().UTC().Format(time.RFC3339Nano), string(contentHash),
	)
	if err != nil {
		return fmt.Errorf("workercache.MarkBlobVerified(%q): %w", contentHash, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.MarkBlobVerified(%q): rows affected: %w", contentHash, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: content_hash=%s", ErrNotFound, contentHash)
	}
	return nil
}

// InvalidateCorruptBlob removes a corrupt blob's physical file and its
// cached_blobs row under one SQLite write fence. The referencing
// asset_key → content_hash mappings are intentionally RETAINED so a later
// resolve of any of them sees a MISS and re-downloads the bytes (the same
// mapping-retention rule as pressure eviction). It is NOT gated on the
// lease/reservation barrier: corrupt bytes must never be served, so a leased
// blob that failed verification is still removed — the running render already
// holds an open file descriptor (unlink is safe on Linux) and any later
// access re-downloads. Returns ErrNotFound when the blob is already gone and
// ErrBlobInFlight when it is still mid-download.
func (c *Cache) InvalidateCorruptBlob(ctx context.Context, contentHash assetref.ContentHash) error {
	if contentHash == "" {
		return ErrInvalidContentHash
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("workercache.InvalidateCorruptBlob(%q): connection: %w", contentHash, err)
	}
	defer conn.Close()
	rollback := func(cause error) error {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return cause
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("workercache.InvalidateCorruptBlob(%q): begin: %w", contentHash, err)
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
		return rollback(fmt.Errorf("workercache.InvalidateCorruptBlob(%q): probe: %w", contentHash, err))
	}
	if dlInt == 0 {
		return rollback(fmt.Errorf("%w: content_hash=%s", ErrBlobInFlight, contentHash))
	}

	if blobPath != "" {
		if err := c.fs.Remove(blobPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return rollback(fmt.Errorf("workercache.InvalidateCorruptBlob(%q): physical remove: %w", contentHash, err))
		}
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM cached_blobs WHERE content_hash = ?`, string(contentHash)); err != nil {
		return rollback(fmt.Errorf("workercache.InvalidateCorruptBlob(%q): delete blob: %w", contentHash, err))
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("workercache.InvalidateCorruptBlob(%q): commit: %w", contentHash, err)
	}
	return nil
}

// ScrubPass runs one throttled integrity-verification pass. It returns the
// scrub stats; the scrub loop's OnTick is the single observability boundary.
func (s *IntegrityScrubber) ScrubPass(ctx context.Context, c *Cache, cfg ScrubConfig) (ScrubStats, error) {
	started := time.Now()
	var stats ScrubStats
	defer func() { stats.DurationMS = time.Since(started).Milliseconds() }()

	if c == nil {
		return stats, fmt.Errorf("workercache.ScrubPass: nil cache")
	}
	if s.hashFile() == nil {
		return stats, fmt.Errorf("workercache.ScrubPass: nil hash function")
	}
	if cfg.BytesPerPass <= 0 {
		return stats, fmt.Errorf("workercache.ScrubPass: BytesPerPass must be positive")
	}
	if cfg.MaxBlobsPerPass <= 0 {
		return stats, fmt.Errorf("workercache.ScrubPass: MaxBlobsPerPass must be positive")
	}

	candidates, err := c.NextScrubBlobs(ctx, cfg.MaxBlobsPerPass)
	if err != nil {
		return stats, err
	}
	hash := s.hashFile()

	for _, blob := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		// Byte-budget throttle: always let the first blob through (a single
		// oversized file must still be verifiable), then stop before the
		// cumulative re-read exceeds BytesPerPass. The remaining candidates
		// are simply deferred to the next pass.
		if stats.Scanned > 0 && stats.ScannedBytes+blob.SizeBytes > cfg.BytesPerPass {
			break
		}

		actual, hashErr := hash(blob.LocalPath)
		if hashErr != nil {
			if errors.Is(hashErr, os.ErrNotExist) {
				// Missing file = stale blob: invalidate so a resolve misses
				// and re-downloads.
				if invErr := c.InvalidateCorruptBlob(ctx, blob.ContentHash); invErr != nil {
					if errors.Is(invErr, ErrNotFound) {
						continue // concurrent eviction already removed it
					}
					return stats, invErr
				}
				stats.Scanned++
				stats.ScannedBytes += blob.SizeBytes
				stats.Corrupt++
				stats.CorruptBytes += blob.SizeBytes
				continue
			}
			// Transient read error (EIO, EACCES, …): never destroy a blob we
			// could not actually read. Skip and retry next pass.
			continue
		}

		stats.Scanned++
		stats.ScannedBytes += blob.SizeBytes
		if actual == blob.ContentHash {
			if err := c.MarkBlobVerified(ctx, blob.ContentHash); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return stats, err
			}
			continue
		}

		// SHA mismatch: bit-rot or out-of-band corruption.
		if invErr := c.InvalidateCorruptBlob(ctx, blob.ContentHash); invErr != nil {
			if errors.Is(invErr, ErrNotFound) {
				continue
			}
			return stats, invErr
		}
		stats.Corrupt++
		stats.CorruptBytes += blob.SizeBytes
	}
	return stats, nil
}

// ScrubLoop is the background scheduler for the integrity scrubber. It mirrors
// CleanupLoop's shape (Run + TickOnce + OnTick) but needs no protection
// barrier: a scrub pass is read-only except for invalidating blobs whose
// bytes provably no longer match their recorded digest, which is safe
// regardless of the master's protected snapshot.
type ScrubLoop struct {
	// Cache is the workercache.Cache being scrubbed. Required.
	Cache *Cache

	// Config throttles each pass (byte budget + blob count). Required.
	Config ScrubConfig

	// Interval is the tick cadence. Required (> 0).
	Interval time.Duration

	// Scrubber performs the verification. Optional: nil defaults to the
	// streaming SHA-256 scrubber.
	Scrubber *IntegrityScrubber

	// OnTick is the observability callback invoked once per Tick with the
	// scrub stats + any error. Optional; production wires Prometheus / logs.
	OnTick func(ScrubStats, error)
}

// Run blocks until ctx is cancelled, firing an initial tick on entry and then
// a tick per Interval. Errors from a tick are surfaced via OnTick and do not
// halt the loop.
func (sl *ScrubLoop) Run(ctx context.Context) error {
	if sl.Cache == nil {
		return errors.New("workercache.ScrubLoop.Run: nil Cache")
	}
	if sl.Interval <= 0 {
		return errors.New("workercache.ScrubLoop.Run: Interval must be positive")
	}
	if sl.Scrubber == nil {
		sl.Scrubber = NewIntegrityScrubber()
	}

	sl.runTick(ctx)
	ticker := time.NewTicker(sl.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sl.runTick(ctx)
		}
	}
}

// runTick is the per-tick observability wrapper.
func (sl *ScrubLoop) runTick(ctx context.Context) {
	stats, err := sl.TickOnce(ctx)
	if sl.OnTick != nil {
		sl.OnTick(stats, err)
	}
}

// TickOnce runs a single scrub pass. Public so tests can drive it
// deterministically; production uses Run.
func (sl *ScrubLoop) TickOnce(ctx context.Context) (ScrubStats, error) {
	if sl.Cache == nil {
		return ScrubStats{}, errors.New("workercache.ScrubLoop.TickOnce: nil Cache")
	}
	if sl.Scrubber == nil {
		sl.Scrubber = NewIntegrityScrubber()
	}
	return sl.Scrubber.ScrubPass(ctx, sl.Cache, sl.Config)
}
