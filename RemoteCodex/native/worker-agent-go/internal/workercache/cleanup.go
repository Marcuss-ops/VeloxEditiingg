// Package workercache — Cleanup is the disk-reclaim pass that removes
// cache entries no longer needed for in-flight work, per the Pass 9
// design.
//
// Cleanup rule (canonical, mirrors the user's specification):
//
//  1. Skip rows with at least one relation in cached_asset_leases
//     (currently leased by one or more jobs).
//     This is the EXPLICIT protection requested by Pass 9 — even if a
//     row is download_complete and is NOT in the master's protected
//     snapshot, an in-flight job's lease must keep it on disk.
//  2. Skip rows with download_complete = false (download-in-flight
//     or half-written). Recovery happens via the resolver, not here.
//  3. Skip rows whose asset_key is in the protected set supplied
//     by the master snapshot. The protected set is the "next N jobs'
//     asset-key union" (see protectedasset.Service / Pass 5/6).
//  4. Otherwise: EvictIfUnleased removes local_path and the index row
//     under one SQLite write fence. A failure on one row does NOT halt the loop.
//
// Errors:
//   - ctx cancellation propagates via the workercache operations the loop
//     is calling under (List/Evict return ctx.Err early).
//   - os.Remove errors are counted into RemoveErrors and the index row is
//     retained so a later pass can retry the physical eviction.
//
// Physical removal and index deletion are fenced by EvictIfUnleased. A
// physical failure rolls back the SQL transaction and leaves the row available
// for retry; a lease or reservation cannot win between the two operations.
package workercache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// CleanupStats summarises the outcome of one Cleanup pass. All fields
// are observable counts so callers can log a single line + expose a
// Prometheus gauge family in a later pass.
//
// SkippedGrace (Pass 12) and SkippedSnapshotStale (Pass 12) are
// additive — older callers reading CleanupStats by name continue to
// work; new fields are zero-initialised and unused by Pass 9's
// Cleanup function (which never populates them).
type CleanupStats struct {
	Inspected                  int
	SkippedLeased              int
	SkippedInFlight            int
	SkippedProtected           int
	SkippedGrace               int
	SkippedSnapshotStale       int
	SkippedSnapshotUnavailable int
	Removed                    int
	RemoveErrors               int
	DurationMS                 int64
}

// Cleanup removes entries that are NOT leased, NOT in flight, and NOT
// in the protected set.
//
// protected is the canonical asset-key union from the master's
// snapshot service (see protectedasset.Service). Pass nil for
// "delete everything not leased and not in flight" (e.g., during
// offline maintenance). The map key set is the API; values are
// ignored.
//
// Cleanup is safe to call concurrently with Acquire / Release. The
// per-row Acquire predicate inserts into cached_asset_leases
// is atomic; List-then-Delete is the documented race window the
// lease exists to protect. Cleanup NEVER inspects less than the
// ACTIVE state of each row, so the rule in (1) holds even when a
// concurrent Acquire fires between List and Delete of a different row.
func Cleanup(ctx context.Context, c *Cache, protected map[string]struct{}) (CleanupStats, error) {
	return CleanupWithAudit(ctx, c, protected, nil, nil)
}

// CleanupWithAudit runs the legacy cleanup predicate while emitting one
// structured decision for every listed cache entry. The legacy Cleanup API
// remains unchanged; callers that need semantic metadata should use this
// helper or CleanupPolicy.AssetMetadata with CleanupWithPolicy.
func CleanupWithAudit(ctx context.Context, c *Cache, protected map[string]struct{}, audit CleanerAuditLogger, metadata map[string]CleanerAssetMetadata) (CleanupStats, error) {
	var stats CleanupStats

	if c == nil {
		return stats, fmt.Errorf("workercache.Cleanup: nil cache")
	}

	entries, err := c.List(ctx)
	if err != nil {
		return stats, fmt.Errorf("workercache.Cleanup: list: %w", err)
	}
	stats.Inspected = len(entries)
	now := time.Now().UTC()

	for _, e := range entries {
		if e.ActiveLeaseCount > 0 {
			stats.SkippedLeased++
			emitCleanerAudit(audit, e, metadata, "kept", "active_lease", now)
			continue
		}
		if e.ActiveReservationCount > 0 {
			stats.SkippedProtected++
			emitCleanerAudit(audit, e, metadata, "kept", "active_reservation", now)
			continue
		}
		if !e.DownloadComplete {
			stats.SkippedInFlight++
			emitCleanerAudit(audit, e, metadata, "kept", "download_in_flight", now)
			continue
		}
		if _, keep := protected[string(e.AssetKey)]; keep {
			stats.SkippedProtected++
			emitCleanerAudit(audit, e, metadata, "kept", "protected_snapshot", now)
			continue
		}

		if err := c.EvictIfUnleased(ctx, string(e.AssetKey), e.LocalPath); err != nil {
			if errors.Is(err, ErrNotFound) {
				stats.SkippedLeased++
				emitCleanerAudit(audit, e, metadata, "kept", "lease_acquired_during_cleanup", now)
			} else {
				stats.RemoveErrors++
				emitCleanerAudit(audit, e, metadata, "kept", "physical_remove_error", now)
			}
			continue
		}
		stats.Removed++
		emitCleanerAudit(audit, e, metadata, "removed", "not_protected_and_grace_expired", now)
	}
	return stats, nil
}
