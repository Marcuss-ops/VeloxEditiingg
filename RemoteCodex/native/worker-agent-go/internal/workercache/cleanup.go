// Package workercache — Cleanup is the disk-reclaim pass that removes
// cache entries no longer needed for in-flight work, per the Pass 9
// design.
//
// Cleanup rule (canonical, mirrors the user's specification):
//
//  1. Skip rows with active_job_id != ” (currently leased by a job).
//     This is the EXPLICIT protection requested by Pass 9 — even if a
//     row is download_complete and is NOT in the master's protected
//     snapshot, an in-flight job's lease must keep it on disk.
//  2. Skip rows with download_complete = false (download-in-flight
//     or half-written). Recovery happens via the resolver, not here.
//  3. Skip rows whose drive_file_id is in the protected set supplied
//     by the master snapshot. The protected set is the "next N jobs'
//     Drive clip union" (see protectedasset.Service / Pass 5/6).
//  4. Otherwise: best-effort os.Remove(local_path), then Delete the
//     row from the index. A failure on one row does NOT halt the loop.
//
// Errors:
//   - ctx cancellation propagates via the workercache verb the loop
//     is calling under (List/Find/Delete return ctx.Err early).
//   - os.Remove errors are counted into RemoveErrors (and the row is
//     still delete-able from the index so subsequent passes do not
//     re-attempt the same physical file).
//
// The lease-vs-cleaner race is documented in the design doc: the
// Acquire/Release predicates in the worker are atomic at the SQL
// level, so a Cleanup that begins while a job's lease is still set
// is correct by construction. The reverse — a job acquires AFTER
// Cleanup has listed but BEFORE it deletes — is the 3-minute grace
// window protected by `last_used_at` in Pass 11 (not in this pass).
package workercache

import (
	"context"
	"errors"
	"fmt"
	"os"
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
}

// Cleanup removes entries that are NOT leased, NOT in flight, and NOT
// in the protected set.
//
// protected is the canonical drive_file_id union from the master's
// snapshot service (see protectedasset.Service). Pass nil for
// "delete everything not leased and not in flight" (e.g., during
// offline maintenance). The map key set is the API; values are
// ignored.
//
// Cleanup is safe to call concurrently with Acquire / Release. The
// per-row Acquire predicate `UPDATE cached_assets SET active_job_id = ?`
// is atomic; List-then-Delete is the documented race window the
// lease exists to protect. Cleanup NEVER inspects less than the
// ACTIVE state of each row, so the rule in (1) holds even when a
// concurrent Acquire fires between List and Delete of a different row.
func Cleanup(ctx context.Context, c *Cache, protected map[string]struct{}) (CleanupStats, error) {
	var stats CleanupStats

	if c == nil {
		return stats, fmt.Errorf("workercache.Cleanup: nil cache")
	}

	entries, err := c.List(ctx)
	if err != nil {
		return stats, fmt.Errorf("workercache.Cleanup: list: %w", err)
	}
	stats.Inspected = len(entries)

	for _, e := range entries {
		if e.ActiveJobID != "" {
			stats.SkippedLeased++
			continue
		}
		if !e.DownloadComplete {
			stats.SkippedInFlight++
			continue
		}
		if _, keep := protected[e.DriveFileID]; keep {
			stats.SkippedProtected++
			continue
		}

		// Order matters: remove the row first so a later pass does
		// not re-attempt os.Remove on a path that no longer has a
		// matching index row. Remove errors on the file do NOT
		// block the index delete — the row is the durable truth.
		if err := c.DeleteIfUnleased(ctx, e.DriveFileID); err != nil {
			stats.RemoveErrors++
			continue
		}
		if err := os.Remove(e.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			stats.RemoveErrors++
			continue
		}
		stats.Removed++
	}
	return stats, nil
}
