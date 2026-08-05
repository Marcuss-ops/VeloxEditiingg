// Package workercache — Pass 12 CleanupPolicy adds two operational
// levers on top of the Pass 9 Cleanup rules:
//
//  1. Snapshot-staleness skip: if the master snapshot the worker
//     polled is older than SnapshotMaxAge, the entire cleanup pass
//     becomes a no-op and surfaces ErrSnapshotStale. Better to grow
//     the local cache temporarily than to wipe active in-flight jobs
//     under the false assumption "nothing is protected".
//
//  2. Recent-use grace: even when a row is un-leased, complete, and
//     missing from the current protected set, Cleanup keeps it if
//     last_used_at is within RecentUseGrace. This dampens the
//     race-window when a snapshot's protected set has shifted after
//     a new job arrived (T0 snapshot, T+5s new job, T+10s cleaner
//     scans). Without the grace, the old snapshot's protected set
//     would let the cleaner wipe the just-used asset.
//
// Pass 12 also introduces the VELOX_CACHE_* env vars the operator
// can set to override defaults without a recompile:
//
//	VELOX_CACHE_CLEANUP_INTERVAL  (default 5m) — cleanup loop ticker
//	VELOX_CACHE_RECENT_USE_GRACE  (default 3m) — grace period on last_used_at
//	VELOX_CACHE_SNAPSHOT_MAX_AGE  (default 2m) — staleness skip threshold
//
// Master-side tunables (VELOX_CACHE_LOOKAHEAD_JOBS,
// VELOX_CACHE_SNAPSHOT_INTERVAL) live in protectedasset/config.go to
// avoid a worker→master import.
//
// Pass 12 does NOT introduce an autonomous cleanup LOOP — that is
// the worker's job to wire (a ticker that calls CleanupWithPolicy).
// Pass 12 ships the predicate; Pass 12.5 / the daemon wires the loop.

package workercache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// ErrSnapshotStale is returned by CleanupWithPolicy when the supplied
// snapshot is older than policy.SnapshotMaxAge. The error is wrapped
// so callers can detect it via errors.Is. The accompanying stats
// reflect the no-op pass (Inspected + SkippedSnapshotStale).
var ErrSnapshotStale = errors.New("workercache: snapshot too old, skipping cleanup")

// ErrSnapshotUnavailable prevents startup cleanup from interpreting a
// missing master snapshot as an empty protected set.
var ErrSnapshotUnavailable = errors.New("workercache: no valid protection snapshot, skipping cleanup")

// CleanupPolicy carries the three tunables governing CleanupWithPolicy.
// Constructed via LoadCleanupPolicy (which reads VELOX_CACHE_*) and
// used by the daemon's ticker to schedule + execute cleanup passes.
type CleanupPolicy struct {
	// AuditLogger receives one structured decision for every cache entry
	// inspected by CleanupWithPolicy. Nil uses the package's structured
	// standard logger.
	AuditLogger CleanerAuditLogger

	// AssetMetadata supplies optional semantic role and future-reference
	// information for audit events. Missing metadata is represented as
	// role=unknown and future_reference_count=0.
	AssetMetadata map[string]CleanerAssetMetadata

	// CleanupInterval is the cadence at which the daemon runs the
	// cleanup loop. Not consulted by CleanupWithPolicy itself; carried
	// here so a single LoadCleanupPolicy(...) call returns all the
	// operator-facing tunables.
	CleanupInterval time.Duration

	// RecentUseGrace is the window during which last_used_at keeps a
	// row alive even when no lease is held AND no protected set covers
	// it. Set to 0 to disable the grace rule entirely (the test
	// matrix exercises both modes).
	RecentUseGrace time.Duration

	// SnapshotMaxAge is the staleness threshold. A snapshot whose
	// GeneratedAt is older than now-SnapshotMaxAge causes the entire
	// cleanup pass to short-circuit with ErrSnapshotStale.
	SnapshotMaxAge time.Duration
}

const (
	defaultCleanupInterval = 5 * time.Minute
	defaultRecentUseGrace  = 3 * time.Minute
	defaultSnapshotMaxAge  = 2 * time.Minute
)

// LoadCleanupPolicy reads VELOX_CACHE_CLEANUP_INTERVAL,
// VELOX_CACHE_RECENT_USE_GRACE, and VELOX_CACHE_SNAPSHOT_MAX_AGE
// from the process env, falling back to the defaults above when a
// var is unset or malformed. Malformed values are SILENTLY ignored
// (defaults are used) on purpose: a typo in the operator's env must
// not prevent the worker from booting — the daemon logs the err on
// first load (Pass 12.5 wiring); for now LoadCleanupPolicy never
// returns an error and is safe for direct use in tests.
func LoadCleanupPolicy() CleanupPolicy {
	p := CleanupPolicy{
		CleanupInterval: defaultCleanupInterval,
		RecentUseGrace:  defaultRecentUseGrace,
		SnapshotMaxAge:  defaultSnapshotMaxAge,
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CACHE_CLEANUP_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			p.CleanupInterval = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CACHE_RECENT_USE_GRACE")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			p.RecentUseGrace = d
		}
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CACHE_SNAPSHOT_MAX_AGE")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			p.SnapshotMaxAge = d
		}
	}
	return p
}

// CleanupWithPolicy is the Pass 12 cleanup pass. It composes:
//   - Pass 9 predicate order: leased → in-flight → protected → grace →
//     otherwise delete.
//   - Pass 12 staleness check: short-circuit when the supplied
//     snapshot is older than SnapshotMaxAge.
//
// `now` is injected so tests can stamp a deterministic clock; in
// production the caller passes time.Now().UTC(). `snapshotGeneratedAt`
// is zero iff no snapshot was ever polled; in that case the grace
// rule alone protects rows (treated as "no protected set").
//
// On ErrSnapshotStale the function returns the rows-listed
// Inspection count + SkippedSnapshotStale=len(entries) so callers can
// emit a `cleanup_skipped_total{reason="snapshot_stale"}` Prometheus
// metric. Stats for the failure case preserve the Inspected count.
func CleanupWithPolicy(
	ctx context.Context,
	c *Cache,
	snapshotGeneratedAt time.Time,
	protectedIDs []string,
	policy CleanupPolicy,
	now time.Time,
) (stats CleanupStats, err error) {
	started := time.Now()
	defer func() { stats.DurationMS = time.Since(started).Milliseconds() }()

	if c == nil {
		return stats, fmt.Errorf("workercache.CleanupWithPolicy: nil cache")
	}
	if now.IsZero() {
		return stats, fmt.Errorf("workercache.CleanupWithPolicy: now is zero (callers must inject time.Now().UTC())")
	}

	// Missing protection data is fail-safe: do not delete anything until the
	// worker has received at least one valid snapshot from the master.
	if snapshotGeneratedAt.IsZero() {
		entries, listErr := c.List(ctx)
		if listErr != nil {
			return stats, fmt.Errorf("workercache.CleanupWithPolicy: list (snapshot-unavailable): %w", listErr)
		}
		stats.Inspected = len(entries)
		stats.SkippedSnapshotUnavailable = len(entries)
		for _, e := range entries {
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "snapshot_unavailable", now)
		}
		return stats, fmt.Errorf("%w: rows_inspected=%d", ErrSnapshotUnavailable, len(entries))
	}

	// Staleness short-circuit: only triggers when BOTH
	// snapshotGeneratedAt is non-zero AND the age exceeds the policy.
	// A zero value (worker never polled) is treated as "no snapshot
	// available" — the cleanup pass still runs under the lease +
	// in-flight + grace rules.
	if !snapshotGeneratedAt.IsZero() && policy.SnapshotMaxAge > 0 &&
		now.Sub(snapshotGeneratedAt) > policy.SnapshotMaxAge {
		entries, listErr := c.List(ctx)
		if listErr != nil {
			return stats, fmt.Errorf("workercache.CleanupWithPolicy: list (stale-noop): %w", listErr)
		}
		stats.Inspected = len(entries)
		stats.SkippedSnapshotStale = len(entries)
		for _, e := range entries {
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "snapshot_stale", now)
		}
		return stats, fmt.Errorf("%w: snapshot_age=%v max_age=%v rows_inspected=%d",
			ErrSnapshotStale,
			now.Sub(snapshotGeneratedAt), policy.SnapshotMaxAge, len(entries))
	}

	protected := make(map[string]struct{}, len(protectedIDs))
	for _, id := range protectedIDs {
		protected[id] = struct{}{}
	}

	entries, err := c.List(ctx)
	if err != nil {
		return stats, fmt.Errorf("workercache.CleanupWithPolicy: list: %w", err)
	}
	stats.Inspected = len(entries)

	for _, e := range entries {
		if e.ActiveLeaseCount > 0 {
			stats.SkippedLeased++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "active_lease", now)
			continue
		}
		if !e.DownloadComplete {
			stats.SkippedInFlight++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "download_in_flight", now)
			continue
		}
		if _, keep := protected[e.DriveFileID]; keep {
			stats.SkippedProtected++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "protected_snapshot", now)
			continue
		}
		// Pass 12 grace rule — the new layer.
		if policy.RecentUseGrace > 0 && now.Sub(e.LastUsedAt) < policy.RecentUseGrace {
			stats.SkippedGrace++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "recent_use_grace", now)
			continue
		}

		if err := c.DeleteIfUnleased(ctx, e.DriveFileID); err != nil {
			// A concurrent Acquire can legitimately win between List and
			// cleanup. Treat that as a protected row, not as a cleanup
			// failure.
			if errors.Is(err, ErrNotFound) {
				stats.SkippedLeased++
				emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "lease_acquired_during_cleanup", now)
				continue
			}
			stats.RemoveErrors++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "kept", "index_delete_failed", now)
			continue
		}
		if err := os.Remove(e.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			stats.RemoveErrors++
			emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "index_removed", "physical_remove_error", now)
			continue
		}
		stats.Removed++
		emitCleanerAudit(policy.AuditLogger, e, policy.AssetMetadata, "removed", "not_protected_and_grace_expired", now)
	}
	return stats, nil
}
