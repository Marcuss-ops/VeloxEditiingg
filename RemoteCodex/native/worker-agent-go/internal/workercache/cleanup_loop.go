// Package workercache — CleanupLoop runs the periodic + post-job
// cache-pressure pass. The user-spec requirement is:
//
//   - "gira dopo ogni job" → after every job the worker dispatches,
//     a Tick fires.
//   - a periodic ticker (interval from VELOX_CACHE_EVICTION_INTERVAL_SECS,
//     default 30s) so pressure is re-evaluated without a job boundary.
//
// On every Tick, the loop:
//   1. Calls SnapshotSource.Current to fetch the latest protected
//      asset snapshot (the worker polled it during Pass 8 wiring).
//   2. Short-circuits (no eviction) when the snapshot is missing or
//      stale — a protected set we cannot trust must never authorize a wipe.
//   3. Delegates to EvictUnderPressure with the HIGH/LOW watermarks,
//      the LRU batch size, and the injectable disk-usage probe:
//      below HIGH nothing is evicted; at/above HIGH LRU blobs are removed
//      until usage drops to LOW.
//   4. Surfaces the result via OnTick callback for observability
//      (Prometheus counters + log lines in production).
//
// The SnapshotSource interface is local to workercache so the loop
// does not import velox-shared/protectedasset directly. Tests use a
// trivial in-memory fake; production wires the authenticated master polling
// client at the worker composition root.

package workercache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// SnapshotSource provides the most recent master-snapshot view: when
// it was generated AND the protected drive IDs at that moment.
// Returning a zero `generatedAt` is the explicit "no snapshot
// available yet" signal — CleanupWithPolicy treats this as the
// no-snapshot scenario (lease/in-flight/grace rules apply, no
// staleness short-circuit).
//
// The error path is a "soft failure": the loop logs + continues with
// the most-recently-good input absent; the very next Tick repeats
// the call. Never panic, never abort.
type SnapshotSource interface {
	Current(ctx context.Context) (generatedAt time.Time, protectedIDs []string, err error)
}

// ProtectionBarrier gates destructive cleanup until the worker has received
// at least one valid protected-assets snapshot. Implementations must remain
// fail-safe: transient poll failures do not open the barrier and cancellation
// must interrupt WaitReady promptly.
// ErrProtectionBarrierNotReady is returned to cleanup observability when
// the current registration session has not yet received a valid snapshot.
var ErrProtectionBarrierNotReady = errors.New("workercache: protection barrier is not ready")

type ProtectionBarrier interface {
	WaitReady(context.Context) error
	IsReady() bool
}

// FixedSnapshotSource is a deterministic SnapshotSource suitable for
// tests + small daemon wirings. Production wires a polling client
// in Pass 8. Set `protectedIDs` to nil for "no snapshot ever polled"
// (zero generatedAt).
type FixedSnapshotSource struct {
	GeneratedAt  time.Time
	ProtectedIDs []string
}

// Current implements SnapshotSource.
func (f *FixedSnapshotSource) Current(_ context.Context) (time.Time, []string, error) {
	return f.GeneratedAt, f.ProtectedIDs, nil
}

// CleanupLoop is the radable cleanup scheduler the daemon starts at
// boot. The constructor wires cache + policy + snapshot source;
// Run + TickOnce are the operational entry points.
type CleanupLoop struct {
	// Cache is the workercache.Cache being kept tidy. Required.
	Cache *Cache

	// Policy is the operator-facing CleanupPolicy (Pass 12). Its
	// SnapshotMaxAge is the staleness threshold for the protected-set
	// snapshot. IdleTTL is applied on every tick in addition to pressure
	// eviction, so an idle worker eventually releases old unneeded assets.
	// Required.
	Policy CleanupPolicy

	// Pressure carries the pressure-eviction hysteresis + batch tuning
	// (HIGH/LOW watermarks and the LRU batch size). The loop is the
	// pressure controller: each tick evicts nothing below the HIGH
	// watermark and evicts LRU blobs down to the LOW watermark above it.
	// Required for eviction; a zero config is a no-op pass.
	Pressure PressureEvictionConfig

	// UsagePercent is the injectable disk-usage probe. Production passes a
	// statfs closure over the blob cache root; tests pass a deterministic
	// sequence. Nil means "no pressure data" — the tick is a fail-safe no-op.
	UsagePercent func() int

	// Snapshot is the master's protected-asset snapshot source.
	// Optional for direct TickOnce callers; production supplies it.
	Snapshot SnapshotSource

	// Barrier is required for Run startup. Run refuses a nil barrier so a
	// production wiring mistake cannot bypass the first-snapshot safety gate.
	// TickOnce remains directly driveable for deterministic unit tests.
	Barrier ProtectionBarrier

	// Interval is the periodic ticker cadence. Required (> 0).
	Interval time.Duration

	// JobDone is the channel that fires once per completed job.
	// Optional: a nil channel means "no per-job trigger; only the
	// periodic ticker fires".
	JobDone <-chan struct{}

	// OnTick is the observability callback invoked once per Tick
	// with the pressure-eviction stats + any error. Optional;
	// production wires Prometheus counters / log lines here.
	OnTick func(PressureEvictionStats, error)

	// Now is the clock injection point. Optional; when nil, the
	// loop falls back to time.Now().UTC(). Tests inject a fixed
	// time so wall-clock drift cannot break the grace-period
	// predicate (which compares row.LastUsedAt to a 3-minute
	// reference instant — a test fixture seeded at 2026-07-27T12:00
	// against a real-now=2026-07-27T18:00 wall clock would always
	// be "expired").
	Now func() time.Time
}

// resolveNow returns cl.Now() if non-nil, otherwise the canonical
// UTC wall clock. Centralised so production and tests route through
// the same predicate semantics.
func (cl *CleanupLoop) resolveNow() time.Time {
	if cl.Now != nil {
		return cl.Now()
	}
	return time.Now().UTC()
}

// Run blocks until ctx is cancelled. The first Tick fires
// immediately on entry so the loop is not empty for the first
// `Interval` duration; subsequent Ticks fire on (a) the periodic
// ticker, (b) any JobDone signal, OR (c) ctx cancellation.
//
// Errors from Tick are surfaced via OnTick and recorded in the
// loop; they do NOT halt the loop. ErrSnapshotStale is the
// canonical non-fatal error: ErrSnapshotStale is expected when the
// master snapshot loop is stalled or the worker has not yet polled.
func (cl *CleanupLoop) Run(ctx context.Context) error {
	if cl.Cache == nil {
		return errors.New("workercache.CleanupLoop.Run: nil Cache")
	}
	if cl.Interval <= 0 {
		return errors.New("workercache.CleanupLoop.Run: Interval must be positive")
	}
	if cl.Barrier == nil {
		return errors.New("workercache.CleanupLoop.Run: nil ProtectionBarrier")
	}

	// Startup barrier: never perform a destructive cleanup pass against an
	// unknown protected set. Context cancellation interrupts the wait.
	if err := cl.Barrier.WaitReady(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Initial tick so the loop isn't empty for the first Interval.
	cl.runTick(ctx)

	ticker := time.NewTicker(cl.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			cl.runTick(ctx)
		case <-cl.JobDoneNonBlocking():
			cl.runTick(ctx)
		}
	}
}

// JobDoneNonBlocking is the channel-source for the run-loop select.
// When cl.JobDone is nil, a nil channel is used so `select`
// correctly skips that case (Go nil-channel semantics).
func (cl *CleanupLoop) JobDoneNonBlocking() <-chan struct{} {
	if cl.JobDone == nil {
		return nil
	}
	return cl.JobDone
}

// runTick is the per-tick observability wrapper. TickOnce does the
// real work; this method just dispatches the result.
func (cl *CleanupLoop) runTick(ctx context.Context) {
	// Re-check the barrier for every tick, not only at startup. A worker
	// session can disconnect after the first valid snapshot; in that case
	// the poller re-arms its barrier and cleanup must wait for the next
	// authenticated snapshot before touching the cache again.
	if err := cl.Barrier.WaitReady(ctx); err != nil {
		if cl.OnTick != nil {
			cl.OnTick(PressureEvictionStats{}, fmt.Errorf("%w: %v", ErrProtectionBarrierNotReady, err))
		}
		return
	}
	stats, err := cl.TickOnce(ctx)
	if cl.OnTick != nil {
		cl.OnTick(stats, err)
	}
}

// TickOnce runs one pressure-eviction pass against the current snapshot.
// Public so tests can drive deterministically without the loop
// overhead; production callers do NOT use this directly (Run is
// the canonical entry point).
//
// TickOnce NEVER blocks; the snapshot fetch is synchronous but
// bounded by ctx. A failed poll, a missing snapshot, or a stale
// snapshot are all fail-safe: the cache is left untouched (no eviction)
// and the corresponding sentinel error is returned for observability.
// With a fresh snapshot the pass delegates to EvictUnderPressure, which
// evicts nothing below the HIGH watermark.
func (cl *CleanupLoop) TickOnce(ctx context.Context) (PressureEvictionStats, error) {
	if cl.Cache == nil {
		return PressureEvictionStats{}, errors.New("workercache.CleanupLoop.TickOnce: nil Cache")
	}

	var generatedAt time.Time
	var protected []string
	if cl.Snapshot != nil {
		snapAt, ids, snapErr := cl.Snapshot.Current(ctx)
		if snapErr != nil {
			// A failed poll must never be converted into an empty
			// protected set. Keep the cache untouched until a valid
			// snapshot is available.
			return PressureEvictionStats{}, fmt.Errorf("snapshot fetch: %w", snapErr)
		}
		generatedAt = snapAt
		protected = ids
	}

	// Missing protection data is fail-safe: do not evict anything until the
	// worker has received at least one valid snapshot from the master.
	if generatedAt.IsZero() {
		return PressureEvictionStats{}, ErrSnapshotUnavailable
	}

	// Staleness short-circuit: the supplied snapshot is too old to authorize
	// eviction, so the pass becomes a no-op.
	if cl.Policy.SnapshotMaxAge > 0 && cl.resolveNow().Sub(generatedAt) > cl.Policy.SnapshotMaxAge {
		return PressureEvictionStats{}, fmt.Errorf("%w: snapshot_age=%v max_age=%v",
			ErrSnapshotStale, cl.resolveNow().Sub(generatedAt), cl.Policy.SnapshotMaxAge)
	}

	if cl.UsagePercent == nil {
		// Without a disk-usage probe there is no pressure signal; a
		// fail-safe no-op beats evicting on a guess.
		return PressureEvictionStats{}, errors.New("workercache.CleanupLoop.TickOnce: nil UsagePercent probe")
	}

	protectedSet := make(map[string]struct{}, len(protected))
	for _, id := range protected {
		protectedSet[id] = struct{}{}
	}

	pressureStats, pressureErr := EvictUnderPressure(ctx, cl.Cache, cl.Pressure, cl.UsagePercent, protectedSet)
	if pressureErr != nil {
		return pressureStats, pressureErr
	}
	_, idleErr := CleanupWithPolicy(ctx, cl.Cache, generatedAt, protected, cl.Policy, cl.resolveNow())
	if idleErr != nil {
		return pressureStats, fmt.Errorf("idle cache cleanup: %w", idleErr)
	}
	return pressureStats, nil
}
