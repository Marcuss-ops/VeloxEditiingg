// Package worker — protected-asset snapshot poller (Pass 7).
//
// The worker periodically fetches GET
// /api/v1/workers/cache/protected-assets (delivered in Pass 6
// of the master plan) and keeps the most recent SUCCESSFUL
// snapshot in memory for the cleanup loop (workercache). The
// snapshot is consumed by the workercache.CleanupLoop via its
// SnapshotSource interface; this poller is the concrete
// SnapshotSource implementation, surfaced as ProtectedAssetsPoller.
//
// DESIGN INVARIANTS:
//
//  1. Tick failure NEVER overwrites the in-memory snapshot.
//     HTTP 4xx/5xx, transport timeout, JSON decode error, and
//     circuit-breaker rejection all leave p.snap untouched. The
//     cleanup loop's SnapshotMaxAge rule is the ONLY mechanism
//     that ever considers a snapshot "stale enough to ignore";
//     the poller itself never self-evicts. This is what the
//     user-spec requires: "Se lo snapshot fallisce, NON
//     svuotare la cache locale."
//
//  2. A successful poll with empty DriveFileIDs is NOT a
//     failure — it means the master has zero jobs in queue. The
//     cleanup loop's grace rule (Pass 12 RecentUseGrace)
//     protects recently-used rows so the local cache is not
//     mass-evicted when the queue is briefly empty.
//
//  3. Snapshots are grow-only by reference: p.applySnapshot
//     replaces p.snap atomically. Old references held by readers
//     are not mutated in place — readers see a stable snapshot
//     for the duration of their read.
//
//  4. The poller does NOT log internally. Telemetry is the
//     worker's responsibility (Prometheus counters, structured
//     events). OnError / OnSuccess are thread-safe injection
//     points; production wires callbacks that increment
//     counters and emit log lines.
//
// CONCURRENCY model (mirrors internal/worker/heartbeat_loop.go):
//
//   Run := initial tick
//        + select { ctx.Done | ticker.C }
//
// The initial tick keeps the worker from spending the first
// Interval idle when the master is reachable at boot.

package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"

	"velox-worker-agent/pkg/api"
)

// ErrPollerInvalidInterval is the canonical error returned when
// NewProtectedAssetsPoller receives Interval <= 0. The constructor
// logs-and-defaults rather than panicking, but the helper that
// surfaces the error during bootstrap is exported for callers that
// prefer hard-fail semantics.
var ErrPollerInvalidInterval = errors.New("worker.ProtectedAssetsPoller: interval must be > 0; defaulting to 30s")
var ErrProtectedSnapshotNil = errors.New("worker.ProtectedAssetsPoller: successful response contained no snapshot")
var ErrProtectedSnapshotInvalid = errors.New("worker.ProtectedAssetsPoller: snapshot generated_at is invalid")
var ErrProtectedSnapshotStale = errors.New("worker.ProtectedAssetsPoller: snapshot generated_at is older than the last valid snapshot")

var _ workercache.ProtectionBarrier = (*ProtectedAssetsPoller)(nil)

// defaultPollInterval is the user-spec cadence when Interval is
// not supplied at construction time. Mirrors the master default
// (also 30s, from VELOX_CACHE_SNAPSHOT_INTERVAL). Same value
// on both sides means a 30s cadence stays in lockstep without
// resync arithmetic.
const defaultPollInterval = 30 * time.Second
const defaultSnapshotMaxAge = 2 * time.Minute

// ProtectedAssetsPoller is the worker-side polling surface for
// the master's protected-asset snapshot. The struct is zero-cost
// when Snapshot() is never called: the goroutine in Run is the
// only side-effecting piece of state.
type ProtectedAssetsPoller struct {
	// Client is the HTTP client surface. Required. Exposed as
	// the api.ProtectedAssetsAPI interface so tests swap in a
	// real httptest-backed *api.Client without a separate mock
	// type.
	Client api.ProtectedAssetsAPI

	// Interval is the polling cadence. Required to be > 0
	// before Run is invoked; NewProtectedAssetsPoller applies
	// the default if zero. Tests drive TickOnce directly with
	// any interval they want (huge values keep the ticker out
	// of the way).
	Interval time.Duration

	// SnapshotMaxAge rejects an already-stale first response before it can
	// open the startup barrier. It mirrors the cleanup policy threshold.
	SnapshotMaxAge time.Duration

	// OnError is the observability callback fired when a tick
	// fails. Optional. BOTH TickOnce and Run fire OnError
	// exactly once per failed tick, via the shared
	// runTickOnce helper:
	//   - TickOnce passes the underlying client error
	//     verbatim (network / 5xx / 4xx / JSON decode).
	//   - Run's automatic ticks wrap with a label prefix
	//     ("worker.ProtectedAssetsPoller: initial tick" /
	//     "worker.ProtectedAssetsPoller: tick") so on-call
	//     operators can grep boot-time vs periodic failures.
	// Run never double-wraps — TestPoller_500_KeepsLastGood
	// asserts the 1-on-1 mapping in the TickOnce path,
	// guaranteeing no double-fire on the Run path either.
	OnError func(err error)

	// OnSuccess is the observability callback fired AFTER
	// p.snap is updated. Optional. The snapshot argument is
	// the same pointer Snapshot() will return to subsequent
	// readers.
	OnSuccess func(snap *api.ProtectedAssetSnapshot)

	mu   sync.RWMutex
	snap *api.ProtectedAssetSnapshot

	// readyCh closes exactly once after the first valid snapshot. It is
	// deliberately independent from snap so a later 401/503 preserves
	// readiness and the last-good snapshot while cleanup applies its
	// own staleness policy.
	readyCh   chan struct{}
	readyOnce sync.Once
}

// NewProtectedAssetsPoller constructs the poller. Interval <= 0
// falls back to defaultPollInterval (30s, the user-spec default).
// Returns a pointer to a Poller with all mutexes zero-init'd.
func NewProtectedAssetsPoller(c api.ProtectedAssetsAPI, interval time.Duration) *ProtectedAssetsPoller {
	if interval <= 0 {
		interval = defaultPollInterval
	}
	return &ProtectedAssetsPoller{
		Client:         c,
		Interval:       interval,
		SnapshotMaxAge: defaultSnapshotMaxAge,
		readyCh:        make(chan struct{}),
	}
}

// Run blocks until ctx is cancelled. The first tick fires
// immediately on entry (so the snapshot is fresh on boot); subsequent
// ticks fire on the periodic ticker OR on ctx cancellation.
//
// Tick failures surface via OnError (if set) — runTickOnce owns
// the OnError firing so a single tick produces exactly one OnError
// event, regardless of whether the caller drove TickOnce or Run.
// Cancellation returns ctx.Err() verbatim.
//
// Returns ctx.Err() on clean cancellation, nil on a misconstructed
// poller (nil Client / zero Interval) — both are programmer
// errors caught at boot, NOT poll failures.
func (p *ProtectedAssetsPoller) Run(ctx context.Context) error {
	if p.Client == nil {
		return errors.New("worker.ProtectedAssetsPoller.Run: nil Client")
	}
	if p.Interval <= 0 {
		return errors.New("worker.ProtectedAssetsPoller.Run: zero Interval (call NewProtectedAssetsPoller instead of zeroing after construction)")
	}

	// Registration establishes the worker credential/session used by the
	// protected-assets endpoint. Do not poll before that gate is open: an
	// early request would be an avoidable 401 during bootstrap. Direct
	// TickOnce callers remain available for deterministic tests.
	if err := p.waitForRegistration(ctx); err != nil {
		return err
	}

	// Initial bootstrap fetch is retried until a valid 2xx snapshot is
	// received or the session context is cancelled. Once the registration
	// and authentication gate opens, a normal bootstrap must not expose a
	// transient 401 as the first observed poll result.
	for {
		if err := p.runTickOnce(ctx, "worker.ProtectedAssetsPoller: initial tick"); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Re-check registration on every poll. A reconnect can invalidate
			// the data-plane credential after the initial barrier opened.
			if err := p.waitForRegistration(ctx); err != nil {
				return err
			}
			// Discard the returned error: runTickOnce already fired OnError
			// if it was set. The Run loop never stops on a poll failure.
			p.runTickOnce(ctx, "worker.ProtectedAssetsPoller: tick")
		}
	}
}

// TickOnce runs one fetch synchronously and updates p.snap on
// success. Returns the underlying error to the caller verbatim
// (caller decides what to do with it; tests assert on it directly).
//
// Public so tests drive deterministically without the loop
// overhead — see the *_test.go file for the contract tests.
//
// CONTRACT:
//   - On success: p.snap is replaced; OnSuccess fires (if set);
//     fn returns nil.
//   - On failure: p.snap is UNAFFECTED; OnError fires (if set)
//     with the underlying error verbatim (no double-wrap); fn
//     returns the error.
//
// Run uses the shared runTickOnce helper with a label prefix so
// log lines from Run's automatic ticks carry "initial
// protected-assets poll" / "protected-assets poll" context;
// TickOnce passes an empty label so callers driving it directly
// see the raw error in OnError without prefix noise.
func (p *ProtectedAssetsPoller) TickOnce(ctx context.Context) error {
	return p.runTickOnce(ctx, "")
}

// runTickOnce is the shared fetch-and-notify helper used by both
// Run (with a label prefix for log identification) and
// TickOnce (with empty label for direct-call observability).
//
// Failure semantics are identical in both paths:
//   - p.snap is UNTOUCHED.
//   - p.OnError fires (if set) with the underlying error.
//   - The error is returned to the caller.
func (p *ProtectedAssetsPoller) runTickOnce(ctx context.Context, label string) error {
	snap, err := p.Client.GetProtectedAssets(ctx)
	if err != nil {
		if p.OnError != nil {
			if label != "" {
				p.OnError(fmt.Errorf("%s: %w", label, err))
			} else {
				p.OnError(err)
			}
		}
		return err
	}
	if snap == nil {
		if p.OnError != nil {
			p.OnError(ErrProtectedSnapshotNil)
		}
		return ErrProtectedSnapshotNil
	}
	generatedAt, parseErr := time.Parse(time.RFC3339Nano, snap.GeneratedAt)
	if snap.GeneratedAt == "" || parseErr != nil {
		err := ErrProtectedSnapshotInvalid
		if parseErr != nil {
			err = fmt.Errorf("%w: %v", ErrProtectedSnapshotInvalid, parseErr)
		}
		if p.OnError != nil {
			p.OnError(err)
		}
		return err
	}
	if p.SnapshotMaxAge > 0 && time.Since(generatedAt) > p.SnapshotMaxAge {
		if p.OnError != nil {
			p.OnError(ErrProtectedSnapshotStale)
		}
		return ErrProtectedSnapshotStale
	}
	if previous := p.Snapshot(); previous != nil {
		previousAt, previousErr := time.Parse(time.RFC3339Nano, previous.GeneratedAt)
		if previousErr == nil {
			older := false
			switch {
			case previous.Version > 0 && snap.Version > 0:
				// Version is authoritative when both snapshots carry it;
				// timestamps may come from a clock-skewed master.
				older = snap.Version < previous.Version ||
					(snap.Version == previous.Version && !generatedAt.After(previousAt))
			default:
				older = !generatedAt.After(previousAt)
			}
			if older {
				if p.OnError != nil {
					p.OnError(ErrProtectedSnapshotStale)
				}
				return ErrProtectedSnapshotStale
			}
		}
	}
	p.applySnapshot(snap)
	return nil
}

// waitForRegistration blocks the first request until the worker session is
// live and the shared API client has a bearer token. The client token may be
// populated before the gRPC Hello/Ack (for example from environment config),
// so checking both conditions prevents an early unauthenticated GET during
// the normal bootstrap race.
func (p *ProtectedAssetsPoller) waitForRegistration(ctx context.Context) error {
	for {
		registered := telemetry.GlobalReady().Snapshot().Registered
		// Do not assume an arbitrary ProtectedAssetsAPI is authenticated:
		// production uses *api.Client, but a fake or alternate client must
		// explicitly expose its current bearer token before Run may issue
		// the first request.
		authenticated := false
		if client, ok := p.Client.(interface{ AuthToken() string }); ok {
			authenticated = strings.TrimSpace(client.AuthToken()) != ""
		}
		if registered && authenticated {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// WaitReady blocks until the first valid protected-assets snapshot has
// been applied or ctx is cancelled. Poll failures intentionally do not
// return from this method: 401/503 and transport failures keep cleanup
// behind the barrier until a later poll succeeds.
func (p *ProtectedAssetsPoller) WaitReady(ctx context.Context) error {
	if p == nil {
		return errors.New("worker.ProtectedAssetsPoller.WaitReady: nil poller")
	}
	if p.readyCh == nil {
		return errors.New("worker.ProtectedAssetsPoller.WaitReady: barrier is not initialized")
	}
	select {
	case <-p.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsReady reports whether at least one valid snapshot has been applied.
func (p *ProtectedAssetsPoller) IsReady() bool {
	if p == nil || p.readyCh == nil {
		return false
	}
	select {
	case <-p.readyCh:
		return true
	default:
		return false
	}
}

// applySnapshot atomically swaps the in-memory snapshot reference.
// Centralised so the grow-only-by-reference invariant lives in
// one place — future enhancements (e.g. monotonic-version
// checking) plug in here without touching TickOnce.
func (p *ProtectedAssetsPoller) applySnapshot(snap *api.ProtectedAssetSnapshot) {
	p.mu.Lock()
	p.snap = snap
	p.mu.Unlock()
	p.readyOnce.Do(func() { close(p.readyCh) })
	if p.OnSuccess != nil {
		p.OnSuccess(snap)
	}
}

// Snapshot returns the most recent good snapshot pointer (or
// nil if no successful poll has happened yet). Thread-safe.
// The returned pointer is shared and must NOT be mutated by the
// caller; the poller is the sole writer.
func (p *ProtectedAssetsPoller) Snapshot() *api.ProtectedAssetSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snap
}

// Current adapts the poller's last successful snapshot to the
// workercache.SnapshotSource contract. A failed or not-yet-completed poll
// deliberately returns a zero timestamp, so cleanup remains fail-safe.
func (p *ProtectedAssetsPoller) Current(_ context.Context) (time.Time, []string, error) {
	snap := p.Snapshot()
	if snap == nil {
		return time.Time{}, nil, nil
	}
	generatedAt, err := time.Parse(time.RFC3339Nano, snap.GeneratedAt)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("parse protected-assets generated_at: %w", err)
	}
	ids := append([]string(nil), snap.DriveFileIDs...)
	ids = append(ids, snap.ProtectedAssetKeys...)
	return generatedAt.UTC(), ids, nil
}
