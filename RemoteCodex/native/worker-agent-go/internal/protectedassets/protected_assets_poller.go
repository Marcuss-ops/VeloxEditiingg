// Package protectedassets — protected-asset snapshot poller (Pass 7).
//
// The worker periodically fetches GET
// /api/v1/agent/cache/protected-assets (the canonical Phase 6
// agent namespace; delivered in Pass 6 of the master plan) and
// keeps the most recent SUCCESSFUL
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
//     cleanup loop receives an error from Current and performs no
//     destructive pass until a later valid 200 clears lastPollErr.
//     The poller never evicts or clears the last valid snapshot. This
//     is what the user-spec requires: "Se lo snapshot fallisce, NON
//     svuotare la cache locale."
//
//  2. A successful poll with empty ProtectedAssetKeys is NOT a
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

package protectedassets

import (
	"errors"
	"sync"
	"time"

	"velox-worker-agent/internal/workercache"

	"velox-worker-agent/pkg/api"
)

// ErrPollerInvalidInterval is the canonical error returned when
// NewProtectedAssetsPoller receives Interval <= 0. The constructor
// logs-and-defaults rather than panicking, but the helper that
// surfaces the error during bootstrap is exported for callers that
// prefer hard-fail semantics.
var ErrPollerInvalidInterval = errors.New("protectedassets.ProtectedAssetsPoller: interval must be > 0; defaulting to 30s")
var ErrProtectedSnapshotNil = errors.New("protectedassets.ProtectedAssetsPoller: successful response contained no snapshot")
var ErrProtectedSnapshotInvalid = errors.New("protectedassets.ProtectedAssetsPoller: snapshot generated_at is invalid")
var ErrProtectedSnapshotStale = errors.New("protectedassets.ProtectedAssetsPoller: snapshot generated_at is older than the last valid snapshot")
var ErrProtectedSnapshotSessionUnavailable = errors.New("protectedassets.ProtectedAssetsPoller: worker session is not registered")

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
	//     ("protectedassets.ProtectedAssetsPoller: initial tick" /
	//     "protectedassets.ProtectedAssetsPoller: tick") so on-call
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

	mu          sync.RWMutex
	snap        *api.ProtectedAssetSnapshot
	lastPollErr error

	// sessionGated is enabled by Run. Direct TickOnce callers remain
	// deterministic and do not need to manufacture registration state.
	sessionGated bool
	sessionEpoch uint64

	// readyMu protects a re-armable readiness barrier. A disconnect
	// replaces readyCh with a fresh channel; the next valid snapshot
	// closes that channel and reopens cleanup/readiness atomically.
	readyMu sync.Mutex
	ready   bool
	readyCh chan struct{}
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
