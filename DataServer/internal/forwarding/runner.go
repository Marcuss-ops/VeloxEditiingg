// Package forwarding provides the CreatorForwardingRunner — a durable,
// lease-based, supervised background runner that replaces the volatile
// in-memory scheduleCreatorPolling goroutines.
//
// The runner claims PENDING/RETRY_WAIT creator_forwardings rows,
// polls the remote creator engine for completion, and transitions
// completed rows to READY_TO_FORWARD so the downstream ForwardingService
// can enqueue them as proper Velox Jobs.
//
// Architecture:
//
//	tick → ClaimCreatorForwardings (batch) → for each lease:
//	  → start lease-renewal goroutine (every leaseDuration/3)
//	  → poll remote engine via GetPipelineStatus
//	  → on terminal+success → MarkCreatorForwardingReadyToForward
//	  → on terminal+failure → MarkCreatorForwardingFailed
//	  → on transient error    → MarkCreatorForwardingRetry (backoff)
//	  → cancel renewal goroutine
//
// The runner uses a bounded semaphore for concurrency and exposes
// lightweight metrics (queue depth, oldest pending age) for monitoring.
package forwarding

import (
	"fmt"
	"sync"
	"time"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// ── Runner ───────────────────────────────────────────────────────────────

// CreatorForwardingRunner drives the persistent polling of remote creator
// jobs and transitions completed results atomically to FORWARDED (Job+Task
// creation + forwarding status update in a single SQLite transaction).
type CreatorForwardingRunner struct {
	cfg          *RunnerConfig
	dbStore      *store.SQLiteStore
	client       *remoteengine.Client
	enqueuer     *enqueue.Enqueuer
	identity     string
	metrics      *RunnerMetrics
	resolver     *creatorflow.Resolver // canonical forward-completed entry point
	resolverOnce sync.Once             // guards lazyResolver against concurrent first-call race

	sem chan struct{} // bounded concurrency

	mu        sync.Mutex
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NewCreatorForwardingRunner wires a runner. dbStore is the durable anchor;
// client provides access to the remote creator engine. enqueuer is optional
// (nil-safe) — when set, the runner handles the full poll+enqueue lifecycle
// atomically; when nil, the runner only polls and stores payloads.
//
// Resolver is built lazily via NewResolverMinimal on first call to
// atomicEnqueueAndForward; callers may pre-build it via SetResolver to
// share a single Resolver instance with the HTTP handler.
func NewCreatorForwardingRunner(cfg *RunnerConfig, dbStore *store.SQLiteStore, client *remoteengine.Client, enqueuer *enqueue.Enqueuer, identity string) *CreatorForwardingRunner {
	if cfg == nil {
		cfg = DefaultRunnerConfig()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if identity == "" {
		identity = fmt.Sprintf("cf-runner-%d", time.Now().UnixNano())
	}
	return &CreatorForwardingRunner{
		cfg:       cfg,
		dbStore:   dbStore,
		client:    client,
		enqueuer:  enqueuer,
		identity:  identity,
		metrics:   &RunnerMetrics{},
		sem:       make(chan struct{}, cfg.Concurrency),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// SetResolver injects a pre-built Resolver so the runner uses the same
// instance the HTTP handler uses. Without this call the runner builds
// its own NewResolverMinimal on first use; both paths produce the
// same job_id + forwarding_id because both go through Resolver.Resolve.
//
// MUST be called before Run() starts. Calling SetResolver concurrently
// with lazyResolver (which fires resolverOnce) is a data race on
// r.resolver. In practice SetResolver is a composition-root / startup
// call, never called from a processing goroutine.
//
// Blocco 5 of the Verdetto (P1 #11): the runner's atomicEnqueueAndForward
// delegates to Resolver.Resolve rather than building the Job+TaskSpec
// inline + calling AtomicForwardAndEnqueue. This unifies the
// forward-completed path: handler and runner converge on the Resolver's
// idempotency + payload-marshal + URL-rewrite + atomic-write pipeline.
func (r *CreatorForwardingRunner) SetResolver(rs *creatorflow.Resolver) {
	if r == nil {
		return
	}
	r.resolver = rs
}

// lazyResolver returns the runner's Resolver, building a minimal one
// on first use if SetResolver was not called. The minimal resolver
// skips URL rewriting (the runner already received a complete payload
// from the remote engine) but owns the full atomic + idempotency flow.
//
// Thread-safety: processLease is called from concurrent goroutines
// (bounded by r.sem). Without a guard, two goroutines could both see
// r.resolver == nil and each build a separate minimal Resolver — a
// data race on the r.resolver field. sync.Once guarantees exactly one
// initialisation across all goroutines; subsequent calls are lock-free.
func (r *CreatorForwardingRunner) lazyResolver() *creatorflow.Resolver {
	r.resolverOnce.Do(func() {
		if r.resolver == nil {
			r.resolver = creatorflow.NewResolverMinimal(r.enqueuer, r.dbStore)
		}
	})
	return r.resolver
}

// Metrics returns the runner's metrics for external consumption.
func (r *CreatorForwardingRunner) Metrics() *RunnerMetrics {
	return r.metrics
}

// Stop signals the runner to exit after the in-flight tick completes.
func (r *CreatorForwardingRunner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	<-r.stoppedCh
}
