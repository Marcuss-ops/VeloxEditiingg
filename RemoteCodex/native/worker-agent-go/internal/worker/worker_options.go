// Package worker — construction options.
//
// worker_options.go owns the Option functional-options surface and every
// With* setter. The concrete construction wiring stays in worker_init.go's
// New(), which reads the populated workerOptions.
package worker

import (
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/cache"
)

// Option configures a Worker returned by New. Backward-compatible:
// existing callers passing only (cfg, version) keep working.
type Option func(*workerOptions)

type workerOptions struct {
	registry            *executor.Registry
	cache               *cache.PersistedLocalCache
	blobs               *blob.BlobArtifacts
	clipCache           *workercache.Cache
	onWorkerIDCollision func(err error)
}

// WithRegistry replaces the default (empty) executor registry. The
// caller owns the registry — Register calls after New() still take
// effect because the worker holds the same pointer.
// // This is the single supported way to surface hello/heartbeat
// capabilities.
//
// Passing nil panics loudly. The previous silent fallback to a fresh
// empty registry masked operator bugs (worker booted, advertised zero
// executors, every job routed to dead-letter). Loud startup is the
// correct safety posture.
func WithRegistry(reg *executor.Registry) Option {
	if reg == nil {
		panic("worker.WithRegistry: registry must not be nil — pass an explicit *executor.Registry or omit WithRegistry")
	}
	return func(o *workerOptions) {
		o.registry = reg
	}
}

// WithCache wires a persistent local cache into the worker.
// The same instance is exposed via Worker.cache and is threaded into
// the TaskRunner built by New() so cache hits/misses/evictions/
// corruptions appear in TaskExecutionReport.Metrics.
//
// Passing nil panics loudly; omit WithCache to fall back to noop
// defaults (useful only for unit tests that don't exercise the
// cache surface).
func WithCache(c *cache.PersistedLocalCache) Option {
	if c == nil {
		panic("worker.WithCache: cache must not be nil — pass an explicit *cache.PersistedLocalCache or omit WithCache")
	}
	return func(o *workerOptions) {
		o.cache = c
	}
}

// WithBlobs wires a content-addressed blob store into the worker. The same
// instance is exposed via Worker.blobs and threaded into the TaskRunner built
// by New(); master publication remains owned by the artifact lifecycle.
//
// Passing nil panics loudly; omit WithBlobs only for test/headless profiles.
func WithBlobs(b *blob.BlobArtifacts) Option {
	if b == nil {
		panic("worker.WithBlobs: blobs must not be nil — pass an explicit *blob.BlobArtifacts or omit WithBlobs")
	}
	return func(o *workerOptions) {
		o.blobs = b
	}
}

// WithClipCache wires the worker-side workercache.Cache into the
// Worker. When set, dispatchTaskRunner acquires a per-job lease relation
// on every cached asset referenced by the job payload BEFORE invoking
// taskRunner.Run, and a defer at the same scope releases it on
// success/error/panic so the workercache.Cleanup loop never deletes an
// asset inside an active render.
//
// Passing nil panics loudly; omit WithClipCache only for legacy
// bootstrap profiles, headless tests, and workers without a clip-cache
// SQLite. CompiledRenderPlanV2 dispatch is fail-closed when this cache
// is absent because its assets must be leased before execution.
func WithClipCache(c *workercache.Cache) Option {
	if c == nil {
		panic("worker.WithClipCache: cache must not be nil — pass an explicit *workercache.Cache or omit WithClipCache")
	}
	return func(o *workerOptions) {
		o.clipCache = c
	}
}

// WithCollisionObserver installs a callback invoked when the master
// rejects the worker's Hello handshake with codes.AlreadyExists because
// another machine is already registered with the same worker_id on a
// different credential (anti-collision invariant RW-PROD-005 §3).
//
// The callback is the SINGLE point of policy for "what should the
// worker do when this happens". The default production handler in
// cmd/velox-worker-agent/main.go logs the diagnostic to stderr and
// calls os.Exit(17) — a hard configuration error (two physical
// machines sharing a worker_id) is not safe to retry with backoff
// because doing so would mask the underlying operational fault and
// keep both machines in a flaky thrash.
//
// A non-nil callback REPLACES the default (no-op) behavior. Pass
// nil to opt out of the observer entirely (legacy / override mode
// where the operator accepts that two machines may register with
// the same worker_id and prefers the worker to keep trying with
// backoff instead of exiting). In production the
// VELOX_ALLOW_MULTI_HOST_WORKER_IDS env var (default false) gates
// whether the observer is wired at all.
//
// The callback receives the underlying ErrWorkerIDCollision-wrapped
// error for log context (peer IP, original gRPC status, etc.). It
// MUST be safe to call from the Start() goroutine context; the
// worker holds no locks during invocation.
func WithCollisionObserver(fn func(err error)) Option {
	return func(o *workerOptions) {
		o.onWorkerIDCollision = fn
	}
}
