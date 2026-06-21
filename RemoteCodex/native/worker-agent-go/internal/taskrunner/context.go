package taskrunner

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/cache"
)

// CacheStatsProvider surfaces cache counters into the taskrunner for
// metrics. PR-3.7: pkg/cache.PersistedLocalCache satisfies this via
// its Stats() method (single source of truth; Stats() is the canonical
// public API).
type CacheStatsProvider interface {
	Stats() cache.CacheStats
}

// BlobStatsProvider surfaces blob counters into the taskrunner for
// metrics. PR-3.7: pkg/blob.BlobArtifacts satisfies this via its
// Stats() method.
type BlobStatsProvider interface {
	Stats() blob.BlobStats
}

// ContextOptions assemble the per-task ExecutionContext that the
// TaskRunner hands to Executor.Execute. Required: Logger, ParentCtx.
// Everything else has a safe noop default so callers can wire the
// minimum surface needed by their executor mix.
type ContextOptions struct {
	// Spec is the (already validated by the runner) TaskSpec for this run.
	// Embedded in the context so an executor can read its own JobID,
	// ExecutorID, Version, and Payload without taking a separate param.
	Spec executor.TaskSpec

	// ParentCtx is the worker's ctx for this task. Cancellation propagates
	// into the derived context that executors see as ExecutionContext.Done().
	ParentCtx context.Context

	// Logger is REQUIRED. Workers must surface executor activity. The
	// caller normally supplies a scopeLogger with executor_id + job_id
	// fields already attached.
	Logger executor.Logger

	// All other deps optional; zero-value = use the noop default.
	Clock      executor.Clock
	Telemetry  executor.Telemetry
	Resources  executor.ResourceLimits
	LocalCache executor.LocalCache
	Artifacts  executor.ArtifactAccess

	// PR-3.7: stats providers for surfacing cache + blob counters into
	// TaskExecutionReport.Metrics. Optional; nil falls back to noop.
	CacheStats CacheStatsProvider
	BlobStats  BlobStatsProvider
}

// runnerContext is the per-task ExecutionContext handed to Executor.Execute.
// PR-3 invariant: does not expose global mutable state to executors.
// Executor implementations must not assume long-lived shared state; the
// context is rebuilt for every Run.
type runnerContext struct {
	spec   executor.TaskSpec
	ctx    context.Context
	cancel context.CancelFunc

	logger     executor.Logger
	clock      executor.Clock
	telemetry  executor.Telemetry
	resources  executor.ResourceLimits
	cache      executor.LocalCache
	artifacts  executor.ArtifactAccess
	cacheStats CacheStatsProvider
	blobStats  BlobStatsProvider
}

func newRunnerContext(opts ContextOptions) (*runnerContext, error) {
	if opts.Logger == nil {
		return nil, fmt.Errorf("%w: ContextOptions.Logger is required", ErrInternalRunnerFault)
	}
	if opts.ParentCtx == nil {
		opts.ParentCtx = context.Background()
	}
	if opts.Clock == nil {
		opts.Clock = wallClock{}
	}
	if opts.Telemetry == nil {
		opts.Telemetry = noopTelemetry{}
	}
	if opts.Resources == nil {
		opts.Resources = DefaultResources()
	}
	if opts.LocalCache == nil {
		opts.LocalCache = noopCache{}
	}
	if opts.Artifacts == nil {
		opts.Artifacts = noopArtifacts{}
	}
	if opts.CacheStats == nil {
		opts.CacheStats = noopCacheStats{}
	}
	if opts.BlobStats == nil {
		opts.BlobStats = noopBlobStats{}
	}
	derived, cancel := context.WithCancel(opts.ParentCtx)
	return &runnerContext{
		spec:       opts.Spec,
		ctx:        derived,
		cancel:     cancel,
		logger:     opts.Logger,
		clock:      opts.Clock,
		telemetry:  opts.Telemetry,
		resources:  opts.Resources,
		cache:      opts.LocalCache,
		artifacts:  opts.Artifacts,
		cacheStats: opts.CacheStats,
		blobStats:  opts.BlobStats,
	}, nil
}

// Spec returns the validated TaskSpec the executor will run against.
// Read-only — the executor MUST NOT mutate this struct or any of its
// fields directly; lifecycle mutations go back through the master.
func (c *runnerContext) Spec() executor.TaskSpec { return c.spec }

func (c *runnerContext) Artifacts() executor.ArtifactAccess { return c.artifacts }
func (c *runnerContext) LocalCache() executor.LocalCache    { return c.cache }
func (c *runnerContext) Telemetry() executor.Telemetry      { return c.telemetry }
func (c *runnerContext) Resources() executor.ResourceLimits { return c.resources }
func (c *runnerContext) Clock() executor.Clock              { return c.clock }
func (c *runnerContext) Logger() executor.Logger            { return c.logger }

// CacheStats and BlobStats surface counters for the post-run metrics
// merge (PR-3.7). Both return noop zero snapshots when no provider
// was wired.
func (c *runnerContext) CacheStats() CacheStatsProvider { return c.cacheStats }
func (c *runnerContext) BlobStats() BlobStatsProvider   { return c.blobStats }

// Done is closed when the parent ctx is canceled AND when the runner
// explicitly fires Cancel(). PR-3 invariant #8: executors MUST check
// Done() in their inner loops.
func (c *runnerContext) Done() <-chan struct{} { return c.ctx.Done() }

// Err returns the cancellation cause (or nil if the derived ctx has
// not been canceled). Mirrors context.Context.Err semantics.
func (c *runnerContext) Err() error { return c.ctx.Err() }

// Cancel explicitly terminates the derived context. The runner uses
// this in panic-recovery paths; external code should prefer canceling
// the parent ctx.
func (c *runnerContext) Cancel() { c.cancel() }

// ── Default sub-interface implementations ──────────────────────────────────

// wallClock returns wall-clock UTC. Real impl would consult a
// monotonic-clock source; for PR-3.2 wall-clock UTC is enough.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

// fixedClock is a deterministic clock for tests.
type fixedClock struct{ T time.Time }

func (f fixedClock) Now() time.Time { return f.T }

// noopTelemetry discards spans. Executors must check the returned error
// if they record; today PR-3.2 returns nil from every call.
type noopTelemetry struct{}

func (noopTelemetry) Record(_ string, _ map[string]interface{}) error { return nil }

// noopCache: Get returns (nil, false, nil); Put is a no-op. The eventual
// PR-3.7 content-addressed cache replaces this.
type noopCache struct{}

func (noopCache) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}
func (noopCache) Put(_ context.Context, _ string, _ []byte) error { return nil }

// noopArtifacts: every Get fails with a sentinel; Put fails likewise.
// PR-3.2 explicit-fail policy matches the "no silent fallback" invariant.
type noopArtifacts struct{}

func (noopArtifacts) Get(_ context.Context, hash string) ([]byte, error) {
	return nil, fmt.Errorf("taskrunner: noopArtifacts.Get(%q): no artifact backend wired", hash)
}
func (noopArtifacts) Put(_ context.Context, hash string, _ []byte) error {
	return fmt.Errorf("taskrunner: noopArtifacts.Put(%q): no artifact backend wired", hash)
}

// noopCacheStats and noopBlobStats are zero-value fallbacks for the
// stats providers (PR-3.7). They keep report.Metrics merge safe when
// no persistent backend is wired; tests that don't pass providers
// still get a zero-valued metrics surface.
type noopCacheStats struct{}

func (noopCacheStats) Stats() cache.CacheStats { return cache.CacheStats{} }

type noopBlobStats struct{}

func (noopBlobStats) Stats() blob.BlobStats { return blob.BlobStats{} }

// staticResources yields a fixed resource snapshot. PR-3.6 sampler
// replaces this with dynamic sampling.
type staticResources struct {
	cpu  int
	mem  int64
	disk int64
	max  int
}

func (r staticResources) CPU() int           { return r.cpu }
func (r staticResources) MemoryMB() int64    { return r.mem }
func (r staticResources) DiskFreeGB() int64  { return r.disk }
func (r staticResources) MaxConcurrent() int { return r.max }

// DefaultResources probes runtime.GOMAXPROCS for CPU and uses zero values
// for mem/disk; the real ResourceLimits sampler arrives in PR-3.6.
func DefaultResources() executor.ResourceLimits {
	return staticResources{
		cpu:  runtime.GOMAXPROCS(0),
		mem:  0,
		disk: 0,
		max:  runtime.GOMAXPROCS(0),
	}
}
