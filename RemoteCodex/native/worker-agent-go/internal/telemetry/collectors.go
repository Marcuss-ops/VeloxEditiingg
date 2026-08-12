package telemetry

// collectors.go — CollectorRegistry and the four canonical per-attempt
// collectors.
//
// Dependency rule (see docs/worker-reliability-fixes.md §telemetry): a
// collector is the ONLY bridge from a producer's RAW facts into the
// AttemptSnapshot. Producers (cache, engine, sampler) never publish to a
// sink directly; the pipeline assembled by AttemptTelemetrySession
// collects facts at Stop and lets the sinks project from the snapshot.
// Collectors never compute a derived KPI — they only copy observed facts
// (the single Deriver owns every ratio).

import (
	"context"
	"strings"
	"sync"
	"time"
)

// AttemptIdentity identifies the attempt a snapshot describes. The worker
// stamps it at pipeline wiring time.
type AttemptIdentity struct {
	JobID      string `json:"job_id"`
	AttemptID  string `json:"attempt_id"`
	WorkerID   string `json:"worker_id"`
	ExecutorID string `json:"executor_id"`
}

// ProcessFacts are the observed process lifecycle facts of an attempt.
// They are RAW counters projected from the canonical journal (PROCESS
// events), never inferred from a wall clock (Fact Owner: ProcessRunner).
type ProcessFacts struct {
	// EngineSpawnCount is the number of canonical worker.engine.spawn
	// events recorded during the attempt.
	EngineSpawnCount int64 `json:"engine_spawn_count"`
}

// MediaFacts are the observed media-backend byte/frame totals projected
// from the canonical journal (Fact Owner: media backend). Zero when no
// media events were recorded (legacy engines).
type MediaFacts struct {
	BytesIn   int64 `json:"bytes_in"`
	BytesOut  int64 `json:"bytes_out"`
	FramesIn  int64 `json:"frames_in"`
	FramesOut int64 `json:"frames_out"`
}

// CacheFacts are the cache counters observed during the attempt. The
// CacheCollector diffs the cache provider's lifetime counters against its
// attempt-start baseline, so the snapshot carries the per-attempt delta.
type CacheFacts struct {
	Hits        int64 `json:"hits"`
	Misses      int64 `json:"misses"`
	Evictions   int64 `json:"evictions"`
	Corruptions int64 `json:"corruptions"`
	BytesUsed   int64 `json:"bytes_used"`
	Entries     int64 `json:"entries"`
}

// CacheFactsSource is the producer-owned cache fact surface. The worker
// adapts its concrete cache onto this interface; telemetry never imports
// the cache package (keeps the dependency direction producer→recorder).
type CacheFactsSource interface {
	CacheFacts() CacheFacts
}

// AttemptSnapshot is the canonical per-attempt bundle of RAW observed
// facts. It is the single input of every sink: Prometheus, receipt,
// benchmark and diagnostic projections all derive from this snapshot and
// never from producer-local state. It deliberately contains no derived
// ratios — those are produced by the single Deriver (pkg/performance).
type AttemptSnapshot struct {
	Identity AttemptIdentity `json:"identity"`
	// Resources is the raw typed fact envelope observed by the
	// AttemptTelemetrySession (Fact Owner: attempt_telemetry). The JSON
	// name remains "resources" for benchmark/diagnostic compatibility;
	// RawMetrics() is the explicit projection API used by sinks.
	Resources RawExecutionMetrics `json:"resources"`
	Process   ProcessFacts        `json:"process"`
	Media     MediaFacts          `json:"media"`
	Cache     CacheFacts          `json:"cache"`
	// Events is the attempt's canonical journal at Stop time (append-only
	// snapshot; the recorder itself is never drained).
	Events      []RecordedPhase `json:"events,omitempty"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt time.Time       `json:"completed_at"`
	WallMs      int64           `json:"wall_ms"`
}

// RawMetrics returns the snapshot's canonical raw execution envelope
// portion (resources plus cache counters). Process and media facts remain
// separate typed owner sections because their wire fields have different
// provenance. It is a value copy: projections cannot mutate the journal
// snapshot or producer-owned state.
func (s *AttemptSnapshot) RawMetrics() RawExecutionMetrics {
	if s == nil {
		return RawExecutionMetrics{}
	}
	return s.Resources
}

// RenderDurationMS returns the observed runner execute span from the
// append-only journal. Prometheus consumes this fact at the sink boundary;
// no producer writes the render histogram directly.
func (s *AttemptSnapshot) RenderDurationMS() (int64, bool) {
	if s == nil {
		return 0, false
	}
	for i := len(s.Events) - 1; i >= 0; i-- {
		event := s.Events[i]
		if event.Component != "runner" || event.Action != "execute" || event.DurationMS < 0 {
			continue
		}
		return event.DurationMS, true
	}
	return 0, false
}

// Collector copies one owner's RAW facts into the AttemptSnapshot.
// Collect must be side-effect free on anything but the snapshot: sinks
// are the only writers of derived state.
type Collector interface {
	Name() string
	Collect(ctx context.Context, snapshot *AttemptSnapshot)
}

// baselineCollector is the optional extension a collector implements when
// it needs an attempt-start baseline (e.g. the cache's lifetime counters).
// The pipeline calls StartBaseline exactly once, at attempt Start, before
// any Collect.
type baselineCollector interface {
	StartBaseline()
}

// CollectorRegistry owns the ordered collector set of one attempt.
// Registration is additive; Collect runs in registration order so a
// collector may read facts a previous collector placed in the snapshot.
type CollectorRegistry struct {
	mu         sync.Mutex
	collectors []Collector
}

func NewCollectorRegistry() *CollectorRegistry {
	return &CollectorRegistry{}
}

func (r *CollectorRegistry) Register(c Collector) {
	if c == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

// Collect runs every registered collector against the snapshot in
// registration order.
func (r *CollectorRegistry) Collect(ctx context.Context, snapshot *AttemptSnapshot) {
	r.mu.Lock()
	collectors := append([]Collector(nil), r.collectors...)
	r.mu.Unlock()
	for _, c := range collectors {
		c.Collect(ctx, snapshot)
	}
}

// StartBaseline starts every registered baselineCollector exactly once.
func (r *CollectorRegistry) StartBaseline() {
	r.mu.Lock()
	collectors := append([]Collector(nil), r.collectors...)
	r.mu.Unlock()
	for _, c := range collectors {
		if baseline, ok := c.(baselineCollector); ok {
			baseline.StartBaseline()
		}
	}
}

// Names returns the registered collector names in order (diagnostics/tests).
func (r *CollectorRegistry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.collectors))
	for _, c := range r.collectors {
		out = append(out, c.Name())
	}
	return out
}

func (r *CollectorRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.collectors)
}

// ── AttemptResourceCollector ──────────────────────────────────────────
// Fact Owner: attempt_telemetry (cgroup v2 / proc sampler). Copies the
// session's final RAW resource facts into the snapshot. The session is
// the single entry point, so the collector only reads the session's
// result — it never re-samples.

type AttemptResourceCollector struct {
	// Session is the bound AttemptTelemetrySession. Required.
	Session *AttemptTelemetrySession
}

func (c *AttemptResourceCollector) Name() string { return "attempt_resources" }

func (c *AttemptResourceCollector) Collect(_ context.Context, snapshot *AttemptSnapshot) {
	if c.Session == nil {
		return
	}
	result := c.Session.Result()
	snapshot.Resources = result.Metrics
	if snapshot.StartedAt.IsZero() {
		snapshot.StartedAt = result.StartedAt
	}
	if snapshot.CompletedAt.IsZero() {
		snapshot.CompletedAt = result.CompletedAt
	}
	if snapshot.WallMs == 0 {
		// Prefer the timestamp diff (integer ms); fall back to the float
		// wall clock only when the timestamps are unavailable.
		if !result.StartedAt.IsZero() && !result.CompletedAt.IsZero() {
			snapshot.WallMs = result.CompletedAt.Sub(result.StartedAt).Milliseconds()
		}
		if snapshot.WallMs == 0 {
			snapshot.WallMs = int64(result.Metrics.WallClockSeconds * 1000)
		}
	}
}

// ── ProcessCollector ──────────────────────────────────────────────────
// Fact Owner: ProcessRunner. Counts the canonical PROCESS_STARTED events
// (worker.engine.spawn, recorded only when cmd.Start() actually
// succeeded) from the attempt journal. No timing-derived inference.

type ProcessCollector struct{}

func (c *ProcessCollector) Name() string { return "process" }

func (c *ProcessCollector) Collect(_ context.Context, snapshot *AttemptSnapshot) {
	for _, event := range snapshot.Events {
		if event.Component == "worker.engine" && event.Action == "spawn" {
			snapshot.Process.EngineSpawnCount++
		}
	}
}

// ── MediaCollector ────────────────────────────────────────────────────
// Fact Owner: media backend (C++ engine / ffmpeg). Sums the byte/frame
// totals of every media producer event in the attempt journal.

type MediaCollector struct{}

func (c *MediaCollector) Name() string { return "media" }

func (c *MediaCollector) Collect(_ context.Context, snapshot *AttemptSnapshot) {
	for _, event := range snapshot.Events {
		if !isMediaProducer(event) {
			continue
		}
		snapshot.Media.BytesIn += event.BytesIn
		snapshot.Media.BytesOut += event.BytesOut
		snapshot.Media.FramesIn += event.FramesIn
		snapshot.Media.FramesOut += event.FramesOut
	}
}

// isMediaProducer classifies an event as media-backend-owned. The engine
// and ffmpeg/ffprobe components are the media producers; everything else
// (downloader, publisher, worker lifecycle) is a different Fact Owner.
func isMediaProducer(event RecordedPhase) bool {
	component := strings.ToLower(event.Component)
	return strings.HasPrefix(component, "engine.") ||
		strings.HasPrefix(component, "ffmpeg") ||
		strings.HasPrefix(component, "ffprobe")
}

// ── CacheCollector ────────────────────────────────────────────────────
// Fact Owner: cache resolver/provider. Diffs the provider's lifetime
// counters against the attempt-start baseline so the snapshot carries the
// per-attempt cache delta (hits/misses/evictions) plus the point-in-time
// size/entry gauges at attempt end.

type CacheCollector struct {
	Source      CacheFactsSource
	baseline    CacheFacts
	hasBaseline bool
}

func (c *CacheCollector) Name() string { return "cache" }

func (c *CacheCollector) StartBaseline() {
	if c.Source == nil {
		return
	}
	c.baseline = c.Source.CacheFacts()
	c.hasBaseline = true
}

func (c *CacheCollector) Collect(_ context.Context, snapshot *AttemptSnapshot) {
	if c.Source == nil {
		return
	}
	current := c.Source.CacheFacts()
	if !c.hasBaseline {
		c.baseline = CacheFacts{}
	}
	snapshot.Cache = CacheFacts{
		Hits:        positiveDelta(current.Hits - c.baseline.Hits),
		Misses:      positiveDelta(current.Misses - c.baseline.Misses),
		Evictions:   positiveDelta(current.Evictions - c.baseline.Evictions),
		Corruptions: positiveDelta(current.Corruptions - c.baseline.Corruptions),
		// Gauges are point-in-time: the current value, not a delta.
		// Note: the cache is process-global, so under concurrent attempts the
		// per-attempt delta includes other in-flight attempts' cache activity;
		// attribution is exact only at concurrency 1 (documented limitation).
		BytesUsed: current.BytesUsed,
		Entries:   current.Entries,
	}
	// The raw typed envelope is the sink input. Keep the structured Cache
	// view for existing diagnostic JSON, but stamp the same owner facts into
	// the canonical raw fields so every projection sees one value.
	snapshot.Resources.CacheLookups = snapshot.Cache.Hits + snapshot.Cache.Misses
	snapshot.Resources.AssetCacheHitCount = snapshot.Cache.Hits
	snapshot.Resources.AssetCacheMissCount = snapshot.Cache.Misses
}
