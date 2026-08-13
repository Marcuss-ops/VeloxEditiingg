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
	"encoding/json"
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

	// Engine-declared process facts projected from the canonical
	// worker.engine.usage event (recorded by the executor from the C++
	// sidecar process_counters block): the engine's own external spawn
	// ledger, its getrusage CPU, context switches and page faults. These
	// are DISJOINT from the /proc sampler's process-tree view. Zero when
	// the engine predates the block or no native render occurred.
	EngineExternalSpawnCount int64 `json:"engine_external_spawn_count"`
	EngineFfmpegSpawnCount   int64 `json:"engine_ffmpeg_spawn_count"`
	EngineFfprobeSpawnCount  int64 `json:"engine_ffprobe_spawn_count"`
	EngineShellSpawnCount    int64 `json:"engine_shell_spawn_count"`
	EngineCurlSpawnCount     int64 `json:"engine_curl_spawn_count"`

	EngineCPUUserMs                  int64 `json:"engine_cpu_user_ms"`
	EngineCPUSystemMs                int64 `json:"engine_cpu_system_ms"`
	EngineVoluntaryContextSwitches   int64 `json:"engine_voluntary_context_switches"`
	EngineInvoluntaryContextSwitches int64 `json:"engine_involuntary_context_switches"`
	EngineMinorPageFaults            int64 `json:"engine_minor_page_faults"`
	EngineMajorPageFaults            int64 `json:"engine_major_page_faults"`
}

// EngineUsageFacts is the metadata payload of the canonical
// worker.engine.usage journal event: the engine-declared process facts
// (C++ sidecar process_counters block) carried from the executor to the
// ProcessCollector. It is the single typed shape shared by the producer
// (executor marshal) and the consumer (collector unmarshal) so the
// wire contract can never drift.
type EngineUsageFacts struct {
	ExternalSpawnCount         int64 `json:"external_spawn_count"`
	FfmpegSpawnCount           int64 `json:"ffmpeg_spawn_count"`
	FfprobeSpawnCount          int64 `json:"ffprobe_spawn_count"`
	ShellSpawnCount            int64 `json:"shell_spawn_count"`
	CurlSpawnCount             int64 `json:"curl_spawn_count"`
	CPUUserMs                  int64 `json:"cpu_user_ms"`
	CPUSystemMs                int64 `json:"cpu_system_ms"`
	VoluntaryContextSwitches   int64 `json:"voluntary_context_switches"`
	InvoluntaryContextSwitches int64 `json:"involuntary_context_switches"`
	MinorPageFaults            int64 `json:"minor_page_faults"`
	MajorPageFaults            int64 `json:"major_page_faults"`
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

// CompleteRawEnvelope is the complete typed RAW fact envelope for one
// attempt. It deliberately groups all fact owners that used to be exposed
// as separate snapshot sections: resource/process/media/cache. Consumers
// must read this envelope rather than joining producer-specific fields
// themselves. Derived ratios are not part of the envelope.
//
// The JSON shape intentionally mirrors the historical AttemptSnapshot
// sections. This makes the envelope safe for benchmark/diagnostic adapters
// while allowing the in-memory consumer API to converge on one raw input.
type CompleteRawEnvelope struct {
	Resources RawExecutionMetrics `json:"resources"`
	Process   ProcessFacts        `json:"process"`
	Media     MediaFacts          `json:"media"`
	Cache     CacheFacts          `json:"cache"`
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
	// RawMetrics() is the explicit complete-envelope projection API used by
	// sinks.
	Resources RawExecutionMetrics `json:"resources"`
	Process   ProcessFacts        `json:"process"`
	Media     MediaFacts          `json:"media"`
	Cache     CacheFacts          `json:"cache"`
	// Events is the attempt's canonical journal at Stop time (append-only
	// snapshot; the recorder itself is never drained).
	Events        []RecordedPhase `json:"events,omitempty"`
	DroppedEvents int64           `json:"dropped_events,omitempty"`
	InvalidEvents int64           `json:"invalid_events,omitempty"`
	StartedAt     time.Time       `json:"started_at"`
	CompletedAt   time.Time       `json:"completed_at"`
	WallMs        int64           `json:"wall_ms"`

	// raw is the canonical in-memory envelope. The exported top-level
	// sections above remain compatibility projections for existing JSON/wire
	// consumers and legacy fixtures; producers and projections use raw via
	// RawEnvelope/applyRawEnvelope.
	raw      CompleteRawEnvelope
	rawValid bool
}

// Clone returns an immutable-boundary copy for a sink. The pipeline owns the
// original snapshot; each projection receives an independent value so a
// buggy or future third-party sink cannot mutate facts observed by another
// sink or by a later diagnostic retry.
func (s *AttemptSnapshot) Clone() *AttemptSnapshot {
	if s == nil {
		return nil
	}
	clone := *s
	if s.Events != nil {
		clone.Events = make([]RecordedPhase, len(s.Events))
		copy(clone.Events, s.Events)
	}
	return &clone
}

// RawEnvelope returns the complete typed raw fact envelope. It is a value
// copy: projections cannot mutate the journal snapshot or producer-owned
// state. The legacy snapshot fields remain the wire/JSON compatibility view
// and are populated from the same envelope by collectors.
func (s *AttemptSnapshot) RawEnvelope() CompleteRawEnvelope {
	if s == nil {
		return CompleteRawEnvelope{}
	}
	if s.rawValid {
		return s.raw
	}
	// Seed the canonical envelope from the historical fields for callers
	// that construct snapshots from the pre-envelope JSON/wire shape.
	return CompleteRawEnvelope{
		Resources: s.Resources,
		Process:   s.Process,
		Media:     s.Media,
		Cache:     s.Cache,
	}
}

// applyRawEnvelope updates the historical snapshot sections from the single
// typed envelope. Keeping this adapter local to telemetry preserves the
// existing JSON/wire shape without maintaining a second producer-owned
// representation.
func (s *AttemptSnapshot) applyRawEnvelope(raw CompleteRawEnvelope) {
	if s == nil {
		return
	}
	s.raw = raw
	s.rawValid = true
	s.Resources = raw.Resources
	s.Process = raw.Process
	s.Media = raw.Media
	s.Cache = raw.Cache
}

// RawMetrics returns the complete typed raw envelope. The method name is
// retained because it is the established snapshot projection API; callers
// that need only the historical resource section should use
// RawResourceMetrics instead.
func (s *AttemptSnapshot) RawMetrics() CompleteRawEnvelope {
	return s.RawEnvelope()
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
	raw := snapshot.RawEnvelope()
	if executorRaw, ok := c.Session.ExecutorRawMetrics(); ok {
		raw.Resources = executorRaw
	}
	MergeAttemptResourceFactsInto(&raw.Resources, result.Metrics)
	snapshot.applyRawEnvelope(raw)
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
	raw := snapshot.RawEnvelope()
	// ProcessCollector owns this section; repeated collection is a fresh
	// projection, not an accumulation of the previous projection.
	raw.Process = ProcessFacts{}
	for _, event := range snapshot.Events {
		if event.Component == "worker.engine" && event.Action == "spawn" {
			raw.Process.EngineSpawnCount++
		}
		if event.Component == "worker.engine" && event.Action == "usage" {
			projectEngineUsage(event.MetadataJSON, &raw.Process)
		}
	}
	snapshot.applyRawEnvelope(raw)
}

// EngineUsageMetadataJSON marshals EngineUsageFacts into the canonical
// worker.engine.usage metadata payload. It lives here (next to the
// consumer) so the executor producer never hand-builds the wire JSON.
func EngineUsageMetadataJSON(usage EngineUsageFacts) string {
	if usage.ExternalSpawnCount == 0 && usage.FfmpegSpawnCount == 0 &&
		usage.FfprobeSpawnCount == 0 && usage.ShellSpawnCount == 0 &&
		usage.CurlSpawnCount == 0 && usage.CPUUserMs == 0 &&
		usage.CPUSystemMs == 0 && usage.VoluntaryContextSwitches == 0 &&
		usage.InvoluntaryContextSwitches == 0 && usage.MinorPageFaults == 0 &&
		usage.MajorPageFaults == 0 {
		return ""
	}
	b, err := json.Marshal(usage)
	if err != nil {
		return ""
	}
	return string(b)
}

// projectEngineUsage parses the canonical worker.engine.usage metadata
// payload into ProcessFacts. A malformed payload is skipped (fail-open:
// the journal still carries the raw event; the receipt keeps zero facts)
// so one bad producer can never corrupt an attempt's process facts.
func projectEngineUsage(metadataJSON string, facts *ProcessFacts) {
	if facts == nil || metadataJSON == "" {
		return
	}
	var usage EngineUsageFacts
	if err := json.Unmarshal([]byte(metadataJSON), &usage); err != nil {
		return
	}
	facts.EngineExternalSpawnCount = usage.ExternalSpawnCount
	facts.EngineFfmpegSpawnCount = usage.FfmpegSpawnCount
	facts.EngineFfprobeSpawnCount = usage.FfprobeSpawnCount
	facts.EngineShellSpawnCount = usage.ShellSpawnCount
	facts.EngineCurlSpawnCount = usage.CurlSpawnCount
	facts.EngineCPUUserMs = usage.CPUUserMs
	facts.EngineCPUSystemMs = usage.CPUSystemMs
	facts.EngineVoluntaryContextSwitches = usage.VoluntaryContextSwitches
	facts.EngineInvoluntaryContextSwitches = usage.InvoluntaryContextSwitches
	facts.EngineMinorPageFaults = usage.MinorPageFaults
	facts.EngineMajorPageFaults = usage.MajorPageFaults
}

// ── MediaCollector ────────────────────────────────────────────────────
// Fact Owner: media backend (C++ engine / ffmpeg). Sums the byte/frame
// totals of every media producer event in the attempt journal.

type MediaCollector struct{}

func (c *MediaCollector) Name() string { return "media" }

func (c *MediaCollector) Collect(_ context.Context, snapshot *AttemptSnapshot) {
	raw := snapshot.RawEnvelope()
	// MediaCollector owns this section; repeated collection is idempotent.
	raw.Media = MediaFacts{}
	for _, event := range snapshot.Events {
		if !isMediaProducer(event) {
			continue
		}
		raw.Media.BytesIn += event.BytesIn
		raw.Media.BytesOut += event.BytesOut
		raw.Media.FramesIn += event.FramesIn
		raw.Media.FramesOut += event.FramesOut
	}
	snapshot.applyRawEnvelope(raw)
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
	raw := snapshot.RawEnvelope()
	current := c.Source.CacheFacts()
	if !c.hasBaseline {
		c.baseline = CacheFacts{}
	}
	raw.Cache = CacheFacts{
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
	// Cache counters also remain in the resource compatibility view because
	// that is part of the historical JSON/wire contract. The envelope is the
	// owner-facing source; this is only a compatibility projection.
	raw.Resources.CacheLookups = raw.Cache.Hits + raw.Cache.Misses
	raw.Resources.AssetCacheHitCount = raw.Cache.Hits
	raw.Resources.AssetCacheMissCount = raw.Cache.Misses
	snapshot.applyRawEnvelope(raw)
}
