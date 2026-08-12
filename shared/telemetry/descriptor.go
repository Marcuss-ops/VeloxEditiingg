package telemetry

// descriptor.go extends the canonical event taxonomy with the semantic
// attributes every projection needs: what a fact measures (Kind), how its
// timing is accounted (TimingMode), how it aggregates across events
// (Aggregation), the resource dimension that bounds its occurrence
// (Cardinality), and the single authoritative producer (Owner — the Fact
// Owner rule).
//
// Single-source rule: these are closed vocabularies, declared once here and
// represented by name in catalog.json. Consumers must never invent a
// Kind/Owner/unit at runtime; adding a member means editing the language-
// neutral source and regenerating the bindings.

import "fmt"

// ── MetricUnit: canonical unit carried by the language-neutral catalog ─────
type MetricUnit string

const (
	UnitCount        MetricUnit = "count"
	UnitMilliseconds MetricUnit = "milliseconds"
	UnitBytes        MetricUnit = "bytes"
	UnitRatio        MetricUnit = "ratio"
	UnitItems        MetricUnit = "items"
	UnitFrames       MetricUnit = "frames"
)

func IsCanonicalMetricUnit(s string) bool {
	switch MetricUnit(s) {
	case UnitCount, UnitMilliseconds, UnitBytes, UnitRatio, UnitItems, UnitFrames:
		return true
	}
	return false
}

// ── EventKind: what the fact measures ──────────────────────────────────────
type EventKind string

const (
	// KindCounter is a monotonic count of discrete occurrences (packets
	// written, processes spawned, cache misses). Aggregates by SUM.
	KindCounter EventKind = "counter"
	// KindGauge is an instantaneous sampled value (output stat, queue
	// depth). Aggregates by LAST.
	KindGauge EventKind = "gauge"
	// KindDuration is a timed operation that is an exclusive, non-overlapping
	// top-level phase of the attempt (the only kind that feeds the
	// accounted_ratio sum).
	KindDuration EventKind = "duration"
	// KindHistogram is a distribution of samples (latency buckets). Reserved
	// for future projections; no canonical entry uses it yet.
	KindHistogram EventKind = "histogram"
	// KindSpan is a timed span with parent/child structure (parallel segment
	// work inside one phase). Span durations are EXCLUDED from the exclusive
	// accounted_ratio sum — see TimingMode. Reserved for the C++ engine
	// span stream; no canonical entry uses it yet.
	KindSpan EventKind = "span"
)

// IsCanonicalEventKind reports whether s is a member of the closed
// EventKind vocabulary.
func IsCanonicalEventKind(s string) bool {
	switch EventKind(s) {
	case KindCounter, KindGauge, KindDuration, KindHistogram, KindSpan:
		return true
	}
	return false
}

// ── TimingMode: how a timed fact is accounted ──────────────────────────────
type TimingMode string

const (
	// TimingNone marks non-timed facts (counters, gauges).
	TimingNone TimingMode = "none"
	// TimingExclusive marks a TOP-LEVEL phase of the attempt: attempt-scoped
	// and non-overlapping with sibling exclusive phases by construction
	// (cardinality MUST be per_attempt). Exclusive phases are the ONLY facts
	// summed into accounted_ratio — never child spans, whose parallel
	// instances would double-count against wall clock.
	TimingExclusive TimingMode = "exclusive"
	// TimingSpanParent marks a span that CONTAINS child spans. Its own
	// duration overlaps its children, so it is EXCLUDED from the exclusive
	// accounted_ratio sum (summing parent + children would double-count).
	// Reserved for the C++ engine span stream; no canonical entry uses it yet.
	TimingSpanParent TimingMode = "span_parent"
	// TimingSpanChild marks a timed operation nested under a top-level phase
	// whose instances can overlap (parallel segments, parallel asset
	// transfers). Child spans are NEVER summed into accounted_ratio.
	TimingSpanChild TimingMode = "span_child"
)

// IsCanonicalTimingMode reports whether s is a member of the closed
// TimingMode vocabulary.
func IsCanonicalTimingMode(s string) bool {
	switch TimingMode(s) {
	case TimingNone, TimingExclusive, TimingSpanParent, TimingSpanChild:
		return true
	}
	return false
}

// ── AggregationMode: how multiple observations combine ─────────────────────
type AggregationMode string

const (
	AggSum   AggregationMode = "sum"
	AggMax   AggregationMode = "max"
	AggMin   AggregationMode = "min"
	AggAvg   AggregationMode = "avg"
	AggLast  AggregationMode = "last"
	AggCount AggregationMode = "count"
)

// IsCanonicalAggregationMode reports whether s is a member of the closed
// AggregationMode vocabulary.
func IsCanonicalAggregationMode(s string) bool {
	switch AggregationMode(s) {
	case AggSum, AggMax, AggMin, AggAvg, AggLast, AggCount:
		return true
	}
	return false
}

// ── CardinalityPolicy: the resource dimension bounding occurrence ──────────
type CardinalityPolicy string

const (
	CardPerAttempt  CardinalityPolicy = "per_attempt"
	CardPerTask     CardinalityPolicy = "per_task"
	CardPerJob      CardinalityPolicy = "per_job"
	CardPerSegment  CardinalityPolicy = "per_segment"
	CardPerAsset    CardinalityPolicy = "per_asset"
	CardPerArtifact CardinalityPolicy = "per_artifact"
	CardPerTrack    CardinalityPolicy = "per_track"
)

// IsCanonicalCardinalityPolicy reports whether s is a member of the closed
// CardinalityPolicy vocabulary.
func IsCanonicalCardinalityPolicy(s string) bool {
	switch CardinalityPolicy(s) {
	case CardPerAttempt, CardPerTask, CardPerJob, CardPerSegment,
		CardPerAsset, CardPerArtifact, CardPerTrack:
		return true
	}
	return false
}

// ── ComponentOwner: the single authoritative producer of a fact ────────────
//
// The Fact Owner rule: every fact has exactly one authoritative producer,
// and no other component may reconstruct it. The table mirrors
// fact_owner.go for receipt-level facts and is stamped on every canonical
// event descriptor here. Owners are closed: a producer must be a member of
// this vocabulary or it does not exist as an owner.
type ComponentOwner string

const (
	// OwnerMaster is the master control plane (db, control, placement,
	// intake, offer, queue).
	OwnerMaster ComponentOwner = "master"
	// OwnerWorker is the worker orchestration layer (runner lifecycle,
	// parallelism, temp housekeeping).
	OwnerWorker ComponentOwner = "worker"
	// OwnerTaskRunner owns the task status fact (runner.execute/run/report).
	OwnerTaskRunner ComponentOwner = "task_runner"
	// OwnerAttemptTelemetry owns CPU/RAM/disk observation of the attempt.
	OwnerAttemptTelemetry ComponentOwner = "attempt_telemetry"
	// OwnerDownloader owns downloaded bytes (worker.asset).
	OwnerDownloader ComponentOwner = "downloader"
	// OwnerCacheResolver owns cache hit/miss (worker.cache).
	OwnerCacheResolver ComponentOwner = "cache_resolver"
	// OwnerProcessRunner owns process spawn/lifecycle (worker.engine, ffmpeg).
	OwnerProcessRunner ComponentOwner = "process_runner"
	// OwnerMediaEngine owns media backend packet reads / demux / probes /
	// composite primitives (engine.*, engine.input.*).
	OwnerMediaEngine ComponentOwner = "media_engine"
	// OwnerDecoder owns frames decoded (engine.video.decode, audio decodes).
	OwnerDecoder ComponentOwner = "decoder"
	// OwnerEncoder owns frames encoded (engine.encode.*, engine.audio.encode).
	OwnerEncoder ComponentOwner = "encoder"
	// OwnerMuxer owns mux bytes (engine.mux.*, engine.output.fsync).
	OwnerMuxer ComponentOwner = "muxer"
	// OwnerPublisher owns the artifact SHA / final artifact verification.
	OwnerPublisher ComponentOwner = "publisher"
	// OwnerUploader owns upload transport bytes (worker.upload, worker.output).
	OwnerUploader ComponentOwner = "uploader"
	// OwnerValidation owns quality/validation scans (quality.*, subtitle
	// validation, io/audio/retry/waste summaries).
	OwnerValidation ComponentOwner = "validation"
	// OwnerRenderPlan owns clip count and expected duration (master.plan,
	// worker.plan).
	OwnerRenderPlan ComponentOwner = "render_plan"
	// OwnerAssetManifest owns the asset SHA/manifest identity
	// (master.manifest, worker.plan.resolve_assets).
	OwnerAssetManifest ComponentOwner = "asset_manifest"
	// OwnerWorkerState owns the worker status fact (state machine; no
	// canonical event uses it yet — reserved for the worker status fact).
	OwnerWorkerState ComponentOwner = "worker_state"
)

// IsCanonicalComponentOwner reports whether s is a member of the closed
// ComponentOwner vocabulary.
func IsCanonicalComponentOwner(s string) bool {
	switch ComponentOwner(s) {
	case OwnerMaster, OwnerWorker, OwnerTaskRunner, OwnerAttemptTelemetry,
		OwnerDownloader, OwnerCacheResolver, OwnerProcessRunner,
		OwnerMediaEngine, OwnerDecoder, OwnerEncoder, OwnerMuxer,
		OwnerPublisher, OwnerUploader, OwnerValidation, OwnerRenderPlan,
		OwnerAssetManifest, OwnerWorkerState:
		return true
	}
	return false
}

// ValidateEventDescriptorSemantics checks the semantic attributes of one
// canonical descriptor: all semantic fields are required members of their
// closed vocabularies, and Kind/Unit/TimingMode are mutually consistent. It
// is called by the catalog constructor so malformed source is a startup
// failure, never a silent taxonomy hole.
func ValidateEventDescriptorSemantics(d EventDescriptor) error {
	if d.Kind == "" || !IsCanonicalEventKind(string(d.Kind)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing kind %q", d.Key(), d.Kind)
	}
	if d.Unit == "" || !IsCanonicalMetricUnit(string(d.Unit)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing unit %q", d.Key(), d.Unit)
	}
	if d.TimingMode == "" || !IsCanonicalTimingMode(string(d.TimingMode)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing timing mode %q", d.Key(), d.TimingMode)
	}
	if d.Aggregation == "" || !IsCanonicalAggregationMode(string(d.Aggregation)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing aggregation %q", d.Key(), d.Aggregation)
	}
	if d.Cardinality == "" || !IsCanonicalCardinalityPolicy(string(d.Cardinality)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing cardinality %q", d.Key(), d.Cardinality)
	}
	if d.Owner == "" || !IsCanonicalComponentOwner(string(d.Owner)) {
		return fmt.Errorf("telemetry: descriptor %s has invalid or missing owner %q", d.Key(), d.Owner)
	}
	switch d.Kind {
	case KindCounter, KindGauge, KindHistogram:
		if d.Unit != UnitCount && d.Kind != KindGauge {
			return fmt.Errorf("telemetry: descriptor %s kind=%s must use count unit, got %s", d.Key(), d.Kind, d.Unit)
		}
		if d.TimingMode != TimingNone {
			return fmt.Errorf("telemetry: descriptor %s kind=%s must use timing none, got %s", d.Key(), d.Kind, d.TimingMode)
		}
	case KindDuration:
		if d.Unit != UnitMilliseconds {
			return fmt.Errorf("telemetry: descriptor %s kind=duration must use milliseconds unit, got %s", d.Key(), d.Unit)
		}
		switch d.TimingMode {
		case TimingExclusive:
			// Exclusive top-level phases must be attempt-scoped: only a
			// per_attempt fact is guaranteed non-overlapping with its
			// siblings. per_segment/per_track/per_asset/per_artifact/…
			// durations can overlap (parallel segments/transfers) and must be
			// span_child — this is the accounted_ratio double-count trap the
			// catalog's accounted_ratio_rule guards against.
			if d.Cardinality != CardPerAttempt {
				return fmt.Errorf("telemetry: descriptor %s exclusive timing requires per_attempt cardinality, got %s", d.Key(), d.Cardinality)
			}
		case TimingSpanChild:
			// Finer-grained parallelizable child span; never summed into
			// accounted_ratio.
		default:
			return fmt.Errorf("telemetry: descriptor %s kind=duration must use timing exclusive or span_child, got %s", d.Key(), d.TimingMode)
		}
	case KindSpan:
		if d.Unit != UnitMilliseconds {
			return fmt.Errorf("telemetry: descriptor %s kind=span must use milliseconds unit, got %s", d.Key(), d.Unit)
		}
		if d.TimingMode != TimingSpanParent && d.TimingMode != TimingSpanChild {
			return fmt.Errorf("telemetry: descriptor %s kind=span must use a span timing mode, got %s", d.Key(), d.TimingMode)
		}
	}
	return nil
}
