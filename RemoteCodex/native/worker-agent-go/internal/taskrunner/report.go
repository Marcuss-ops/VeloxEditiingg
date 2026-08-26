package taskrunner

import (
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

// SegmentTiming re-exports executor.SegmentTiming so consumers of the
// taskrunner package can build segment lists without importing executor.
type SegmentTiming = executor.SegmentTiming

// TaskExecutionReport is the canonical per-task report the TaskRunner
// emits on every Run call.
//
// PR-3.3 invariants:
//   - Exactly ONE report exists per Run call (never omitted, even if Run
//     itself panics or the runner is fatally misconfigured).
//   - Status is exactly "succeeded" or "failed". Free-form strings are
//     treated as "failed" downstream.
//   - ErrorCode is empty on success; otherwise one of the Code* constants
//     in errors.go. Free-form strings here are a runner bug.
//   - PhaseMarkers contains at most one entry per canonical phase
//     (PhaseCacheLookup, PhasePrefetch, PhaseExecute, PhaseUpload,
//     PhaseReport), in that order. Empty phases are not emitted.
type TaskExecutionReport struct {
	JobID       string                 `json:"job_id"`
	ExecutorID  string                 `json:"executor_id"`
	ExecutorKey string                 `json:"executor_key"` // canonical "id@version"
	Status      string                 `json:"status"`
	ErrorCode   string                 `json:"error_code,omitempty"`
	ErrorDetail string                 `json:"error_detail,omitempty"`
	Outputs     []executor.ArtifactRef `json:"outputs,omitempty"`
	// Metrics is a display-only projection map. It carries executor
	// projection keys (pipeline.*, native.*, render_profile.*) and
	// provider display facts (cache.*, blob.*, ffmpeg.aggregate) for
	// dashboard compatibility. RawMetrics is the canonical typed envelope.
	Metrics  map[string]interface{}   `json:"metrics,omitempty"`
	Segments []executor.SegmentTiming `json:"segments,omitempty"`
	// RawMetrics is the canonical typed raw metric envelope. Migrated
	// producers write this value directly; no map round-trip is involved.
	RawMetrics *telemetry.RawExecutionMetrics `json:"-"`
	// TypedMetrics is the source-compatible wire alias retained during the
	// migration window. It points at the same raw envelope and is used by
	// the protobuf builder. New code must prefer RawMetrics.
	TypedMetrics *telemetry.TypedExecutionMetrics `json:"typed_metrics,omitempty"`
	Attempts     int                              `json:"attempts"`
	StartedAt    time.Time                        `json:"started_at"`
	CompletedAt  time.Time                        `json:"completed_at"`
	PhaseMarkers []PhaseMarker                    `json:"phase_markers,omitempty"`
	// DetailedPhases is the full, ordered, event-taxonomy phase list for
	// the attempt, snapshotted from the append-only recorder at Run completion.
	// Serialized to TaskResult.phase_timings (proto field 20); the
	// master ingests the rows into task_execution_events and derives
	// task_phase_timings PARTIAL/FAILED summaries from them. Legacy
	// masters (pre-block-1) ignore the field entirely.
	DetailedPhases []DetailedPhaseTiming `json:"detailed_phases,omitempty"`
	// AttemptRecorder remains attached until the outer worker lifecycle has
	// finished upload/commit. It is transport-local state and never serialized.
	AttemptRecorder       *telemetry.EventRecorder       `json:"-"`
	AttemptRecorderOffset int                            `json:"-"`
	AttemptEvents         *telemetry.AttemptEventMachine `json:"-"`
	// CacheBaseline stores provider counters at attempt start so the report
	// carries per-attempt deltas rather than worker lifetime totals.
	CacheBaseline    map[string]int64 `json:"-"`
	CacheBaselineSet bool             `json:"-"`
	// FFmpegProfiles is the attempt-scoped ffmpeg profile accumulator
	// (B2). Executors push every FFmpegResult into it; mergeStatsInto
	// stamps the JSON-safe aggregate into Metrics as "ffmpeg.aggregate"
	// on every outcome. Transport-local, never serialized directly.
	FFmpegProfiles *ffmpegrunner.Aggregator `json:"-"`
}

// DetailedPhaseTiming is the worker-side mirror of
// pb.PhaseTimingDetailed (proto fields 1–24): a single execution event
// with closed origin/scope enums, a per-origin monotonic event_index,
// and identity fields. The transport boundary (submitTaskResult)
// converts each entry via ToProto.
//
// Clock contract: DurationMS comes from a monotonic clock; StartedAt /
// CompletedAt are UTC wall stamps for cross-host correlation only.
type DetailedPhaseTiming struct {
	PhaseOrder   int
	Component    string
	Action       string
	StartedAt    time.Time
	CompletedAt  time.Time
	DurationMS   int64
	Status       string
	ErrorCode    string
	ErrorMessage string
	BytesIn      int64
	BytesOut     int64
	Frames       int64
	MetadataJSON string
	// ── Observability chain / block 1: event taxonomy ────────────────
	Origin string
	Scope  string
	// TelemetrySchemaVersion identifies the shared event catalog used by
	// this phase. It is zero only for legacy in-process callers.
	TelemetrySchemaVersion int32
	// SchemaVersion preserves a producer-invalid version for master
	// quarantine. Valid events use TelemetrySchemaVersion on the wire.
	SchemaVersion int32
	EventType     string
	EventName     string
	EventIndex    int64
	Phase         string
	// ── Identity (master overrides at ingest) ────────────────────────
	ExecutorID       string
	ExecutorVersion  int32
	LeaseID          string
	SegmentIndex     int32
	TrackKind        string
	TrackIndex       int32
	StartedOffsetMS  float64
	FinishedOffsetMS float64
	CPUMS            float64
	QueueWaitMS      float64
	FramesIn         int64
	FramesOut        int64
}

func fromRecordedPhase(p telemetry.RecordedPhase, phaseOrder int, executorID string, executorVersion int32, leaseID string) DetailedPhaseTiming {
	return DetailedPhaseTiming{
		PhaseOrder:             phaseOrder,
		Component:              p.Component,
		Action:                 p.Action,
		StartedAt:              p.StartedAt,
		CompletedAt:            p.CompletedAt,
		DurationMS:             p.DurationMS,
		Status:                 p.Status,
		ErrorCode:              p.ErrorCode,
		ErrorMessage:           p.ErrorMessage,
		BytesIn:                p.BytesIn,
		BytesOut:               p.BytesOut,
		Frames:                 p.Frames,
		MetadataJSON:           p.MetadataJSON,
		Origin:                 p.Origin,
		Scope:                  p.Scope,
		TelemetrySchemaVersion: p.SchemaVersion,
		SchemaVersion:          p.SchemaVersion,
		EventType:              p.EventType,
		EventName:              p.EventName,
		EventIndex:             p.EventIndex,
		Phase:                  p.Phase,
		ExecutorID:             executorID,
		ExecutorVersion:        executorVersion,
		LeaseID:                leaseID,
		SegmentIndex:           p.SegmentIndex,
		TrackKind:              p.TrackKind,
		TrackIndex:             p.TrackIndex,
		StartedOffsetMS:        p.StartedOffsetMS,
		FinishedOffsetMS:       p.FinishedOffsetMS,
		CPUMS:                  p.CPUMS,
		QueueWaitMS:            p.QueueWaitMS,
		FramesIn:               p.FramesIn,
		FramesOut:              p.FramesOut,
	}
}

// PhaseMarker records one canonical phase's timing and outcome. Status
// is one of "ok", "failed", or "skipped" (only documented here; the
// runner currently only emits "ok" and "failed"). Notes carries the
// phase error short-form for downstream graphing.
type PhaseMarker struct {
	Name        string    `json:"name"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Status      string    `json:"status"`
	Notes       string    `json:"notes,omitempty"`
}

// RenderDuration returns the canonical execution-phase duration. It is a
// report lifecycle fact, not a legacy metric-map lookup. The boolean is false
// when no execute marker was recorded, allowing callers to preserve their
// wall-clock fallback for validation/early-failure paths.
func (r TaskExecutionReport) RenderDuration() (time.Duration, bool) {
	for i := len(r.PhaseMarkers) - 1; i >= 0; i-- {
		marker := r.PhaseMarkers[i]
		if marker.Name != PhaseExecute || marker.Status == "deferred" {
			continue
		}
		if marker.CompletedAt.Before(marker.StartedAt) {
			return 0, false
		}
		return marker.CompletedAt.Sub(marker.StartedAt), true
	}
	return 0, false
}

// Succeeded returns true when the report reflects an executed-succeeded
// task. Helper for tests, alerting, and downstream branches.
func (r TaskExecutionReport) Succeeded() bool { return r.Status == "succeeded" }

// PhaseCount returns the number of PhaseMarkers recorded. Useful for
// invariants and tests.
func (r TaskExecutionReport) PhaseCount() int { return len(r.PhaseMarkers) }
