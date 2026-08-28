package telemetry

// phase_types.go holds the type definitions, constants, and pure helpers
// shared by the EventRecorder, EventHandle, and C++ import files.
// EventRecorder remains the single truth source; these types are its
// vocabulary.

import (
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

const (
	// StatusOK / StatusFailed are aliases to the shared canonical status
	// vocabulary (velox-shared/telemetry) — this package owns no second
	// literal list.
	StatusOK     = sharedtelemetry.StatusOK
	StatusFailed = sharedtelemetry.StatusFailed

	// MaxAttemptEvents bounds the in-memory attempt journal. Once full,
	// new observations are dropped and counted on the final snapshot. This
	// explicit best-effort policy prevents pathological attempts from growing
	// memory without limit.
	MaxAttemptEvents = 4096
)

// RecordedPhase is one immutable execution event stored in an EventRecorder.
type RecordedPhase struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	SchemaVersion    int32
	EventIndex       int64
	ArtifactID       string
	StartedAt        time.Time
	CompletedAt      time.Time
	DurationMS       int64
	Status           string
	ErrorCode        string
	ErrorMessage     string
	BytesIn          int64
	BytesOut         int64
	Frames           int64
	MetadataJSON     string
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

// EventSpec describes an event. Origin, scope, and component/action should be
// registered in phase_registry.go. Invalid specs are retained and sent to the
// master for quarantine rather than being silently dropped at the worker.
type EventSpec struct {
	Origin           string
	Scope            string
	Component        string
	Action           string
	Phase            string
	EventType        string
	EventName        string
	SchemaVersion    int32
	ArtifactID       string
	MetadataJSON     string
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

type eventIdentity struct {
	origin string
	index  int64
}

// ── Pure helpers ──────────────────────────────────────────────────────────

func eventTypeFor(explicit, status string) string {
	if explicit != "" {
		return explicit
	}
	if status == StatusFailed {
		return sharedtelemetry.EventTypeFailed
	}
	return sharedtelemetry.EventTypeCompleted
}

func normalizeEventSpec(spec *EventSpec) bool {
	return spec != nil && CanonicalizeEventSpec(spec)
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

// ── JobPhaseTimer vocabulary ──────────────────────────────────────────────

// PhaseTiming accumulates timing and data volume for one fine-grained phase.
type PhaseTiming struct {
	Duration    time.Duration
	Count       int64
	BytesIn     int64
	BytesOut    int64
	FramesIn    int64
	FramesOut   int64
	CPUMs       float64
	QueueWaitMs float64
	Errors      int64
}

// Add merges src into this timing.
func (t *PhaseTiming) Add(src PhaseTiming) {
	t.Duration += src.Duration
	t.Count += src.Count
	t.BytesIn += src.BytesIn
	t.BytesOut += src.BytesOut
	t.FramesIn += src.FramesIn
	t.FramesOut += src.FramesOut
	t.CPUMs += src.CPUMs
	t.QueueWaitMs += src.QueueWaitMs
	t.Errors += src.Errors
}

// DurationMs returns the duration in milliseconds.
func (t PhaseTiming) DurationMs() int64 {
	return t.Duration.Milliseconds()
}

// ScenePhaseTiming is per-scene timing data.
type ScenePhaseTiming struct {
	SceneID          string
	SourceDurationMs int64
	OutputDurationMs int64
	Phases           map[string]PhaseTiming
	InputBytes       int64
	OutputBytes      int64
	FramesDecoded    int64
	FramesEncoded    int64
	FPS              float64
}

// TotalMs returns the sum of all phase durations for this scene.
func (s ScenePhaseTiming) TotalMs() int64 {
	var total time.Duration
	for _, p := range s.Phases {
		total += p.Duration
	}
	return total.Milliseconds()
}

// RenderSpeed returns the render speed multiplier (media duration / processing time).
func (s ScenePhaseTiming) RenderSpeed() float64 {
	total := s.TotalMs()
	if total <= 0 {
		return 0
	}
	return float64(s.OutputDurationMs) / float64(total)
}

// PhaseTimingWithName pairs a phase name with its timing.
type PhaseTimingWithName struct {
	Name   string
	Timing PhaseTiming
}

// SceneTimingWithName pairs a scene ID with its timing.
type SceneTimingWithName struct {
	SceneID string
	Timing  ScenePhaseTiming
}

// ── GPU transfer metrics ──────────────────────────────────────────────────

// GPUTransferMetrics tracks VRAM ↔ RAM data movement.
type GPUTransferMetrics struct {
	GPUToCPUMs          int64 `json:"gpu_to_cpu_transfer_ms"`
	CPUToGPUMs          int64 `json:"cpu_to_gpu_transfer_ms"`
	GPUToCPUBytes       int64 `json:"gpu_to_cpu_bytes"`
	CPUToGPUBytes       int64 `json:"cpu_to_gpu_bytes"`
	FramesDownloadedGPU int64 `json:"frames_downloaded_from_gpu"`
	FramesUploadedGPU   int64 `json:"frames_uploaded_to_gpu"`
}

// Add merges src into these metrics.
func (g *GPUTransferMetrics) Add(src GPUTransferMetrics) {
	g.GPUToCPUMs += src.GPUToCPUMs
	g.CPUToGPUMs += src.CPUToGPUMs
	g.GPUToCPUBytes += src.GPUToCPUBytes
	g.CPUToGPUBytes += src.CPUToGPUBytes
	g.FramesDownloadedGPU += src.FramesDownloadedGPU
	g.FramesUploadedGPU += src.FramesUploadedGPU
}
