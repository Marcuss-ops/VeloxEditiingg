package taskattempts

import "time"

// ExecutionEventOrigin identifies the subsystem that emitted an execution
// event. The values are intentionally closed so SQL, validation, and
// dashboards share one stable vocabulary.
type ExecutionEventOrigin string

const (
	ExecutionEventOriginMaster     ExecutionEventOrigin = "master"
	ExecutionEventOriginWorker     ExecutionEventOrigin = "worker"
	ExecutionEventOriginEngine     ExecutionEventOrigin = "engine"
	ExecutionEventOriginFFmpeg     ExecutionEventOrigin = "ffmpeg"
	ExecutionEventOriginUpload     ExecutionEventOrigin = "upload"
	ExecutionEventOriginValidation ExecutionEventOrigin = "validation"
)

// IsValid reports whether the origin is part of the canonical closed set.
func (o ExecutionEventOrigin) IsValid() bool {
	switch o {
	case ExecutionEventOriginMaster,
		ExecutionEventOriginWorker,
		ExecutionEventOriginEngine,
		ExecutionEventOriginFFmpeg,
		ExecutionEventOriginUpload,
		ExecutionEventOriginValidation:
		return true
	default:
		return false
	}
}

// ExecutionEventScope identifies the object whose work is described by an
// event. The values mirror the CHECK constraint in task_execution_events.
type ExecutionEventScope string

const (
	ExecutionEventScopeJob           ExecutionEventScope = "job"
	ExecutionEventScopeTask          ExecutionEventScope = "task"
	ExecutionEventScopeAttempt       ExecutionEventScope = "attempt"
	ExecutionEventScopeSegment       ExecutionEventScope = "segment"
	ExecutionEventScopeAudioTrack    ExecutionEventScope = "audio_track"
	ExecutionEventScopeSubtitleTrack ExecutionEventScope = "subtitle_track"
	ExecutionEventScopeArtifact      ExecutionEventScope = "artifact"
)

// IsValid reports whether the scope is part of the canonical closed set.
func (s ExecutionEventScope) IsValid() bool {
	switch s {
	case ExecutionEventScopeJob,
		ExecutionEventScopeTask,
		ExecutionEventScopeAttempt,
		ExecutionEventScopeSegment,
		ExecutionEventScopeAudioTrack,
		ExecutionEventScopeSubtitleTrack,
		ExecutionEventScopeArtifact:
		return true
	default:
		return false
	}
}

// ExecutionEvent is the canonical domain representation of one immutable
// row in task_execution_events. Repeated operations are distinguished by
// EventID; component/action are descriptive dimensions, not identity keys.
type ExecutionEvent struct {
	ID               int64                `json:"id"`
	EventID          string               `json:"event_id"`
	AttemptID        string               `json:"attempt_id"`
	JobID            string               `json:"job_id"`
	TaskID           string               `json:"task_id"`
	WorkerID         string               `json:"worker_id"`
	WorkerSessionID  string               `json:"worker_session_id"`
	WorkerSnapshotID string               `json:"worker_snapshot_id"`
	LeaseID          string               `json:"lease_id"`
	ExecutorID       string               `json:"executor_id"`
	ExecutorVersion  int                  `json:"executor_version"`
	EventIndex       int64                `json:"event_index"`
	Origin           ExecutionEventOrigin `json:"origin"`
	Scope            ExecutionEventScope  `json:"scope"`
	EventType        string               `json:"event_type"`
	EventName        string               `json:"event_name"`
	Component        string               `json:"component"`
	Action           string               `json:"action"`
	Phase            string               `json:"phase"`
	Status           string               `json:"status"`
	ErrorCode        string               `json:"error_code,omitempty"`
	ErrorMessage     string               `json:"error_message,omitempty"`
	StartedAt        *time.Time           `json:"started_at,omitempty"`
	CompletedAt      *time.Time           `json:"completed_at,omitempty"`
	DurationMS       int64                `json:"duration_ms"`
	BytesIn          int64                `json:"bytes_in"`
	BytesOut         int64                `json:"bytes_out"`
	Frames           int64                `json:"frames"`
	MetadataJSON     string               `json:"metadata_json"`
	CreatedAt        time.Time            `json:"created_at"`
	SegmentIndex     *int                 `json:"segment_index,omitempty"`
	TrackKind        string               `json:"track_kind,omitempty"`
	TrackIndex       *int                 `json:"track_index,omitempty"`
	ArtifactID       string               `json:"artifact_id,omitempty"`
	StartedOffsetMS  float64              `json:"started_offset_ms"`
	FinishedOffsetMS float64              `json:"finished_offset_ms"`
	CPUMS            float64              `json:"cpu_ms"`
	QueueWaitMS      float64              `json:"queue_wait_ms"`
	FramesIn         int64                `json:"frames_in"`
	FramesOut        int64                `json:"frames_out"`
}

// Validate enforces the invariants that are independent of the database.
// Scope-specific requirements intentionally match the migration triggers.
func (e ExecutionEvent) Validate() error {
	if e.EventID == "" {
		return ErrInvalidExecutionEvent("event_id is required")
	}
	if e.AttemptID == "" {
		return ErrInvalidExecutionEvent("attempt_id is required")
	}
	if !e.Origin.IsValid() {
		return ErrInvalidExecutionEvent("origin is not canonical")
	}
	if !e.Scope.IsValid() {
		return ErrInvalidExecutionEvent("scope is not canonical")
	}
	if e.EventIndex < 0 {
		return ErrInvalidExecutionEvent("event_index must be non-negative")
	}
	if e.Scope == ExecutionEventScopeSegment && e.SegmentIndex == nil {
		return ErrInvalidExecutionEvent("segment scope requires segment_index")
	}
	if (e.Scope == ExecutionEventScopeAudioTrack || e.Scope == ExecutionEventScopeSubtitleTrack) && e.TrackIndex == nil {
		return ErrInvalidExecutionEvent("track scope requires track_index")
	}
	if e.Scope == ExecutionEventScopeArtifact && e.ArtifactID == "" {
		return ErrInvalidExecutionEvent("artifact scope requires artifact_id")
	}
	return nil
}

// InvalidExecutionEventError is returned when an event violates a domain
// invariant before it reaches SQL persistence.
type InvalidExecutionEventError string

func (e InvalidExecutionEventError) Error() string { return string(e) }

func ErrInvalidExecutionEvent(message string) error {
	return InvalidExecutionEventError("invalid execution event: " + message)
}
