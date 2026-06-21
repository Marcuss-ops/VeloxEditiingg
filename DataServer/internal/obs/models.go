// Package obs provides observability models for the DataServer.
//
// These types describe task execution, attempts, and phase timings
// without changing rendering behavior. They are intended for
// structured logging, metrics emission, and future pipeline
// introspection features.
package obs

import "time"

// AttemptStatus is a typed constant for TaskAttempt.Status.
type AttemptStatus string

const (
	AttemptPending    AttemptStatus = "PENDING"
	AttemptRunning    AttemptStatus = "RUNNING"
	AttemptSucceeded  AttemptStatus = "SUCCEEDED"
	AttemptFailed     AttemptStatus = "FAILED"
	AttemptCancelled  AttemptStatus = "CANCELLED"
)

// TaskSpec describes the specification of a unit of work within the
// rendering or delivery pipeline. It is intentionally decoupled from
// the jobs package so observability consumers do not need to depend
// on the full job domain.
type TaskSpec struct {
	// TaskID is a stable identifier for this task spec (e.g. "render-video",
	// "upload-youtube", "transcribe-audio").
	TaskID string `json:"task_id"`

	// TaskType categorizes the task for aggregation dashboards.
	// Examples: "render", "upload", "deliver", "transcode".
	TaskType string `json:"task_type"`

	// JobID links this task to the parent job.
	JobID string `json:"job_id"`

	// WorkerID identifies the worker assigned to this task, if any.
	WorkerID string `json:"worker_id,omitempty"`

	// Parameters are the immutable inputs that define this task.
	// Stored as a JSON blob; consumers should treat this as opaque.
	Parameters string `json:"parameters,omitempty"`

	// Dependencies lists TaskIDs that must complete before this task
	// can start. Empty means no dependencies.
	Dependencies []string `json:"dependencies,omitempty"`

	// CreatedAt is when this spec was first recorded.
	CreatedAt time.Time `json:"created_at"`
}

// TaskAttempt records one execution attempt of a TaskSpec.
// A single TaskSpec may have many TaskAttempts (retries).
type TaskAttempt struct {
	// AttemptID is a unique identifier for this attempt (UUID v4).
	AttemptID string `json:"attempt_id"`

	// TaskID links back to the parent TaskSpec.
	TaskID string `json:"task_id"`

	// JobID links back to the parent job.
	JobID string `json:"job_id"`

	// WorkerID identifies the worker that executed (or is executing)
	// this attempt.
	WorkerID string `json:"worker_id,omitempty"`

	// Status is the terminal or in-flight status of this attempt.
	Status AttemptStatus `json:"status"`

	// ErrorCode is a machine-readable error classification for failed
	// attempts (e.g. "WORKER_TIMEOUT", "RENDER_CRASH").
	ErrorCode string `json:"error_code,omitempty"`

	// ErrorMessage is a human-readable description of the failure.
	ErrorMessage string `json:"error_message,omitempty"`

	// StartedAt is when the attempt transitioned to RUNNING.
	StartedAt *time.Time `json:"started_at,omitempty"`

	// FinishedAt is when the attempt reached a terminal state.
	FinishedAt *time.Time `json:"finished_at,omitempty"`

	// LeaseID is the lease token held by the worker for this attempt.
	LeaseID string `json:"lease_id,omitempty"`

	// Revision is the optimistic-locking version for CAS updates.
	Revision int `json:"revision"`
}

// PhaseTiming records the wall-clock duration of a named phase within
// a TaskAttempt. Phases are user-defined buckets (e.g. "download_assets",
// "render_scene", "encode_video") that add up to the total attempt
// duration.
type PhaseTiming struct {
	// AttemptID links this phase to a TaskAttempt.
	AttemptID string `json:"attempt_id"`

	// TaskID links this phase to a TaskSpec.
	TaskID string `json:"task_id"`

	// JobID links this phase to a job.
	JobID string `json:"job_id"`

	// Phase is the human-readable phase name (e.g. "render_scene").
	Phase string `json:"phase"`

	// StartedAt is when the phase began.
	StartedAt time.Time `json:"started_at"`

	// FinishedAt is when the phase ended.
	FinishedAt time.Time `json:"finished_at"`

	// DurationMs is the computed wall-clock duration in milliseconds.
	// Derived from FinishedAt - StartedAt. Pre-computed so that
	// log/metrics consumers do not need to calculate it themselves.
	DurationMs int64 `json:"duration_ms"`
}

// Finish finalises a PhaseTiming by setting FinishedAt and computing
// DurationMs. It is safe to call on a zero-value PhaseTiming.
func (p *PhaseTiming) Finish(end time.Time) {
	p.FinishedAt = end
	p.DurationMs = end.Sub(p.StartedAt).Milliseconds()
}
