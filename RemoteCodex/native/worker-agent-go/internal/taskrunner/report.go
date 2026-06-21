package taskrunner

import (
	"time"

	"velox-worker-agent/internal/executor"
)

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
	JobID        string                 `json:"job_id"`
	ExecutorID   string                 `json:"executor_id"`
	ExecutorKey  string                 `json:"executor_key"` // canonical "id@version"
	Status       string                 `json:"status"`
	ErrorCode    string                 `json:"error_code,omitempty"`
	ErrorDetail  string                 `json:"error_detail,omitempty"`
	Outputs      []executor.ArtifactRef `json:"outputs,omitempty"`
	Metrics      map[string]interface{} `json:"metrics,omitempty"`
	Attempts     int                    `json:"attempts"`
	StartedAt    time.Time              `json:"started_at"`
	CompletedAt  time.Time              `json:"completed_at"`
	PhaseMarkers []PhaseMarker          `json:"phase_markers,omitempty"`
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

// Succeeded returns true when the report reflects an executed-succeeded
// task. Helper for tests, alerting, and downstream branches.
func (r TaskExecutionReport) Succeeded() bool { return r.Status == "succeeded" }

// PhaseCount returns the number of PhaseMarkers recorded. Useful for
// invariants and tests.
func (r TaskExecutionReport) PhaseCount() int { return len(r.PhaseMarkers) }
