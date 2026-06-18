// Package workflow — domain types for the Workflow v2 runtime (PR 8 + PR 9).
//
// The package owns the structured representation of a multi-step pipeline.
// Persistence lives in sqlite_repository.go; transitions live in repository.go.
// Handlers that bridge outbox events into workflow.Run transitions live in
// internal/handlers/outbox (registered with the outbox.Registry at startup).
package workflow

import "time"

// RunStatus is the lifecycle of a workflow_runs row.
//
//	PENDING     : created, not yet RUNNING
//	RUNNING     : first step dispatched
//	SUCCEEDED   : all terminal steps succeeded
//	FAILED      : at least one step failed with no retries remaining
//	CANCELLED   : operator cancelled
type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusSucceeded RunStatus = "SUCCEEDED"
	RunStatusFailed    RunStatus = "FAILED"
	RunStatusCancelled RunStatus = "CANCELLED"
)

// StepStatus is the lifecycle of a workflow_steps row.
//
//	BLOCKED : at least one predecessor step is not yet SUCCEEDED
//	READY   : all predecessors succeeded, may be dispatched
//	RUNNING : a job has been created (workflow_steps.job_id != "") and is in flight
//	SUCCEEDED : the job succeeded and the step produced its output
//	FAILED : the job exhausted retries; the step is terminal-failed
type StepStatus string

const (
	StepStatusBlocked   StepStatus = "BLOCKED"
	StepStatusReady     StepStatus = "READY"
	StepStatusRunning   StepStatus = "RUNNING"
	StepStatusSucceeded StepStatus = "SUCCEEDED"
	StepStatusFailed    StepStatus = "FAILED"
)

// Run is the canonical projection of a workflow_runs row.
type Run struct {
	RunID        string
	WorkflowType string
	Status       RunStatus

	Input    map[string]any
	Output   map[string]any
	Revision int64

	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time

	LastErrorCode    string
	LastErrorMessage string
}

// Step is the canonical projection of a workflow_steps row.
type Step struct {
	StepID  string
	RunID   string
	StepKey string
	JobID   *string

	Status      StepStatus
	Attempt     int
	MaxAttempts int

	Input    map[string]any
	Output   map[string]any
	Revision int64

	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time

	ErrorCode    string
	ErrorMessage string
}

// RunProgress is what CompleteStepAndReleaseDependents returns so callers
// (CLI/UI) can introspect DAG state without re-querying per-step.
type RunProgress struct {
	Run        Run
	Steps      []Step
	Activated  []string // step_keys that just flipped BLOCKED → READY in this transaction
	Completed  bool     // true if Run now reaches SUCCEEDED
}

// WorkflowSpec is what a producer hands to Repository.CreateRun.
//
// Steps must be in topological order (a step may only depend on prior steps
// in the slice); the Repository re-validates before insert.
type WorkflowSpec struct {
	RunID        string
	WorkflowType string
	Input        map[string]any
	Steps        []WorkflowStepSpec
}

// WorkflowStepSpec is one element of WorkflowSpec.Steps.
type WorkflowStepSpec struct {
	StepKey       string
	JobType       string         // payload["job_type"] when dispatched
	Input         map[string]any // payload yielded into the worker submission
	DependsOnKeys []string       // step_keys of predecessor steps
	MaxAttempts   int            // 0 → configurable default
}

// StartStep is the input for Repository.MarkStepRunning.
type StartStep struct {
	RunID    string
	StepID   string
	JobID    string // the job created by the workflowStepReadyHandler
	Attempt  int    // 1-based attempt number
}

// CompleteStep is the input for Repository.CompleteStepAndReleaseDependents.
type CompleteStep struct {
	RunID        string
	StepID       string
	Output       map[string]any
	Attempt      int
	CompletedAt  time.Time
}

// FailStep is the input for Repository.FailStep.
type FailStep struct {
	RunID        string
	StepID       string
	ErrorCode    string
	ErrorMessage string
	Attempt      int
	Requeue      bool // true → step flips back to READY for re-dispatch (retry)
}
