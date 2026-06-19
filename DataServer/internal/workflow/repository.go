// Package workflow/repository — Repository interface per PR 9 spec:
//
//	type Repository interface {
//	    CreateRun(ctx, spec WorkflowSpec) (*Run, error)
//	    GetRun(ctx, runID string) (*Run, error)
//	    ListSteps(ctx, runID string) ([]Step, error)
//	    MarkStepRunning(ctx, cmd StartStep) error
//	    CompleteStepAndReleaseDependents(ctx, cmd CompleteStep) (*RunProgress, error)
//	    FailStep(ctx, cmd FailStep) (*RunProgress, error)
//	    CancelRun(ctx, runID string) error
//	}
//
// Plus the helpers needed by the cutover wire-up: ListRuns, GetStepByJobID,
// Stats. These are not in PR 9 §"Implementation §1" but are referenced by
// the legacy /api/v1/orchestrator/* endpoints that survive the cutover
// (treated as read-only adapters per PR 9 §Cutover).
package workflow

import "context"

// Repository is the algebraic surface of the Workflow v2 persistence layer.
//
// All methods are safe for concurrent callers; SQLite writes are serialised
// by the storage engine, but the Repository takes care to start a transaction
// for any operation that mutates more than one row.
//
// Application code receives a Repository from cmd/server bootstrap and uses
// it directly — no caching or read-through layer is required because there
// is no in-memory authoritative state (PR 9 §Legacy eliminato).
type Repository interface {
	// CreateRun inserts a new workflow run with all its steps and
	// dependency rows in a single transaction. The initial status of
	// every step is derived from its predecessors:
	//   * no deps                 → READY
	//   * any predecessor missing → BLOCKED
	// The run itself is set to PENDING.
	CreateRun(ctx context.Context, spec WorkflowSpec) (*Run, error)

	// GetRun returns the run + revision metadata by run_id.
	// Returns (nil, nil) when the run does not exist.
	GetRun(ctx context.Context, runID string) (*Run, error)

	// ListSteps returns the run's steps in stable (creation) order.
	ListSteps(ctx context.Context, runID string) ([]Step, error)

	// MarkStepRunning flips the given step from READY → RUNNING and
	// stamps workflow_steps.job_id with the externally-created job_id.
	// Idempotent: re-running on a step already RUNNING is a no-op (but the
	// job_id is re-stamped — useful when the worker reports a reassignment).
	MarkStepRunning(ctx context.Context, cmd StartStep) error

	// CompleteStepAndReleaseDependents atomically:
	//   * RUNNING → SUCCEEDED on the target step (stamping output,
	//     completed_at, attempt_count, revision++)
	//   * for each step_id that depends on the target:
	//     - if all of its predecessors are SUCCEEDED → BLOCKED → READY
	//   * when every step is SUCCEEDED, the run itself flips to SUCCEEDED
	//     with completed_at stamped.
	// It emits workflow_events rows for each transition and outbox_events
	// rows for WORKFLOW_STEP_SUCCEEDED and (for each activated step)
	// WORKFLOW_STEP_READY, and WORKFLOW_RUN_SUCCEEDED when applicable.
	// Returns a RunProgress summary of the change.
	CompleteStepAndReleaseDependents(ctx context.Context, cmd CompleteStep) (*RunProgress, error)

	// FailStep flips a RUNNING step to FAILED or READY (depending on
	// FailStep.Requeue and the per-step max_attempts). When the step
	// reaches FAILED with no retries remaining, the run itself is
	// marked FAILED with last_error_code/message stamped.
	FailStep(ctx context.Context, cmd FailStep) (*RunProgress, error)

	// CancelRun marks the run CANCELLED. Any READY/BLOCKED/RUNNING step
	// is also marked CANCELLED so re-clocked dispatcher cycles do not
	// resurrect work. RUNNING steps emit CANCEL_REQUESTED workflow_events
	// so observers can tell signal-vs-state.
	CancelRun(ctx context.Context, runID string) error

	// ListRuns paginates runs (newest updated_at first) for the legacy
	// /api/v1/orchestrator/jobs read-only adapter. limit<=0 means default.
	ListRuns(ctx context.Context, limit int) ([]Run, error)

	// GetStepByJobID is the inverse lookup used by the JOB_SUCCEEDED
	// outbox consumer: the handler reads the aggregate_id from the outbox
	// row and asks the Repository "which workflow step owns this job_id?".
	// Returns (*nil, "", nil) if no
	// step claims the job_id (orphan — submitted outside any workflow
	// run, e.g. direct POST /orchestrator/jobs shaped submission backed
	// by CreateRun but with a manually-composed job_id).
	GetStepByJobID(ctx context.Context, jobID string) (*Step, string, error)

	// Stats returns aggregate counts per run-status + step-status for the
	// legacy /api/v1/orchestrator/stats adapter.
	Stats(ctx context.Context) (StatsReport, error)
}

// StatsReport is the dashboard projection returned by Repository.Stats.
type StatsReport struct {
	TotalRuns     int
	RunsByStatus  map[RunStatus]int
	TotalSteps    int
	StepsByStatus map[StepStatus]int
}

// OutboxWriter is the minimal interface the Repository uses to emit
// outbox_events. Provided by the bootstrap wiring against the outbox
// package; nil disables emission (still emits workflow_events locally).
type OutboxWriter interface {
	Enqueue(ctx context.Context, ev WorkflowOutboxEvent) error
}

// WorkflowOutboxEvent is the shape of an outbox event the workflow
// Repository publishes. outbox.Insert maps it to a concrete outbox row.
type WorkflowOutboxEvent struct {
	AggregateID string
	EventType   string
	Payload     []byte
}
