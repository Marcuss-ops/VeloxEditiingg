// Package taskgraph defines the canonical Task domain model for distributed
// rendering. A Task is the unit of work assigned to a single worker execution.
//
// One Job owns exactly one Task (initial shape). Multiple tasks per job are
// out of scope for this PR.
package taskgraph

import "time"

// Task is the canonical domain model for a render task.
//
// Identity and executor fields are immutable after publication.
// Status changes only through LifecycleService with optimistic revision checks.
//
// PR #4: DependsOn holds the task IDs this task must wait for.
// Empty means no dependencies (single-task model).
//
// PR-2 / fix/canonical-attempt-identity: AttemptID + AttemptNumber are
// stamped on the tasks row at Claim time (the same tx that flips the
// status READY→LEASED AND inserts the matching PENDING TaskAttempt).
// Pre-PR-2 these columns were NULL / 0; post-Claim they're populated
// for every newly-leased task. CAS-keyed queries (RenewLease,
// AcceptTaskAtomic-replay, etc.) read attempt_id from the tasks row,
// closing the previous dependency on a pre-existing attempt row to
// supply the canonical identity.
type Task struct {
	ID              string     `json:"id"`
	JobID           string     `json:"job_id"`
	ProjectID       string     `json:"project_id,omitempty"`
	RenderPlanID    string     `json:"render_plan_id,omitempty"`
	ExecutorID      string     `json:"executor_id,omitempty"`
	ExecutorVersion int        `json:"executor_version"`
	Status          Status     `json:"status"`
	Priority        int        `json:"priority"`
	Revision        int        `json:"revision"`
	AttemptCount    int        `json:"attempt_count"`
	AttemptID       string     `json:"attempt_id,omitempty"`
	AttemptNumber   int        `json:"attempt_number"`
	WorkerID        string     `json:"worker_id,omitempty"`
	LeaseID         string     `json:"lease_id,omitempty"`
	DependsOn       []string   `json:"depends_on,omitempty"`
	ReadyAt         *time.Time `json:"ready_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// TaskWithSpec is a Task with its canonical TaskSpec payload resolved
// from the task_specs table. Used by the task-native dispatch path (PR #4).
type TaskWithSpec struct {
	Task
	SpecPayload map[string]interface{} `json:"spec_payload,omitempty"`
}
