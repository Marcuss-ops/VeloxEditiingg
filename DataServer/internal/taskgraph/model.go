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
	WorkerID        string     `json:"worker_id,omitempty"`
	LeaseID         string     `json:"lease_id,omitempty"`
	ReadyAt         *time.Time `json:"ready_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}
