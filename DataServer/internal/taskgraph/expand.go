package taskgraph

import (
	"context"
	"fmt"
)

// ExpandService is the fan-out enqueue service (100-percent-plan/04 §1-2):
// it expands one Job into a validated set of PENDING Tasks with persisted
// dependency edges, atomically and fail-closed.
//
// The graph gate runs BEFORE any write: a plan with dangling edges or a
// dependency cycle is rejected in full (no partial fan-out is ever
// persisted). Every task is created in PENDING with its edge list;
// TickReadiness owns the PENDING→READY transition once dependencies
// succeed, so a fan-out job starts executing at its DAG roots while the
// dependent tasks stay inert.
//
// Deliberately NOT part of this type: spec payload construction and
// executor selection. The caller (enqueue path) decides what each task
// executes; this service only guarantees graph integrity + atomic
// persistence of the task set.
type ExpandService struct {
	repo Repository
}

// NewExpandService constructs the fan-out service over the canonical
// task repository.
func NewExpandService(repo Repository) (*ExpandService, error) {
	if repo == nil {
		return nil, fmt.Errorf("taskgraph.NewExpandService: repository is required")
	}
	return &ExpandService{repo: repo}, nil
}

// ExpandedTask is one task of a fan-out plan before persistence. TaskID may
// be empty: the repository assigns one at Create time and the plan is
// rewritten in place so the caller observes the persisted identity.
type ExpandedTask struct {
	TaskID       string
	ExecutorID   string
	ExecutorVer  int
	Priority     int
	ProjectID    string
	RenderPlanID string
	DependsOn    []string
}

// ExpandPlan is the input fan-out plan for one job.
type ExpandPlan struct {
	JobID string
	Tasks []ExpandedTask
}

// Expand validates the plan as a graph and persists every task in PENDING
// with its edges in one pass. SQLiteTaskRepository implements the optional
// AtomicTaskWriter capability, so production persistence uses one database
// transaction and readers never observe a partial graph. Lightweight test
// repositories retain the compensating fallback below.
//
// Returns the persisted tasks (IDs filled in, input order preserved).
func (s *ExpandService) Expand(ctx context.Context, plan ExpandPlan) ([]Task, error) {
	if plan.JobID == "" {
		return nil, fmt.Errorf("taskgraph.Expand: job_id is required")
	}
	if len(plan.Tasks) == 0 {
		return nil, fmt.Errorf("taskgraph.Expand: plan must contain at least one task")
	}

	// Assign identity up front so the graph gate validates the REAL edges
	// (callers may reference tasks by their assigned IDs).
	tasks := make([]Task, 0, len(plan.Tasks))
	assigned := make([]string, len(plan.Tasks))
	for i := range plan.Tasks {
		t := plan.Tasks[i]
		id := t.TaskID
		if id == "" {
			assigned[i] = fmt.Sprintf("%s-t%02d", plan.JobID, i+1)
		} else {
			assigned[i] = id
		}
	}
	for i := range plan.Tasks {
		t := plan.Tasks[i]
		// Rewrite edges: they are already task IDs in the plan contract —
		// validation below is the dangling-edge authority.
		tasks = append(tasks, Task{
			ID:              assigned[i],
			JobID:           plan.JobID,
			ProjectID:       t.ProjectID,
			RenderPlanID:    t.RenderPlanID,
			ExecutorID:      t.ExecutorID,
			ExecutorVersion: t.ExecutorVer,
			Status:          StatusPending,
			Priority:        t.Priority,
			DependsOn:       t.DependsOn,
		})
	}

	// Fail-closed graph gate BEFORE any persistence.
	if err := ValidateTaskGraph(tasks); err != nil {
		return nil, err
	}

	if atomicRepo, ok := s.repo.(AtomicTaskWriter); ok {
		if err := atomicRepo.CreateTasksAtomic(ctx, tasks); err != nil {
			return nil, fmt.Errorf("taskgraph.Expand: atomic create: %w", err)
		}
		return tasks, nil
	}

	created := make([]Task, 0, len(tasks))
	for i := range tasks {
		t := tasks[i]
		if err := s.repo.Create(ctx, &t); err != nil {
			// Compensate: remove the rows created in this pass so a failed
			// expansion never leaves a partial DAG behind (the parent job
			// remains PENDING and can be re-expanded or failed upstream).
			for j := range created {
				_ = s.repo.Delete(ctx, created[j].ID)
			}
			return nil, fmt.Errorf("taskgraph.Expand: create task %s: %w", t.ID, err)
		}
		created = append(created, t)
	}
	return created, nil
}
