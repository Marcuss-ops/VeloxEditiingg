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
// Each expanded task carries its worker payload so the production atomic
// writer persists task rows and executable specs together.
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
	Payload      map[string]interface{}
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
			SpecPayload:     t.Payload,
		})
	}

	// Fail-closed graph gate BEFORE any persistence.
	if err := ValidateTaskGraph(tasks); err != nil {
		return nil, err
	}

	atomicRepo, ok := s.repo.(AtomicTaskWriter)
	if !ok {
		return nil, fmt.Errorf("taskgraph.Expand: repository must atomically persist task rows and executable specs")
	}
	if err := atomicRepo.CreateTasksAtomic(ctx, tasks); err != nil {
		return nil, fmt.Errorf("taskgraph.Expand: atomic create: %w", err)
	}
	return tasks, nil
}

// Extend atomically adds preparation roots and connects them to an existing
// unleased assembly task. The complete job graph is validated before any
// persistence; the assembly TaskSpec patch is committed with rows and edges.
func (s *ExpandService) Extend(ctx context.Context, parentTaskID string, additions []ExpandedTask, parentDependsOn []string, parentPayloadPatch map[string]interface{}) ([]Task, error) {
	if parentTaskID == "" || len(additions) == 0 {
		return nil, fmt.Errorf("taskgraph.Extend: parent task and preparation tasks are required")
	}
	parent, err := s.repo.Get(ctx, parentTaskID)
	if err != nil {
		return nil, fmt.Errorf("taskgraph.Extend: read parent: %w", err)
	}
	if parent == nil || (parent.Status != StatusPending && parent.Status != StatusReady) || parent.WorkerID != "" {
		return nil, fmt.Errorf("taskgraph.Extend: parent must exist and remain unleased")
	}
	all, err := s.repo.List(ctx, Filter{JobIDs: []string{parent.JobID}})
	if err != nil {
		return nil, fmt.Errorf("taskgraph.Extend: list job graph: %w", err)
	}
	if len(parentDependsOn) == 0 {
		return nil, fmt.Errorf("taskgraph.Extend: assembly parent requires preparation dependencies")
	}
	parent.DependsOn = append([]string(nil), parentDependsOn...)
	allByID := make(map[string]struct{}, len(all)+len(additions))
	for _, task := range all {
		allByID[task.ID] = struct{}{}
	}
	created := make([]Task, 0, len(additions))
	for i, addition := range additions {
		if addition.TaskID == "" || addition.ExecutorID == "" || addition.ExecutorVer <= 0 {
			return nil, fmt.Errorf("taskgraph.Extend: addition %d needs stable task and executor identities", i)
		}
		if _, exists := allByID[addition.TaskID]; exists {
			return nil, fmt.Errorf("taskgraph.Extend: task ID %q already exists in the job", addition.TaskID)
		}
		allByID[addition.TaskID] = struct{}{}
		created = append(created, Task{
			ID: addition.TaskID, JobID: parent.JobID, ProjectID: addition.ProjectID,
			RenderPlanID: addition.RenderPlanID, ExecutorID: addition.ExecutorID,
			ExecutorVersion: addition.ExecutorVer, Status: StatusPending,
			Priority: addition.Priority, DependsOn: append([]string(nil), addition.DependsOn...),
			SpecPayload: addition.Payload,
		})
	}
	all = append(all, created...)
	for i := range all {
		if all[i].ID == parentTaskID {
			all[i].DependsOn = parent.DependsOn
			break
		}
	}
	if err := ValidateTaskGraph(all); err != nil {
		return nil, fmt.Errorf("taskgraph.Extend: graph validation: %w", err)
	}
	extender, ok := s.repo.(AtomicTaskGraphExtender)
	if !ok {
		return nil, fmt.Errorf("taskgraph.Extend: repository must atomically extend task rows, dependencies and parent spec")
	}
	if err := extender.ExtendTaskGraphAtomic(ctx, parentTaskID, created, parent.DependsOn, parentPayloadPatch); err != nil {
		return nil, fmt.Errorf("taskgraph.Extend: atomic persistence: %w", err)
	}
	return created, nil
}
