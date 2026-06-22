// Package taskgraph defines the canonical Task domain model for distributed
// rendering. A Task is the unit of work assigned to a single worker execution.
//
// LifecycleService manages transactional task state transitions.
// Repository is the canonical persistence contract.
package taskgraph

import (
	"context"
	"fmt"
	"time"
)

// LifecycleService manages transactional task state transitions.
type LifecycleService struct {
	repo Repository
}

// NewLifecycleService constructs the transactional LifecycleService.
func NewLifecycleService(repo Repository) (*LifecycleService, error) {
	if repo == nil {
		return nil, fmt.Errorf("taskgraph.Repository is required")
	}
	return &LifecycleService{repo: repo}, nil
}

// Repo exposes the canonical taskgraph.Repository.
func (l *LifecycleService) Repo() Repository { return l.repo }

// CreateTask creates a new task in PENDING state.
func (l *LifecycleService) CreateTask(ctx context.Context, task *Task) error {
	if task == nil {
		return fmt.Errorf("taskgraph: nil task")
	}
	return l.repo.Create(ctx, task)
}

// Transition validates and executes a status transition.
func (l *LifecycleService) Transition(ctx context.Context, id string, from, to Status, revision int) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("taskgraph: illegal transition %s → %s", from, to)
	}
	return l.repo.SetStatus(ctx, id, from, to, revision)
}

// Lease transitions READY → LEASED and assigns a worker.
func (l *LifecycleService) Lease(ctx context.Context, id, workerID, leaseID string) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("taskgraph.Lease: missing identity")
	}
	return l.repo.Lease(ctx, id, workerID, leaseID)
}

// Start transitions LEASED → RUNNING.
func (l *LifecycleService) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("taskgraph.Start: missing identity")
	}
	return l.repo.Start(ctx, id, workerID, leaseID, attempt, revision)
}

// Fail marks a task FAILED.
func (l *LifecycleService) Fail(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("taskgraph.Fail: empty taskID")
	}
	return l.repo.Fail(ctx, id, reason, revision)
}

// now normalizes a time to UTC. If t is zero, returns current time.
func now(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC()
}

// TickReadiness evaluates PENDING tasks and transitions them to READY
// when their dependencies are resolved (PR #4: real dependency verification).
//
// For each PENDING task, the dispatcher checks whether ALL tasks in
// t.DependsOn are SUCCEEDED before flipping to READY. Tasks with no
// dependencies (DependsOn empty, the single-task model) transition
// unconditionally. CAS failures from concurrent goroutines are non-fatal.
//
// Returns the number of tasks transitioned. limit caps how many tasks are
// scanned per tick; 0 uses the safe default of 100.
func (l *LifecycleService) TickReadiness(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	tasks, err := l.repo.List(ctx, Filter{
		Statuses: []Status{StatusPending},
		Limit:    limit,
	})
	if err != nil {
		return 0, fmt.Errorf("taskgraph.TickReadiness: list PENDING: %w", err)
	}
	var transitioned int
	for _, t := range tasks {
		// PR #4: verify real dependencies before transitioning.
		// Single-task model (empty DependsOn) always passes.
		satisfied, depErr := l.repo.AreDependenciesSatisfied(ctx, t.DependsOn)
		if depErr != nil {
			continue
		}
		if !satisfied {
			continue
		}
		if err := l.repo.SetStatus(ctx, t.ID, StatusPending, StatusReady, t.Revision); err != nil {
			// CAS failure (another goroutine raced) is non-fatal — skip.
			continue
		}
		transitioned++
	}
	return transitioned, nil
}
