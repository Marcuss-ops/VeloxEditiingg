package store

// sqlite_task_crud.go: single-row CRUD + non-leasing lifecycle CAS
// transitions on the tasks table. No transaction wrappers, no lease-
// related CAS tuples — those live in sqlite_task_lease.go. No atomic
// multi-row gates — those live in sqlite_task_atomic.go.
// Extracted from sqlite_task_repository.go (commit dc63c57 → next).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/statemachine"
	"velox-server/internal/taskgraph"
)

// Create inserts a new task in PENDING state.
func (r *SQLiteTaskRepository) Create(ctx context.Context, task *taskgraph.Task) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if task.ID == "" {
		task.ID = uuid.NewString()
	}
	if task.Status == "" {
		task.Status = taskgraph.StatusPending
	}
	now := time.Now().UTC()
	createdAt := now.Format(time.RFC3339)
	updatedAt := createdAt

	dependsOnJSON, err := taskgraph.MarshalDependsOn(task.DependsOn)
	if err != nil {
		return fmt.Errorf("task create: depends_on: %w", err)
	}
	_, err = r.store.db.ExecContext(ctx,
		`INSERT INTO tasks (
			task_id, job_id, project_id, render_plan_id,
			executor_id, executor_version, status, priority,
			revision, attempt_count, worker_id, lease_id,
			created_at, updated_at, depends_on
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', '', ?, ?, ?)`,
		task.ID, task.JobID, task.ProjectID, task.RenderPlanID,
		task.ExecutorID, task.ExecutorVersion, string(task.Status), task.Priority,
		createdAt, updatedAt, dependsOnJSON,
	)
	if err != nil {
		return wrapDBInfrastructure("task create", err)
	}
	return nil
}

// SetDependsOn replaces the dependency edge list of one task. The fan-out
// enqueue path uses it to publish graph edges atomically with task creation;
// callers MUST pass tasks still in PENDING (edges are immutable once the
// task is dispatchable — a READY task may already have been claimed against
// the old edge list).
func (r *SQLiteTaskRepository) SetDependsOn(ctx context.Context, id string, dependsOn []string) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if id == "" {
		return fmt.Errorf("task repository: SetDependsOn: empty id")
	}
	dependsOnJSON, err := taskgraph.MarshalDependsOn(dependsOn)
	if err != nil {
		return fmt.Errorf("task repository: SetDependsOn %s: %w", id, err)
	}
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks SET depends_on = ?, updated_at = ?
		  WHERE task_id = ? AND status = 'PENDING'`,
		dependsOnJSON, time.Now().UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		return wrapDBInfrastructure("task set depends_on", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("task repository: SetDependsOn %s: task not found or not PENDING", id)
	}
	return nil
}

// Get returns a single task by ID, or (nil, nil) on missing.
func (r *SQLiteTaskRepository) Get(ctx context.Context, id string) (*taskgraph.Task, error) {
	if id == "" {
		return nil, fmt.Errorf("task repository: empty id")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ",")+` FROM tasks WHERE task_id = ?`,
		id,
	)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, wrapDBInfrastructure("task get", err)
	}
	return t, nil
}

// GetByJobID returns the task for a given job, or (nil, nil) on missing.
// Multi-task fan-out jobs (migration 176 dropped the one-task-per-job
// unique index) make this a deterministic PROJECTION rather than a
// one-to-one lookup: the earliest-created task row wins, matching the
// DAG head. Callers needing every task of a job (observability roll-ups,
// per-task live views) must use List with Filter{JobIDs: ...} instead —
// job live status and execution summaries already route per-task
// through GetWorkerTaskRuntimeByTask.
func (r *SQLiteTaskRepository) GetByJobID(ctx context.Context, jobID string) (*taskgraph.Task, error) {
	if jobID == "" {
		return nil, fmt.Errorf("task repository: empty jobID")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ",")+` FROM tasks WHERE job_id = ?
		  ORDER BY created_at ASC, task_id ASC LIMIT 1`,
		jobID,
	)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, wrapDBInfrastructure("task get by job", err)
	}
	return t, nil
}

// List returns tasks matching the filter.
func (r *SQLiteTaskRepository) List(ctx context.Context, filter taskgraph.Filter) ([]taskgraph.Task, error) {
	var conditions []string
	var args []interface{}

	if len(filter.JobIDs) > 0 {
		placeholders := strings.Repeat(",?", len(filter.JobIDs))[1:]
		conditions = append(conditions, "job_id IN ("+placeholders+")")
		for _, id := range filter.JobIDs {
			args = append(args, id)
		}
	}
	if len(filter.Statuses) > 0 {
		placeholders := strings.Repeat(",?", len(filter.Statuses))[1:]
		conditions = append(conditions, "status IN ("+placeholders+")")
		for _, s := range filter.Statuses {
			args = append(args, string(s))
		}
	}
	if filter.WorkerID != "" {
		conditions = append(conditions, "worker_id = ?")
		args = append(args, filter.WorkerID)
	}

	where := ""
	if len(conditions) > 0 {
		where = "WHERE " + strings.Join(conditions, " AND ")
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 1000
	}
	args = append(args, limit)

	query := fmt.Sprintf(
		`SELECT %s FROM tasks %s ORDER BY created_at DESC LIMIT ?`,
		strings.Join(taskColumns, ","),
		where,
	)

	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, wrapDBInfrastructure("task list", err)
	}
	defer rows.Close()

	var results []taskgraph.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, wrapDBInfrastructure("task list scan", err)
		}
		results = append(results, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("task list rows", err)
	}
	return results, nil
}

// SetStatus performs a CAS status change from → to, verifying revision.
func (r *SQLiteTaskRepository) SetStatus(ctx context.Context, id string, from, to taskgraph.Status, revision int) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainTask, string(from), string(to), ""); err != nil {
		return fmt.Errorf("task set status: %w", err)
	}
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := nowRFC3339()
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = ? AND revision = ?`,
		string(to), now, id, string(from), revision,
	)
	if err != nil {
		return wrapDBInfrastructure("task set status", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task set status rows", err)
	}
	if n == 0 {
		return fmt.Errorf("task set status %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// Lease atomically assigns a READY task to a worker.
func (r *SQLiteTaskRepository) Lease(ctx context.Context, id, workerID, leaseID string) error {
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := nowRFC3339()
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY'`,
		workerID, leaseID, now, id,
	)
	if err != nil {
		return wrapDBInfrastructure("task lease", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task lease rows", err)
	}
	if n == 0 {
		return fmt.Errorf("task lease %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// Start transitions LEASED → RUNNING with full CAS tuple.
func (r *SQLiteTaskRepository) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := nowRFC3339()
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'RUNNING', started_at = ?, revision = revision + 1,
		     attempt_count = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?`,
		now, attempt, now, id, workerID, leaseID, revision,
	)
	if err != nil {
		return wrapDBInfrastructure("task start", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task start rows", err)
	}
	if n == 0 {
		return fmt.Errorf("task start %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// Fail marks a task FAILED.
func (r *SQLiteTaskRepository) Fail(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := nowRFC3339()
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'FAILED', completed_at = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND revision = ?`,
		now, now, id, revision,
	)
	if err != nil {
		return wrapDBInfrastructure("task fail", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task fail rows", err)
	}
	if n == 0 {
		return fmt.Errorf("task fail %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// IncrementAttempt bumps the attempt counter atomically.
func (r *SQLiteTaskRepository) IncrementAttempt(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := nowRFC3339()
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks SET attempt_count = attempt_count + 1, updated_at = ? WHERE task_id = ?`,
		now, id,
	)
	if err != nil {
		return wrapDBInfrastructure("task increment attempt", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task increment attempt rows", err)
	}
	if n != 1 {
		return fmt.Errorf("task increment attempt %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// Delete hard-deletes a task.
func (r *SQLiteTaskRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx, `DELETE FROM tasks WHERE task_id = ?`, id)
	if err != nil {
		return wrapDBInfrastructure("task delete", err)
	}
	return nil
}
