package store

// sqlite_task_crud.go: single-row CRUD + non-leasing lifecycle CAS
// transitions on the tasks table. No transaction wrappers, no lease-
// related CAS tuples — those live in sqlite_task_lease.go. No atomic
// multi-row gates — those live in sqlite_task_atomic.go.
// Extracted from sqlite_task_repository.go (commit dc63c57 → next).

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

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

	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO tasks (
			task_id, job_id, project_id, render_plan_id,
			executor_id, executor_version, status, priority,
			revision, attempt_count, worker_id, lease_id,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', '', ?, ?)`,
		task.ID, task.JobID, task.ProjectID, task.RenderPlanID,
		task.ExecutorID, task.ExecutorVersion, string(task.Status), task.Priority,
		createdAt, updatedAt,
	)
	if err != nil {
		return fmt.Errorf("task create: %w", err)
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
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task get: %w", err)
	}
	return t, nil
}

// GetByJobID returns the task for a given job, or (nil, nil) on missing.
func (r *SQLiteTaskRepository) GetByJobID(ctx context.Context, jobID string) (*taskgraph.Task, error) {
	if jobID == "" {
		return nil, fmt.Errorf("task repository: empty jobID")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ",")+` FROM tasks WHERE job_id = ?`,
		jobID,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task get by job: %w", err)
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
		return nil, fmt.Errorf("task list: %w", err)
	}
	defer rows.Close()

	var results []taskgraph.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			continue
		}
		results = append(results, *t)
	}
	return results, rows.Err()
}

// SetStatus performs a CAS status change from → to, verifying revision.
func (r *SQLiteTaskRepository) SetStatus(ctx context.Context, id string, from, to taskgraph.Status, revision int) error {
	if id == "" {
		return fmt.Errorf("task repository: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = ? AND revision = ?`,
		string(to), now, id, string(from), revision,
	)
	if err != nil {
		return fmt.Errorf("task set status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task set status rows: %w", err)
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
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY'`,
		workerID, leaseID, now, id,
	)
	if err != nil {
		return fmt.Errorf("task lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task lease rows: %w", err)
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
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'RUNNING', started_at = ?, revision = revision + 1,
		     attempt_count = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?`,
		now, attempt, now, id, workerID, leaseID, revision,
	)
	if err != nil {
		return fmt.Errorf("task start: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task start rows: %w", err)
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
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'FAILED', completed_at = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND revision = ?`,
		now, now, id, revision,
	)
	if err != nil {
		return fmt.Errorf("task fail: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task fail rows: %w", err)
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
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks SET attempt_count = attempt_count + 1, updated_at = ? WHERE task_id = ?`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("task increment attempt: %w", err)
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
		return fmt.Errorf("task delete: %w", err)
	}
	return nil
}
