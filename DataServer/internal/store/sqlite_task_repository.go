package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/taskgraph"
)

// SQLiteTaskRepository implements taskgraph.Repository against *SQLiteStore.
type SQLiteTaskRepository struct {
	store *SQLiteStore
}

// Compile-time assertion.
var _ taskgraph.Repository = (*SQLiteTaskRepository)(nil)

// NewSQLiteTaskRepository wraps a SQLiteStore as a taskgraph.Repository.
func NewSQLiteTaskRepository(store *SQLiteStore) *SQLiteTaskRepository {
	return &SQLiteTaskRepository{store: store}
}

var taskColumns = []string{
	"task_id", "job_id", "project_id", "render_plan_id",
	"executor_id", "executor_version", "status", "priority",
	"revision", "attempt_count", "worker_id", "lease_id",
	"ready_at", "started_at", "completed_at", "created_at", "updated_at",
}

func scanTask(row interface{ Scan(...interface{}) error }) (*taskgraph.Task, error) {
	var t taskgraph.Task
	var readyAt, startedAt, completedAt sql.NullString
	err := row.Scan(
		&t.ID, &t.JobID, &t.ProjectID, &t.RenderPlanID,
		&t.ExecutorID, &t.ExecutorVersion, &t.Status, &t.Priority,
		&t.Revision, &t.AttemptCount, &t.WorkerID, &t.LeaseID,
		&readyAt, &startedAt, &completedAt, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if readyAt.Valid && readyAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, readyAt.String); e == nil {
			t.ReadyAt = &pt
		}
	}
	if startedAt.Valid && startedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			t.StartedAt = &pt
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, completedAt.String); e == nil {
			t.CompletedAt = &pt
		}
	}
	return &t, nil
}

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

// ClaimNextReadyTask atomically claims the next READY task for a worker.
// CAS: READY → LEASED with workerID + leaseID. Returns the task with its
// spec payload from task_specs, or (nil, nil) if no READY task is available.
// PR #4: task-native dispatch path replaces job-based claim.
func (r *SQLiteTaskRepository) ClaimNextReadyTask(ctx context.Context, workerID, leaseID string) (*taskgraph.TaskWithSpec, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if workerID == "" || leaseID == "" {
		return nil, fmt.Errorf("task repository: claim requires workerID + leaseID")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// Find and CAS-claim the next READY task in a single tx.
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("task claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Select the next READY task (highest priority, then oldest).
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ",")+`
		 FROM tasks
		 WHERE status = 'READY'
		   AND (worker_id = '' OR worker_id IS NULL)
		 ORDER BY priority DESC, created_at ASC
		 LIMIT 1`,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task claim select: %w", err)
	}

	// CAS: READY → LEASED with workerID + leaseID.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?`,
		workerID, leaseID, now, t.ID, t.Revision,
	)
	if err != nil {
		return nil, fmt.Errorf("task claim cas: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("task claim rows: %w", err)
	}
	if n == 0 {
		// Raced with another claimer — return nil gracefully.
		return nil, nil
	}

	// Read the task_spec payload.
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("task claim spec read: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("task claim commit: %w", err)
	}

	// Update in-memory fields after successful commit.
	t.WorkerID = workerID
	t.LeaseID = leaseID
	t.Revision++

	tws := &taskgraph.TaskWithSpec{Task: *t}
	if specPayloadJSON.Valid && specPayloadJSON.String != "" && specPayloadJSON.String != "{}" {
		var payload map[string]interface{}
		if json.Unmarshal([]byte(specPayloadJSON.String), &payload) == nil {
			tws.SpecPayload = payload
		}
	}
	return tws, nil
}

// ReleaseLease atomically resets a LEASED/RUNNING task back to READY.
// Used on session teardown to release orphaned task claims (PR #4).
func (r *SQLiteTaskRepository) ReleaseLease(ctx context.Context, taskID string) error {
	if taskID == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'READY', worker_id = '', lease_id = '',
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING')`,
		now, taskID,
	)
	if err != nil {
		return fmt.Errorf("task release lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task release lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task release lease %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// AreDependenciesSatisfied returns true when all tasks in dependsOn
// have status SUCCEEDED. Returns true when dependsOn is empty.
// PR #4: used by TickReadiness for real dependency verification.
func (r *SQLiteTaskRepository) AreDependenciesSatisfied(ctx context.Context, dependsOn []string) (bool, error) {
	if len(dependsOn) == 0 {
		return true, nil
	}
	placeholders := strings.Repeat(",?", len(dependsOn))[1:]
	args := make([]interface{}, len(dependsOn))
	for i, id := range dependsOn {
		args[i] = id
	}
	var count int
	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM tasks
		 WHERE task_id IN (%s) AND status = 'SUCCEEDED'`,
		placeholders,
	)
	err := r.store.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("task deps check: %w", err)
	}
	return count == len(dependsOn), nil
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
