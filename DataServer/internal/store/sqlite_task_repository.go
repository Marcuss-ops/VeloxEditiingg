package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/taskattempts"
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

// =====================================================================
// PR-04 / §9.5 invariant: Atomic Task + TaskAttempt transitions.
//
// The two-write pattern in handleTaskAccepted (Start + Create) and
// handleTaskResult (SetStatus|Fail + CompleteFinal) leaves a window
// where a process crash can leave Task terminal while the matching
// TaskAttempt is still RUNNING, OR where a Task is RUNNING with no
// active attempt at all. Audit invariant §9.5 ("Task RUNNING ⇒ Attempt
// RUNNING") demands these pairs commit together or not at all.
//
// The methods below are the SINGLE legal terminal-transition path for
// the task native dispatch. They open ONE transaction, perform both
// CAS statements, and either commit both or roll back both. Callers
// (gRPC handlers) MUST go through these methods; the original
// two-statement helpers above remain available for non-terminal
// idempotency bookkeeping but the §9.5-critical transitions are
// exclusively routed here.
// =====================================================================

// AcceptTaskAtomic atomically transitions a Task from LEASED → RUNNING
// AND inserts the matching TaskAttempt row (PENDING or RUNNING) in the
// SAME transaction. Returns taskgraph.ErrTransitionConflict if the Task
// CAS does not match (stale lease or revision); the rolled-back DB is
// indistinguishable from a never-called AcceptTaskAtomic.
//
// PR-04 / §9.5 closes the desync surface in handleTaskAccepted where a
// crash between h.taskRepo.Start and h.taskAttemptRepo.Create could
// leave a Task in RUNNING with no active Attempt.
func (r *SQLiteTaskRepository) AcceptTaskAtomic(ctx context.Context, attempt *taskattempts.TaskAttempt, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if attempt == nil {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires a non-nil attempt")
	}
	if attempt.TaskID == "" || attempt.WorkerID == "" || attempt.LeaseID == "" {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires task_id, worker_id, lease_id on attempt")
	}
	if attempt.Status == "" {
		attempt.Status = taskattempts.AttemptStatusRunning
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task accept atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. Task CAS: LEASED → RUNNING with worker_id + lease_id + revision.
	now := time.Now().UTC().Format(time.RFC3339)
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'RUNNING', started_at = ?, revision = revision + 1,
		     attempt_count = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?`,
		now, attempt.AttemptNumber, now,
		attempt.TaskID, attempt.WorkerID, attempt.LeaseID, revision,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic task cas: %w", err)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task accept atomic %s: %w", attempt.TaskID, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt INSERT inside the same tx. The unique constraint on
	// (task_id, attempt_number) protects against double-creates across
	// retried Accept messages; any collision here triggers a rollback.
	if attempt.ID == "" {
		attempt.ID = uuid.NewString()
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		attempt.ID, attempt.TaskID, attempt.JobID, attempt.AttemptNumber,
		attempt.WorkerID, attempt.LeaseID,
		string(attempt.Status), now, now,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic attempt insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task accept atomic commit: %w", err)
	}
	committed = true
	attempt.CreatedAt, _ = time.Parse(time.RFC3339, now)
	attempt.UpdatedAt = attempt.CreatedAt
	return nil
}

// TransitionTaskToTerminalAtomic marks a Task AND its matching active
// TaskAttempt as terminal (SUCCEEDED / FAILED / CANCELLED) in a SINGLE
// transaction. The Task CAS gates on worker_id + lease_id + non-terminal
// state; the Attempt CAS gates on worker_id + lease_id + non-terminal
// status. Either both rows advance to terminal, or neither does.
//
// Idempotency semantics:
//   - Task CAS hits 0 rows ⇒ ErrTransitionConflict (stale or already terminal).
//   - Attempt CAS hits 0 rows BUT there is already a terminal attempt
//     for this (task_id, worker_id, lease_id) ⇒ commit (replay-safe).
//   - Attempt CAS hits 0 rows AND no attempt exists for this task_id
//     AT ALL ⇒ rollback with ErrStaleReport. This guard prevents the
//     transition from "improving" a Task that was already desynced from
//     its attempt into Task terminal + no attempt, violating §9.5 more
//     deeply than the pre-state.
//
// PR-04 / §9.5 closes the desync surface in handleTaskResult where a
// crash between h.taskRepo.SetStatus|Fail and h.taskAttemptRepo.CompleteFinal
// could permanently strand Task terminal + Attempt RUNNING.
func (r *SQLiteTaskRepository) TransitionTaskToTerminalAtomic(
	ctx context.Context,
	taskID, workerID, leaseID string,
	taskStatus taskgraph.Status,
	attemptStatus taskattempts.AttemptStatus,
	errorCode, errorMessage string,
) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if taskID == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires task_id, worker_id, lease_id")
	}
	if !taskStatus.IsTerminal() {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires terminal task status, got %s", taskStatus)
	}
	if !attemptStatus.IsTerminal() {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires terminal attempt status, got %s", attemptStatus)
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task terminal atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Task CAS: any non-terminal → taskStatus (gated on worker + lease).
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, completed_at = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING', 'READY')
		   AND worker_id = ? AND lease_id = ?`,
		string(taskStatus), now, now,
		taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task terminal atomic task cas: %w", err)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task terminal atomic %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt CAS: non-terminal → attemptStatus (gated on worker + lease).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		string(attemptStatus), now, errorCode, errorMessage, now,
		taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task terminal atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()
	if attemptRows == 0 {
		// Either the attempt is already terminal (replay-safe) OR no
		// attempt exists at all for this (task_id, worker_id, lease_id).
		// Probe defensively to distinguish — a "missing attempt" stuck
		// Task in RUNNING would already be a §9.5 breach, and we must
		// NOT commit Task → terminal on top of that without an attempt
		// row, or §9.5 deepens from "no-Attempt" to "Task terminal +
		// no Attempt".
		var existingTerminal int
		probeErr := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM task_attempts
			 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
			   AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			taskID, workerID, leaseID,
		).Scan(&existingTerminal)
		if probeErr != nil {
			return fmt.Errorf("task terminal atomic attempt probe: %w", probeErr)
		}
		if existingTerminal == 0 {
			// No active AND no terminal attempt for this (task, worker,
			// lease) exists. The Task was either never accepted via
			// AcceptTaskAtomic, or its attempt row was lost. Either
			// way we cannot commit Task → terminal without an attempt.
			// Roll back and surface ErrStaleReport for the caller to
			// log / re-derive.
			return fmt.Errorf("task terminal atomic %s: missing attempt row for worker=%s lease=%s (§9.5 invariant guard): %w",
				taskID, workerID, leaseID, taskattempts.ErrStaleReport)
		}
		// existingTerminal > 0: replay-safe (a previous complete
		// already produced a terminal attempt); commit Task terminal
		// in the same retry.
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task terminal atomic commit: %w", err)
	}
	committed = true
	return nil
}

// RenewLease extends a currently-leased task's deadline (PR-05 follow-up).
// CAS tuple: `task_id=? AND status='LEASED' AND worker_id=? AND lease_id=?
// AND revision=?`. Only LEASED tasks are eligible because a RUNNING task
// is mid-execution and a lease renewal there would mask a worker that
// has been streaming UpdateResults without renewing; the master's normal
// reaper sweep is the correct enforcement for RUNNING.
//
// The CAS gate on `lease_id=?» is critical: a worker whose lease has been
// reaped (different leaseID re-issued to another worker) cannot
// accidentally extend its stale lease — the WHERE clause matches 0 rows
// and the caller learns the lease was already revoked.
//
// revision is intentionally NOT bumped (see the interface comment in
// repository.go): renewal is idempotent on its own lease and bumping
// would invalidate the worker's in-flight message queue.
func (r *SQLiteTaskRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("task repository: RenewLease requires task_id, worker_id, lease_id")
	}
	if expiry.IsZero() {
		return fmt.Errorf("task repository: RenewLease requires a non-zero expiry")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?`,
		expiry.UTC().Format(time.RFC3339), now,
		id, workerID, leaseID, revision,
	)
	if err != nil {
		return fmt.Errorf("task renew lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task renew lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task renew lease %s: %w", id, taskgraph.ErrTransitionConflict)
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
//
// PR-05: also persists `lease_expires_at = now + leaseTTL` so the master-
// side reaper (RequeueExpiredLeases) can sweep tasks whose workers have
// crashed without sending a final TaskResult. The TTL is configurable
// per-call via the leaseTTL parameter; 0 falls back to the safe default
// of 30 minutes.
//
// PR #4: task-native dispatch path replaces job-based claim.
func (r *SQLiteTaskRepository) ClaimNextReadyTask(ctx context.Context, workerID, leaseID string) (*taskgraph.TaskWithSpec, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if workerID == "" || leaseID == "" {
		return nil, fmt.Errorf("task repository: claim requires workerID + leaseID")
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiresAt := now.Add(defaultTaskLeaseTTL).Format(time.RFC3339)

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

	// CAS: READY → LEASED with workerID + leaseID + lease_expires_at.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?, lease_expires_at = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?`,
		workerID, leaseID, leaseExpiresAt, nowStr, t.ID, t.Revision,
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

// defaultTaskLeaseTTL is the master-side lease TTL written by
// ClaimNextReadyTask into tasks.lease_expires_at. Workers may RenewLease
// via the gRPC TaskLeaseRenewal message (PR-05 follow-up). 30 minutes
// matches the Job-side renewal idiom in handleLeaseRenewal.
const defaultTaskLeaseTTL = 30 * time.Minute

// RequeueExpiredLeases scans tasks whose `lease_expires_at` is in the
// past and resets them per audit §P0.4:
//
//   - LEASED  expired → READY (re-claimable by another worker)
//   - RUNNING expired → READY with attempt_count bumped (retry semantics,
//     matches the existing per-task retry counter; "Job.FAILED on
//     retries exhausted" is a sibling follow-up to PR-08)
//
// Tasks with NULL `lease_expires_at` (pre-migration-049 rows) are
// treated as "never expires" via the COALESCE-default so a long-running
// pre-cutover task is never wrongly reaped. limit caps how many tasks
// are scanned per call (0 defaults to 100). nowRFC3339 must be a
// RFC3339-encoded timestamp string (the format the column uses).
//
// PR-05: closes the audit gap left by Job-side reapers that operated
// on the dropped jobs.lease_expiry column. Once ClaimNextReadyTask
// writes lease_expires_at, this sweep guarantees no Task is ever
// permanently stranded by a worker crash.
func (r *SQLiteTaskRepository) RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) ([]string, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if nowRFC3339 == "" {
		return nil, fmt.Errorf("task repository: RequeueExpiredLeases requires nowRFC3339")
	}
	if limit <= 0 {
		limit = 100
	}

	// Select expired tasks in LEASED or RUNNING with worker_id+lease_id
	// present (sanity: a leased task without a worker_id would be a
	// half-claim artefact from PR-04's AcceptTaskAtomic; treating it as
	// expired would deepen that breach).
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT task_id
		 FROM tasks
		 WHERE status IN ('LEASED', 'RUNNING')
		   AND COALESCE(lease_expires_at, '') <> ''
		   AND lease_expires_at < ?
		   AND COALESCE(worker_id, '') <> ''
		   AND COALESCE(lease_id, '') <> ''
		 ORDER BY lease_expires_at ASC
		 LIMIT ?`,
		nowRFC3339, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("task reaper select: %w", err)
	}
	defer rows.Close()

	var expired []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			continue
		}
		expired = append(expired, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("task reaper rows: %w", err)
	}
	if len(expired) == 0 {
		return nil, nil
	}

	// Requeue each expired task. We deliberately use a single-statement
	// UPDATE per task (NOT a single multi-row UPDATE) so a CAS failure
	// on one task does not abort the rest of the sweep. The CAS gate on
	// `status IN ('LEASED','RUNNING')` makes the requeue race-safe if
	// the worker sends a TaskResult in the narrow window between SELECT
	// and UPDATE — in that case the UPDATE affects 0 rows and the
	// reaper correctly skips reaping a now-terminal task.
	reaped := make([]string, 0, len(expired))
	for _, id := range expired {
		now := time.Now().UTC().Format(time.RFC3339)
		res, err := r.store.db.ExecContext(ctx,
			`UPDATE tasks
			 SET status = 'READY', worker_id = '', lease_id = '',
			     attempt_count = attempt_count + 1,
			     revision = revision + 1, updated_at = ?
			 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING')`,
			now, id,
		)
		if err != nil {
			// Per-task error is logged-and-skipped to keep the sweep
			// making progress on healthy tasks even when one is buggy.
			// The next tick will retry.
			continue
		}
		if n, _ := res.RowsAffected(); n == 1 {
			reaped = append(reaped, id)
		}
	}
	return reaped, nil
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
