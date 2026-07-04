package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// SQLiteTaskRepository implements taskgraph.Repository against *SQLiteStore.
type SQLiteTaskRepository struct {
	store *SQLiteStore
}

func maxAttemptOrdinal(a, b int) int {
	if b > a {
		return b
	}
	return a
}

// Compile-time assertion.
var _ taskgraph.Repository = (*SQLiteTaskRepository)(nil)

// NewSQLiteTaskRepository wraps a SQLiteStore as a taskgraph.Repository.
func NewSQLiteTaskRepository(store *SQLiteStore) *SQLiteTaskRepository {
	return &SQLiteTaskRepository{store: store}
}

// taskColumns is the SELECT projection used by every Task read. Order
// MUST stay in sync with scanTask below. PR-2 (canonical-attempt-identity)
// added attempt_id + attempt_number; any later additions must update both
// the slice and the scanner.
var taskColumns = []string{
	"task_id", "job_id", "project_id", "render_plan_id",
	"executor_id", "executor_version", "status", "priority",
	"revision", "attempt_count", "attempt_id", "attempt_number",
	"worker_id", "lease_id",
	"ready_at", "started_at", "completed_at", "created_at", "updated_at",
}

func scanTask(row interface{ Scan(...interface{}) error }) (*taskgraph.Task, error) {
	var t taskgraph.Task
	var attemptID sql.NullString
	var readyAt, startedAt, completedAt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(
		&t.ID, &t.JobID, &t.ProjectID, &t.RenderPlanID,
		&t.ExecutorID, &t.ExecutorVersion, &t.Status, &t.Priority,
		&t.Revision, &t.AttemptCount, &attemptID, &t.AttemptNumber,
		&t.WorkerID, &t.LeaseID,
		&readyAt, &startedAt, &completedAt, &createdAt, &updatedAt,
	)
	if attemptID.Valid {
		t.AttemptID = attemptID.String
	}
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
	// createdAt and updatedAt are non-nullable TIMESTAMP columns stored as
	// RFC3339 strings — must be parsed explicitly (sql.Scan cannot convert
	// a TEXT column into time.Time).
	if createdAt != "" {
		if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
			t.CreatedAt = pt
		}
	}
	if updatedAt != "" {
		if pt, e := time.Parse(time.RFC3339, updatedAt); e == nil {
			t.UpdatedAt = pt
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
// §9.5 invariant: Atomic Task + TaskAttempt transitions.
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

// ClaimNextWithAttemptAtomic atomically claims the next READY task for a
// worker AND inserts the matching PENDING TaskAttempt row AND stamps
// (tasks.attempt_id, tasks.attempt_number) on the tasks row — all in
// ONE transaction. PR-2 / fix/canonical-attempt-identity single-source
// invariant: the canonical attempt identity is minted at Claim time
// and is available on the wire in the subsequent TaskOffer envelope.
//
// On success returns the claimed task (with spec payload attached) AND
// the freshly-created PENDING attempt. On contention (concurrent
// claimer wins) returns (nil, nil, nil) identically to "no READY task
// available" — the dispatcher's loop will retry on the next tick.
//
// Concurrency: SELECT…LIMIT 1 + CAS UPDATE READY→LEASED + INSERT attempt
// + rowstamp attempt_id/attempt_number on tasks. All in one tx.
//
// Failure modes (ErrTransitionConflict surfaced clearly):
//   - worker_id or lease_id is empty (programmer error)
//   - no READY task is available → (nil, nil, nil), not an error
//   - UPDATE row count != 1 (stale READY → another dispatcher took it)
//   - INSERT attempt collision with UNIQUE(task_id, attempt_number) —
//     should never happen for freshly-minted UUIDs but a stale manual
//     duplicate inject would surface as ErrTransitionConflict
func (r *SQLiteTaskRepository) ClaimNextWithAttemptAtomic(ctx context.Context, workerID, leaseID string) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	if r.store == nil || r.store.db == nil {
		return nil, nil, fmt.Errorf("task repository: store not initialized")
	}
	if workerID == "" || leaseID == "" {
		return nil, nil, fmt.Errorf("task repository: claim-with-attempt requires workerID + leaseID")
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiresAt := now.Add(defaultTaskLeaseTTL).Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1. SELECT next READY task candidate (priority DESC, created_at ASC).
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ", ")+`
		 FROM tasks
		 WHERE status = 'READY'
		   AND (worker_id = '' OR worker_id IS NULL)
		 ORDER BY priority DESC, created_at ASC
		 LIMIT 1`,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt select: %w", err)
	}

	// 2. Self-heal stale attempt_count from immutable attempt history.
	// If a prior timeout/requeue left tasks.attempt_count behind the
	// actual max(task_attempts.attempt_number), deriving the next attempt
	// from the stale task row would collide on UNIQUE(task_id,
	// attempt_number) and strand the task in READY forever.
	var maxSeenAttempt sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?`,
		t.ID,
	).Scan(&maxSeenAttempt); err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt max attempt read: %w", err)
	}
	effectiveAttemptCount := t.AttemptCount
	if maxSeenAttempt.Valid {
		effectiveAttemptCount = maxAttemptOrdinal(effectiveAttemptCount, int(maxSeenAttempt.Int64))
	}

	// 3. Generate canonical attempt identity BEFORE CAS so a CAS race
	// failure doesn't leave a task_attempts row orphaned.
	attemptID := uuid.NewString()
	attemptNumber := effectiveAttemptCount + 1

	// 4. CAS: READY → LEASED on tasks + stamp attempt_id + attempt_number.
	// attempt_count advances to the freshly-minted attempt so the task row
	// stays aligned with immutable task_attempts history even before the
	// worker accepts the offer.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?, lease_expires_at = ?,
		     attempt_count = ?, attempt_id = ?, attempt_number = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?`,
		workerID, leaseID, leaseExpiresAt, attemptNumber, attemptID, attemptNumber,
		nowStr, t.ID, t.Revision,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt cas: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt rows: %w", err)
	}
	if n == 0 {
		// Raced with another claimer — return nil gracefully.
		return nil, nil, nil
	}

	// 5. INSERT PENDING TaskAttempt in the same tx.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, t.ID, t.JobID, attemptNumber, workerID, leaseID, nowStr, nowStr,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt insert: %w", err)
	}

	// 6. Read task_spec payload (continues ClaimNextReadyTask ergonomics).
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, fmt.Errorf("task claim-with-attempt spec read: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt commit: %w", err)
	}

	// Update in-memory fields after successful commit.
	t.WorkerID = workerID
	t.LeaseID = leaseID
	t.AttemptCount = attemptNumber
	t.AttemptID = attemptID
	t.AttemptNumber = attemptNumber
	t.Revision++

	tws := &taskgraph.TaskWithSpec{Task: *t}
	if specPayloadJSON.Valid && specPayloadJSON.String != "" && specPayloadJSON.String != "{}" {
		var payload map[string]interface{}
		if json.Unmarshal([]byte(specPayloadJSON.String), &payload) == nil {
			tws.SpecPayload = payload
		}
	}

	att := &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        t.ID,
		JobID:         t.JobID,
		AttemptNumber: attemptNumber,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		Status:        taskattempts.AttemptStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	return tws, att, nil
}

// AcceptTaskAtomic atomically transitions a Task from LEASED → RUNNING
// AND UPDATES the matching PENDING TaskAttempt to RUNNING in the SAME
// transaction. The single legal entry point for promoting a worker
// offer to a running execution. Returns taskgraph.ErrTransitionConflict
// if the Task CAS does not match (stale lease or revision); the
// rolled-back DB is indistinguishable from a never-called AcceptTaskAtomic.
//
// PR-2 (canonical-attempt-identity) CHANGED this method:
//   - Pre-PR-2 INSERTed the TaskAttempt row (because Claim did NOT pre-create one).
//   - Post-PR-2 the PENDING TaskAttempt row was inserted by ClaimNextWithAttemptAtomic
//     at Claim time, so AcceptTaskAtomic now UPDATEs status PENDING → RUNNING.
//   - The CAS tuple gains attempt_id + attempt_number on the Task row stamp
//     so a replay / stale-acceptance is bounded by both Task CAS and Attempt CAS.
//
// §9.5 closes the desync surface in handleTaskAccepted where a
// crash between h.taskRepo.Start and h.taskAttemptRepo.Create could
// leave a Task in RUNNING with no active Attempt. POST-PR-2 the PENDING
// attempt row is created atomically with the LEASED CAS at Claim time,
// so the §9.5 invariant holds at the moment of TaskOffer send.
func (r *SQLiteTaskRepository) AcceptTaskAtomic(ctx context.Context, attempt *taskattempts.TaskAttempt, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if attempt == nil {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires a non-nil attempt")
	}
	if attempt.TaskID == "" || attempt.WorkerID == "" || attempt.LeaseID == "" || attempt.ID == "" {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires task_id, worker_id, lease_id, attempt_id (canonical from Claim)")
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

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Task CAS: LEASED → RUNNING with worker_id + lease_id + revision.
	// PR-2 also asserts (attempt_id, attempt_number) match the canonical
	// row stamped at Claim time, so a re-entry with a mismatched attempt
	// surfaces as ErrTransitionConflict instead of silently advancing the
	// wrong attempt.
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'RUNNING', started_at = ?, revision = revision + 1,
		     attempt_count = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?
		   AND attempt_id = ? AND attempt_number = ?`,
		now, attempt.AttemptNumber, now,
		attempt.TaskID, attempt.WorkerID, attempt.LeaseID, revision,
		attempt.ID, attempt.AttemptNumber,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic task cas: %w", err)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task accept atomic %s (canonical attempt mismatch?): %w", attempt.TaskID, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt UPDATE: PENDING → RUNNING in the same tx. The CAS tuple
	// enforces (id, task_id, attempt_number, worker_id, lease_id, PENDING);
	// any collision surfaces ErrTransitionConflict (attempt_row CAS gate
	// matches the audit §9.5 invariant on Task RUNNING ⇒ Attempt RUNNING).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = 'RUNNING', updated_at = ?
		 WHERE id = ? AND task_id = ? AND attempt_number = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status = 'PENDING'`,
		now, attempt.ID, attempt.TaskID, attempt.AttemptNumber,
		attempt.WorkerID, attempt.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic attempt cas: %w", err)
	}
	if n, _ := attRes.RowsAffected(); n != 1 {
		// Either: attempt row missing (reject — a §9.5 desync since
		// ClaimNextWithAttemptAtomic would have created it) OR attempt
		// is already RUNNING (replay-safe no-op: but in that case the
		// UPDATE should have hit 1 row, so we land here only on
		// genuinely-missing rows).
		return fmt.Errorf("task accept atomic attempt %s not PENDING or missing (canonical drift): %w",
			attempt.ID, taskgraph.ErrTransitionConflict)
	}

	// 3. Job roll-up: once the worker acceptance is persisted, the parent
	// Job must become RUNNING in the same transaction so artifact upload
	// admission sees a consistent lifecycle state. We intentionally keep the
	// BeginUpload gate strict and only promote promotable Job states here.
	jobRes, err := tx.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'RUNNING',
		     started_at = COALESCE(started_at, ?),
		     updated_at = ?,
		     revision = CASE
		         WHEN status IN ('PENDING', 'RETRY_WAIT') THEN revision + 1
		         ELSE revision
		     END
		 WHERE job_id = ?
		   AND status IN ('PENDING', 'RETRY_WAIT', 'RUNNING')`,
		now, now, attempt.JobID,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic job cas: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task accept atomic job %s not promotable to %s: %w",
			attempt.JobID, jobs.StatusRunning, taskgraph.ErrTransitionConflict)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task accept atomic commit: %w", err)
	}
	committed = true
	attempt.CreatedAt, _ = time.Parse(time.RFC3339, now)
	attempt.UpdatedAt = attempt.CreatedAt
	attempt.Status = taskattempts.AttemptStatusRunning
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
// §9.5 closes the desync surface in handleTaskResult where a
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

// IngestTaskResultAtomic is the single legal entry point for ingesting
// a worker TaskResult. It atomically transitions Task + Attempt to
// terminal AND persists typed metrics, cache stats, cost basis, AND
// registers output artifact declarations in ONE database transaction.
// No partial writes: if any step fails, everything rolls back.
//
// fix/atomic-ingestion: replaces the 4-step sequence
// (TransitionTaskToTerminalAtomic + PersistMetrics + PersistCacheStats +
// PersistCostBasis + per-artifact Register) with a single atomic call.
//
// Returns ErrTransitionConflict on stale Task CAS; the caller must NOT
// proceed with artifact registration or job roll-up on this error.
// Returns taskattempts.ErrStaleReport when the Task CAS succeeds but
// no active attempt exists for the identity tuple (§9.5 guard).
func (r *SQLiteTaskRepository) IngestTaskResultAtomic(ctx context.Context, cmd taskgraph.IngestResultCommand) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if cmd.TaskID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires task_id, worker_id, lease_id")
	}
	if !cmd.TaskStatus.IsTerminal() {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires terminal task status, got %s", cmd.TaskStatus)
	}
	if !cmd.AttemptStatus.IsTerminal() {
		return fmt.Errorf("task repository: IngestTaskResultAtomic requires terminal attempt status, got %s", cmd.AttemptStatus)
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task ingest atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Task CAS:
	//   - SUCCEEDED: Phase 2.6 — do NOT flip tasks.status to
	//     SUCCEEDED. Instead stamp winning_attempt_terminal_pending=1
	//     while leaving status='RUNNING'. Coordinator.CommitAttempt
	//     ratifies SUCCEEDED in a single atomic tx (Fase 2.5).
	//     REPLAY-SAFE: if the task is already SUCCEEDED for THIS
	//     attempt (CommitAttempt may have raced ahead and committed
	//     before this Ingest landed), we skip the Task CAS so the
	//     metric/cache/cost/artifacts writes below still commit.
	//   - FAILED: existing terminal write (no commit-protocol gate;
	//     the task truly failed and must NOT be reanimated by
	//     Reconcile).
	alreadyTerminalForThisAttempt, probeErr := func() (bool, error) {
		var cs, ca string
		probeErr := tx.QueryRowContext(ctx,
			`SELECT status, COALESCE(attempt_id, '') FROM tasks WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		).Scan(&cs, &ca)
		if probeErr == sql.ErrNoRows {
			return false, fmt.Errorf("task ingest atomic %s: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
		}
		if probeErr != nil {
			return false, fmt.Errorf("task ingest atomic probe: %w", probeErr)
		}
		return cs == "SUCCEEDED" && ca == cmd.AttemptID, nil
	}()
	if probeErr != nil {
		return probeErr
	}
	if !alreadyTerminalForThisAttempt {
		var (
			taskRes sql.Result
			errCas  error
		)
		if cmd.TaskStatus == taskgraph.StatusSucceeded {
			taskRes, errCas = tx.ExecContext(ctx,
				`UPDATE tasks
				 SET winning_attempt_terminal_pending = 1,
				     completed_at = ?, updated_at = ?
				 WHERE task_id = ? AND status = 'RUNNING'
				   AND attempt_id = ? AND worker_id = ? AND lease_id = ?`,
				now, now,
				cmd.TaskID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID,
			)
		} else {
			taskRes, errCas = tx.ExecContext(ctx,
				`UPDATE tasks
				 SET status = ?, completed_at = ?, revision = revision + 1, updated_at = ?
				 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING', 'READY')
				   AND worker_id = ? AND lease_id = ?`,
				string(cmd.TaskStatus), now, now,
				cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
			)
		}
		if errCas != nil {
			return fmt.Errorf("task ingest atomic task cas: %w", errCas)
		}
		if n, _ := taskRes.RowsAffected(); n != 1 {
			return fmt.Errorf("task ingest atomic %s: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
		}
	}

	// 2. Attempt CAS: non-terminal → attemptStatus (gated on worker + lease).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		string(cmd.AttemptStatus), now, cmd.ErrorCode, cmd.ErrorMsg, now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()
	if attemptRows == 0 {
		// Either the attempt is already terminal (replay-safe) OR no
		// attempt exists at all for this (task_id, worker_id, lease_id).
		var existingTerminal int
		probeErr := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM task_attempts
			 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
			   AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		).Scan(&existingTerminal)
		if probeErr != nil {
			return fmt.Errorf("task ingest atomic attempt probe: %w", probeErr)
		}
		if existingTerminal == 0 {
			return fmt.Errorf("task ingest atomic %s: missing attempt row for worker=%s lease=%s (§9.5 invariant guard): %w",
				cmd.TaskID, cmd.WorkerID, cmd.LeaseID, taskattempts.ErrStaleReport)
		}
		// existingTerminal > 0: replay-safe — continue with idempotent
		// metrics/cache/cost/artifact writes below.
	}

	// 3. Persist typed execution metrics (idempotent INSERT OR REPLACE).
	if cmd.Metrics.AttemptID != "" {
		streamCopy := 0
		if cmd.Metrics.FinalConcatStreamCopy {
			streamCopy = 1
		}
		concatMode := cmd.Metrics.ConcatMode
		if concatMode == "" {
			concatMode = "n/a"
		}
		_, err = tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_attempt_metrics (
				attempt_id, input_bytes, output_bytes,
				bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
				cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
				frames_decoded, frames_composited, frames_encoded,
				ffmpeg_speed_ratio, encode_passes,
				final_concat_stream_copy, concat_mode,
				temp_bytes_written, duplicate_download_bytes,
				media_duration_seconds, wall_clock_seconds
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cmd.Metrics.AttemptID, cmd.Metrics.InputBytes, cmd.Metrics.OutputBytes,
			cmd.Metrics.BytesFromDrive, cmd.Metrics.BytesFromBlobstore, cmd.Metrics.BytesFromLocalCache,
			cmd.Metrics.CPUTimeMS, cmd.Metrics.GPUTimeMS, cmd.Metrics.PeakRSSBytes, cmd.Metrics.PeakVRAMBytes,
			cmd.Metrics.FramesDecoded, cmd.Metrics.FramesComposited, cmd.Metrics.FramesEncoded,
			cmd.Metrics.FFmpegSpeedRatio, cmd.Metrics.EncodePasses,
			streamCopy, concatMode,
			cmd.Metrics.TempBytesWritten, cmd.Metrics.DuplicateDownloadBytes,
			cmd.Metrics.MediaDurationSeconds, cmd.Metrics.WallClockSeconds,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic metrics: %w", err)
		}
	}

	// 4. Persist cache stats (idempotent INSERT OR REPLACE).
	if cmd.CacheStats.AttemptID != "" {
		_, err = tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_attempt_cache_stats (
				attempt_id, cache_hits, cache_misses, cache_evictions,
				cache_corruptions, cache_bytes_used, cache_entries
			) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			cmd.CacheStats.AttemptID, cmd.CacheStats.CacheHits, cmd.CacheStats.CacheMisses,
			cmd.CacheStats.CacheEvictions, cmd.CacheStats.CacheCorruptions,
			cmd.CacheStats.CacheBytesUsed, cmd.CacheStats.CacheEntries,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic cache stats: %w", err)
		}
	}

	// 5. Persist cost basis (idempotent INSERT OR REPLACE).
	if cmd.CostBasis.AttemptID != "" {
		_, err = tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_attempt_cost_basis (
				attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
				cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			cmd.CostBasis.AttemptID, cmd.CostBasis.CPUPricePerSecond,
			cmd.CostBasis.StoragePricePerGB, cmd.CostBasis.NetworkPricePerGB,
			cmd.CostBasis.CPUTimeSecondsTotal, cmd.CostBasis.StorageGBWritten,
			cmd.CostBasis.NetworkGBEgressed, cmd.CostBasis.OutputMinutesTotal,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic cost basis: %w", err)
		}
	}

	// 6. Register output artifacts (skip on UNIQUE conflict, fail on other errors).
	for _, a := range cmd.Artifacts {
		if a.ArtifactID == "" {
			continue
		}
		metadata := a.MetadataJSON
		if metadata == "" {
			metadata = "{}"
		}
		_, artErr := tx.ExecContext(ctx,
			`INSERT INTO task_output_artifacts
			 (task_id, attempt_id, artifact_id, artifact_type, declared_path,
			  declared_size, declared_sha256, metadata_json, registered_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.TaskID, a.AttemptID, a.ArtifactID, a.ArtifactType, a.DeclaredPath,
			a.DeclaredSize, a.DeclaredSHA256, metadata, now,
		)
		if artErr != nil {
			if isUniqueConflict(artErr) {
				continue // duplicate declaration → skip, don't fail the tx
			}
			return fmt.Errorf("task ingest atomic artifact %s: %w", a.ArtifactID, artErr)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task ingest atomic commit: %w", err)
	}
	committed = true
	return nil
}

// RenewLease extends a currently-leased or running task's deadline
// (PR-03 / fix/task-lease-renewal-protocol). CAS tuple:
//
//	task_id=? AND worker_id=? AND lease_id=?
//	AND status IN ('LEASED', 'RUNNING') AND revision=?
//
// Acceptance of BOTH states is intentional: a worker progressed to
// RUNNING after TaskLeaseGranted is acknowledged and a task longer
// than the 30-min TTL must renew without first being reaped.
//
// The CAS intentionally does NOT gate on attempt_id: AcceptTaskAtomic
// is the sole writer of attempt_id on tasks, and a worker cannot hold
// two different attempt_ids for the same task concurrently. The
// (worker_id, lease_id) tuple already binds the renewal to the
// canonical attempt implicitly. The TOCTOU race against reaper-reset
// is closed by (worker_id, lease_id) gates alone — a stale worker on
// (W1, L1) cannot match a freshly re-stamped row with (W2, L2).
//
// revision is intentionally NOT bumped (see the interface comment):
// renewal is idempotent on its own (task_id, worker_id, lease_id, revision)
// tuple.
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
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ? AND revision = ?
		   AND status IN ('LEASED', 'RUNNING')`,
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

// ExpireTaskLeaseAtomic reaps a single task in one atomic transaction
// following the audit-mandated contract for the atomic transition:
//
//  1. CAS-gate on (task_id, lease_id, lease_expires_at, worker_id) where
//     lease_expires_at is the OBSERVED (pre-reap) value; a worker that
//     just renewed would have written a NEWER lease_expires_at and our
//     CAS sees 0 rows → ErrTransitionConflict (audit P0#6 fix).
//  2. Attempt close: TX-gated UPDATE on task_attempts for the
//     (task_id, worker_id, lease_id) tuple, status non-terminal →
//     TIMED_OUT. Inlined into the same tx so a process crash between
//     Task UPDATE and Attempt UPDATE cannot leave Task at READY/FAILED
//     with Attempt RUNNING (audit §9.5 invariant).
//  3. Retry budget: if attempt_count >= maxRetries + 1, set task →
//     FAILED (terminal). Otherwise task → READY (re-claimable).
//  4. Clear worker_id, lease_id, lease_expires_at; bump revision.
//  5. attempt_count is INTENTIONALLY NOT bumped here (audit P0#4:
//     counter reflects STARTED attempts, owned by AcceptTaskAtomic).
//
// maxRetries <= 0 falls back to a safe default of 3.
func (r *SQLiteTaskRepository) ExpireTaskLeaseAtomic(
	ctx context.Context,
	taskID, leaseID, leaseExpiresAtObserved string,
	maxRetries int,
) (taskgraph.ExpireResult, error) {
	if r.store == nil || r.store.db == nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task repository: store not initialized")
	}
	if taskID == "" || leaseID == "" {
		return taskgraph.ExpireResult{}, fmt.Errorf("task repository: ExpireTaskLeaseAtomic requires task_id and lease_id")
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Read task to obtain attempt_count + status + worker_id + lease_id
	// + lease_expires_at for the CAS gate.
	var (
		attemptCount         int
		currentAttemptNumber int
		currentStatus        string
		currentWorker        string
		currentLeaseID       string
		currentLeaseExp      string
	)
	err = tx.QueryRowContext(ctx,
		`SELECT attempt_count, attempt_number, status,
		        COALESCE(worker_id, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&attemptCount, &currentAttemptNumber, &currentStatus, &currentWorker, &currentLeaseID, &currentLeaseExp)
	if err == sql.ErrNoRows {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: not found", taskID)
	}
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic read: %w", err)
	}

	if currentStatus != string(taskgraph.StatusLeased) && currentStatus != string(taskgraph.StatusRunning) {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: not in LEASED/RUNNING (status=%s): %w",
			taskID, currentStatus, taskgraph.ErrTransitionConflict)
	}
	if currentLeaseID != leaseID {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: lease_id mismatch (got=%s, db=%s): %w",
			taskID, leaseID, currentLeaseID, taskgraph.ErrTransitionConflict)
	}
	if currentLeaseExp != leaseExpiresAtObserved {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: lease_expires_at mismatch (got=%s, db=%s): %w",
			taskID, leaseExpiresAtObserved, currentLeaseExp, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt close: TX-gated UPDATE on task_attempts for the identity
	// tuple. Inlined into the same tx (no CompleteByIdentityTimedOut
	// indirection) so a process crash between Task UPDATE and Attempt
	// UPDATE cannot leave Task at READY/FAILED with Attempt still
	// RUNNING — both commit together or neither does (audit §9.5).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = 'TIMED_OUT', completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`,
		now, "LEASE_EXPIRED", "master-side lease TTL exceeded", now,
		taskID, currentWorker, leaseID,
	)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()

	var attemptID string
	idProbeErr := tx.QueryRowContext(ctx,
		`SELECT id FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, currentWorker, leaseID,
	).Scan(&attemptID)
	if idProbeErr != nil {
		// Defensive §9.5 case: task in LEASED/RUNNING with no matching
		// attempt row. The Task CAS still proceeds (lease recovered),
		// but AttemptClosed=false so the reaper logs the breach.
		attemptID = ""
		attemptRows = 0
	}

	effectiveAttemptCount := maxAttemptOrdinal(attemptCount, currentAttemptNumber)

	// 3. Retry budget. attempt_count >= maxRetries + 1 means the next
	// AcceptTask would exceed the configured budget — reap terminates
	// the task as FAILED. Otherwise the task is requeueable as READY.
	exhausted := effectiveAttemptCount >= maxRetries+1
	newStatus := taskgraph.StatusReady
	if exhausted {
		newStatus = taskgraph.StatusFailed
	}

	// 4. Task CAS-gate update. Status flips to newStatus; worker/lease/
	// lease_expires_at cleared; revision bumped. CAS-tuple reinforces
	// the read above so a parallel AcceptTaskAtomic / Transition races
	// us out instead of us blindly overwriting.
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, completed_at = ?,
		     worker_id = '', lease_id = '', lease_expires_at = NULL,
		     attempt_count = ?, attempt_id = '', attempt_number = 0,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = ? AND worker_id = ? AND lease_id = ?`,
		string(newStatus), now, effectiveAttemptCount, now,
		taskID, currentStatus, currentWorker, leaseID,
	)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic task cas: %w", err)
	}
	taskRows, _ := taskRes.RowsAffected()
	if taskRows == 0 {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: task CAS raced out: %w",
			taskID, taskgraph.ErrTransitionConflict)
	}

	if err := tx.Commit(); err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic commit: %w", err)
	}
	committed = true

	return taskgraph.ExpireResult{
		TaskID:            taskID,
		TaskStatus:        newStatus,
		AttemptsExhausted: exhausted,
		AttemptID:         attemptID,
		AttemptClosed:     attemptRows > 0 && attemptID != "",
	}, nil
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

// IsAllAttemptCommitsCommittedForTasks is the Phase 2.8 roll-up gate
// consumed by TaskReportIngestionService.maybeTransitionJob. Returns
// true iff every taskID has an attempt_commits row with status='COMMITTED'.
// Tasks with no attempt_commits row (legacy pre-Phase-2 paths or
// pre-commit-protocol workers) are treated as NOT-committed and block
// the Job's AWAITING_ARTIFACT promotion.
//
// Distinct CAST ensures the COUNT only counts rows that are uniquely
// matched per task_id; duplicates from re-declaration (UNIQUE
// task_id+attempt_id is a different layer) are still distinct here.
//
// Empty taskIDs returns false (defensive: nothing to commit).
func (r *SQLiteTaskRepository) IsAllAttemptCommitsCommittedForTasks(ctx context.Context, taskIDs []string) (bool, error) {
	if r.store == nil || r.store.db == nil {
		return false, fmt.Errorf("task repository: store not initialized")
	}
	if len(taskIDs) == 0 {
		return false, nil
	}
	placeholders := strings.Repeat(",?", len(taskIDs))[1:]
	args := make([]interface{}, len(taskIDs))
	for i, id := range taskIDs {
		args[i] = id
	}
	var committed int
	err := r.store.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT task_id) FROM attempt_commits
		  WHERE task_id IN (`+placeholders+`) AND status = 'COMMITTED'`,
		args...,
	).Scan(&committed)
	if err != nil {
		return false, fmt.Errorf("task repository: IsAllAttemptCommitsCommittedForTasks: %w", err)
	}
	return committed == len(taskIDs), nil
}

// defaultTaskLeaseTTL is the master-side lease TTL written by
// ClaimNextReadyTask into tasks.lease_expires_at. Workers may RenewLease
// via the gRPC TaskLeaseRenewal message (PR-05 follow-up). 30 minutes
// matches the Job-side renewal idiom in handleLeaseRenewal.
const defaultTaskLeaseTTL = 30 * time.Minute

// RequeueExpiredLeases scans tasks whose `lease_expires_at` is in the
// past and surfaces them as RequeueCandidate rows. SELECT-only: no
// UPDATE happens here. Per-task ExpireTaskLeaseAtomic owns the write
// so the audit-mandated CAS tuple + retry budget + Attempt close
// always run in a single tx.
//
// Tasks with NULL `lease_expires_at` (pre-migration-049 rows) are
// treated as "never expires" via the COALESCE-default so a long-running
// pre-cutover task is never wrongly reaped. limit caps how many tasks
// are scanned per call (0 defaults to 100). nowRFC3339 must be a
// RFC3339-encoded timestamp string (the format the column uses).
//
// PR-05 set up the master-side lease enforcement. The audit
// P0#4+P0#6 transforms this method into SELECT-only so per-task
// ExpireTaskLeaseAtomic closes the attempt + applies retry budget +
// CAS-gates on (task_id, lease_id, lease_expires_at, worker_id) in
// one tx.
func (r *SQLiteTaskRepository) RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) ([]taskgraph.RequeueCandidate, error) {
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
	// present. Full identity columns (worker_id, lease_id,
	// lease_expires_at) are pulled so the reaper can build the
	// candidate without a second roundtrip. A leased task without a
	// worker_id is a half-claim artefact and is skipped.
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT task_id, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''), attempt_count
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

	var candidates []taskgraph.RequeueCandidate
	for rows.Next() {
		var c taskgraph.RequeueCandidate
		if scanErr := rows.Scan(&c.ID, &c.WorkerID, &c.LeaseID, &c.LeaseExpiresAt, &c.AttemptCount); scanErr != nil {
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("task reaper rows: %w", err)
	}
	return candidates, nil
}

// ReleaseLease atomically resets a LEASED/RUNNING task back to READY.
// CAS gates on (task_id, worker_id, lease_id) so a stale reject from
// Worker A with lease L1 cannot release a task reassigned to Worker B
// with lease L2 (TOCTOU closure for handleTaskRejected — the previously
// documented read-then-release gap is now closed at the SQL level).
//
// Used on session teardown to release orphaned task claims (PR #4)
// and by handleTaskRejected to return a rejected task to the pool.
func (r *SQLiteTaskRepository) ReleaseLease(ctx context.Context, taskID, workerID, leaseID string) error {
	if taskID == "" {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task release lease begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'READY', worker_id = '', lease_id = '',
		     lease_expires_at = NULL, attempt_id = NULL, attempt_number = 0,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status IN ('LEASED', 'RUNNING')`,
		now, taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task release lease task update: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task release lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task release lease %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}

	// A released offer was never accepted into RUNNING, so its canonical
	// PENDING attempt must be removed to let the next claim reuse the same
	// attempt number (attempt_count only advances on AcceptTaskAtomic).
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ? AND status = 'PENDING'`,
		taskID, workerID, leaseID,
	); err != nil {
		return fmt.Errorf("task release lease delete pending attempt: %w", err)
	}

	// Recompute attempt_count from the immutable residual history after
	// deleting the released PENDING offer. This keeps
	// tasks.attempt_count >= MAX(task_attempts.attempt_number) without
	// permanently skipping an ordinal for offers that never started.
	if _, err := tx.ExecContext(ctx,
		`UPDATE tasks
		    SET attempt_count = COALESCE(
		    	(SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?),
		    	0
		    )
		  WHERE task_id = ?`,
		taskID, taskID,
	); err != nil {
		return fmt.Errorf("task release lease reconcile attempt_count: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task release lease commit: %w", err)
	}
	committed = true
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

// placementCandidateBatch is the default limit for ListReadyCandidates.
const placementCandidateBatch = 64

// ListReadyCandidates returns lightweight task metadata rows for the
// placement matcher. Only the columns needed for placement decisions
// are fetched — full payloads are loaded later by ClaimTaskForWorkerAtomic.
//
// Query: SELECT task_id, job_id, revision, priority, created_at,
// executor_id, executor_version FROM tasks WHERE status='READY'
// AND (worker_id=” OR worker_id IS NULL) ORDER BY priority DESC,
// created_at ASC LIMIT ?.
//
// limit <= 0 falls back to a safe default (placementCandidateBatch = 64).
func (r *SQLiteTaskRepository) ListReadyCandidates(ctx context.Context, limit int) ([]placement.TaskCandidate, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if limit <= 0 {
		limit = placementCandidateBatch
	}

	rows, err := r.store.db.QueryContext(ctx,
		`SELECT t.task_id, t.job_id, t.revision, t.priority, t.created_at,
		        t.executor_id, t.executor_version,
		        GROUP_CONCAT(tr.capability) AS required_capabilities
		 FROM tasks t
		 LEFT JOIN task_requirements tr ON tr.task_id = t.task_id
		 WHERE t.status = 'READY'
		   AND (t.worker_id = '' OR t.worker_id IS NULL)
		 GROUP BY t.task_id
		 ORDER BY t.priority DESC, t.created_at ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("task list ready candidates: %w", err)
	}
	defer rows.Close()

	var candidates []placement.TaskCandidate
	for rows.Next() {
		var (
			taskID             string
			jobID              string
			revision           int
			priority           int
			createdAt          string
			executorID         string
			executorVersion    int
			capabilitiesConcat sql.NullString
		)
		if scanErr := rows.Scan(&taskID, &jobID, &revision, &priority, &createdAt, &executorID, &executorVersion, &capabilitiesConcat); scanErr != nil {
			continue
		}

		var parsedTime time.Time
		if createdAt != "" {
			if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
				parsedTime = pt
			}
		}

		var capabilities []string
		if capabilitiesConcat.Valid && capabilitiesConcat.String != "" {
			capabilities = strings.Split(capabilitiesConcat.String, ",")
		}

		execKey := placement.NormalizeExecutorKey(executorID, executorVersion)

		candidates = append(candidates, placement.TaskCandidate{
			TaskID:               taskID,
			JobID:                jobID,
			Revision:             revision,
			Priority:             priority,
			CreatedAt:            parsedTime,
			Executor:             execKey,
			RequiredCapabilities: capabilities,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("task list ready candidates rows: %w", err)
	}

	return candidates, nil
}

// ClaimTaskForWorkerAtomic atomically claims a specific READY task
// chosen by the placement matcher. CAS-gates on (task_id, revision,
// executor_id, executor_version) so a concurrent dispatcher that
// claimed the same task between ListReadyCandidates and this call
// will see the CAS fail and return ErrTransitionConflict.
//
// The transaction steps mirror ClaimNextWithAttemptAtomic:
//  1. SELECT task WHERE task_id=? AND status='READY' AND revision=?
//     AND executor_id=? AND executor_version=?
//  2. Self-heal attempt_count from immutable attempt history.
//  3. Generate canonical attempt ID before CAS.
//  4. CAS READY → LEASED + stamp attempt_id / attempt_number.
//  5. INSERT PENDING TaskAttempt.
//  6. Read task_spec payload.
//  7. Commit.
//
// SessionID and CapabilityRevision are carried through for fencing
// (the caller checks them before and after the claim); they are NOT
// persisted yet but must travel in the command so the eventual
// transactional fencing doesn't require a signature change.
func (r *SQLiteTaskRepository) ClaimTaskForWorkerAtomic(
	ctx context.Context,
	cmd taskgraph.ClaimTaskForWorkerCommand,
) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	if r.store == nil || r.store.db == nil {
		return nil, nil, fmt.Errorf("task repository: store not initialized")
	}
	if cmd.TaskID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return nil, nil, fmt.Errorf("task repository: ClaimTaskForWorkerAtomic requires task_id, worker_id, lease_id")
	}
	execKey := placement.NormalizeExecutorKey(cmd.ExecutorID, cmd.ExecutorVersion)
	if execKey.ID == "" || execKey.Version <= 0 {
		return nil, nil, fmt.Errorf("task repository: ClaimTaskForWorkerAtomic requires executor_id and executor_version > 0")
	}
	legacyExecutorID := placement.VersionedExecutorID(execKey.ID, execKey.Version)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiresAt := now.Add(defaultTaskLeaseTTL).Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1. SELECT the specific task candidate with revision + executor gate.
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ", ")+`
		 FROM tasks
		 WHERE task_id = ?
		   AND status = 'READY'
		   AND revision = ?
		   AND executor_id IN (?, ?)
		   AND executor_version = ?
		   AND (worker_id = '' OR worker_id IS NULL)`,
		cmd.TaskID, cmd.ExpectedTaskRevision, execKey.ID, legacyExecutorID, execKey.Version,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: task not READY or executor/revision mismatch: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker select: %w", err)
	}

	// 2. Self-heal stale attempt_count from immutable attempt history.
	var maxSeenAttempt sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?`,
		t.ID,
	).Scan(&maxSeenAttempt); err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker max attempt read: %w", err)
	}
	effectiveAttemptCount := t.AttemptCount
	if maxSeenAttempt.Valid {
		effectiveAttemptCount = maxAttemptOrdinal(effectiveAttemptCount, int(maxSeenAttempt.Int64))
	}

	// 3. Generate canonical attempt identity BEFORE CAS.
	attemptID := uuid.NewString()
	attemptNumber := effectiveAttemptCount + 1

	// 4. CAS: READY → LEASED on tasks + stamp attempt_id + attempt_number.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?, lease_expires_at = ?,
		     attempt_count = ?, attempt_id = ?, attempt_number = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?
		   AND executor_id IN (?, ?) AND executor_version = ?`,
		cmd.WorkerID, cmd.LeaseID, leaseExpiresAt, attemptNumber, attemptID, attemptNumber,
		nowStr, t.ID, cmd.ExpectedTaskRevision,
		execKey.ID, legacyExecutorID, execKey.Version,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker cas: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker rows: %w", err)
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: CAS raced out (revision/executor mismatch or concurrent claim): %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}

	// 5. INSERT PENDING TaskAttempt in the same tx.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, t.ID, t.JobID, attemptNumber, cmd.WorkerID, cmd.LeaseID, nowStr, nowStr,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker insert: %w", err)
	}

	// 6. Read task_spec payload.
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, fmt.Errorf("task claim-for-worker spec read: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker commit: %w", err)
	}

	// Update in-memory fields after successful commit.
	t.WorkerID = cmd.WorkerID
	t.LeaseID = cmd.LeaseID
	t.AttemptCount = attemptNumber
	t.AttemptID = attemptID
	t.AttemptNumber = attemptNumber
	t.Revision++

	tws := &taskgraph.TaskWithSpec{Task: *t}
	if specPayloadJSON.Valid && specPayloadJSON.String != "" && specPayloadJSON.String != "{}" {
		var payload map[string]interface{}
		if json.Unmarshal([]byte(specPayloadJSON.String), &payload) == nil {
			tws.SpecPayload = payload
		}
	}

	att := &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        t.ID,
		JobID:         t.JobID,
		AttemptNumber: attemptNumber,
		WorkerID:      cmd.WorkerID,
		LeaseID:       cmd.LeaseID,
		Status:        taskattempts.AttemptStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	return tws, att, nil
}
