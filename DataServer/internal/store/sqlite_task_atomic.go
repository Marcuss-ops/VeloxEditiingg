package store

// sqlite_task_atomic.go: §9.5-critical atomic Task + TaskAttempt
// transitions. Every method here opens ONE transaction that performs
// both the Task CAS and the matching TaskAttempt CAS, then either
// commits both or rolls back both. Caller code MUST route §9.5-bound
// transitions exclusively through these methods; the two-statement
// helpers in sqlite_task_crud.go remain available for non-terminal
// idempotent bookkeeping only.
// Extracted from sqlite_task_repository.go (commit f71e2df → next).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

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

	if err := ingestTaskCAS(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := ingestAttemptCAS(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistAttemptVersioning(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistAttemptTracing(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistAttemptMetrics(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistAttemptCacheStats(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistAttemptCostBasis(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistOutputArtifacts(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistSegmentTimings(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistParallelism(ctx, tx, cmd, now); err != nil {
		return err
	}
	if err := persistPartialPhaseMetrics(ctx, tx, cmd); err != nil {
		return err
	}
	if err := persistRawReport(ctx, tx, cmd, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task ingest atomic commit: %w", err)
	}
	committed = true
	return nil
}
