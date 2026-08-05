package store

// sqlite_task_atomic.go: §9.5-critical atomic Task + TaskAttempt
// transitions. Every method here opens ONE transaction that performs
// both the Task CAS and the matching TaskAttempt CAS, then either
// commits both or rolls back both. Caller code MUST route §9.5-bound
// transitions exclusively through these methods; the two-statement
// helpers in sqlite_task_crud.go remain available for non-terminal
// idempotent bookkeeping only.
// Extracted from sqlite_task_repository.go (commit f71e2df → next).
//
// The file is split by responsibility:
//   - sqlite_task_atomic.go                  → ClaimNextWithAttemptAtomic
//   - sqlite_task_atomic_accept.go           → AcceptTaskAtomic
//   - sqlite_task_atomic_transition.go       → TransitionTaskToTerminalAtomic
//   - sqlite_task_atomic_ingest.go           → IngestTaskResultAtomic

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

	sharedtelemetry "velox-shared/telemetry"
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

	// 5. Resolve the canonical runtime identity for legacy callers that
	// only provide worker_id. The placement path uses ClaimTaskForWorkerAtomic
	// and supplies both IDs directly; this fallback keeps the older atomic
	// method attributable whenever the production session/snapshot tables
	// are available, while preserving minimal historical test fixtures.
	workerSessionID, workerSnapshotID := resolveWorkerRuntimeIdentityTx(ctx, tx, workerID)
	if err := validateWorkerRuntimeIdentityTx(ctx, tx, workerID, workerSessionID, workerSnapshotID); err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt runtime identity: %w", err)
	}

	// INSERT PENDING TaskAttempt in the same tx.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id,
			worker_session_id, worker_snapshot_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, t.ID, t.JobID, attemptNumber, workerID,
		workerSessionID, workerSnapshotID, leaseID, nowStr, nowStr,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-with-attempt insert: %w", err)
	}
	if err := persistMasterExecutionEventTx(ctx, tx, masterExecutionEvent{
		AttemptID: attemptID, JobID: t.JobID, TaskID: t.ID, WorkerID: workerID,
		WorkerSessionID: workerSessionID, SnapshotID: workerSnapshotID, LeaseID: leaseID,
		ExecutorID: t.ExecutorID, ExecutorVersion: t.ExecutorVersion,
		Scope: sharedtelemetry.ScopeTask, Component: "master.placement", Action: "match", Phase: "queue",
		StartedAt: now, CompletedAt: time.Now().UTC(),
	}); err != nil {
		return nil, nil, fmt.Errorf("task claim master telemetry: %w", err)
	}
	if err := persistMasterExecutionEventTx(ctx, tx, masterExecutionEvent{
		AttemptID: attemptID, JobID: t.JobID, TaskID: t.ID, WorkerID: workerID,
		WorkerSessionID: workerSessionID, SnapshotID: workerSnapshotID, LeaseID: leaseID,
		ExecutorID: t.ExecutorID, ExecutorVersion: t.ExecutorVersion,
		Scope: sharedtelemetry.ScopeTask, Component: "master.lease", Action: "issue", Phase: "queue",
		StartedAt: now, CompletedAt: time.Now().UTC(),
	}); err != nil {
		return nil, nil, fmt.Errorf("task claim lease telemetry: %w", err)
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
		ID:               attemptID,
		TaskID:           t.ID,
		JobID:            t.JobID,
		AttemptNumber:    attemptNumber,
		WorkerID:         workerID,
		WorkerSessionID:  workerSessionID,
		WorkerSnapshotID: workerSnapshotID,
		LeaseID:          leaseID,
		Status:           taskattempts.AttemptStatusPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	return tws, att, nil
}
