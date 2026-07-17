// Package store / forwarding_transitions.go
//
// State TRANSITION methods for the creator_forwardings table. Split out
// of store_creator_forwardings_lease.go as part of the per-concern
// refactor (claim / renew / transitions). All 12 transition methods
// live here because they share the same CAS-guard / lease-release
// pattern and the same set of state-machine invariants documented at
// the top of the original file.
//
// State transitions enforced here:
//
//	POLLING → READY_TO_FORWARD  (MarkCreatorForwardingReadyToForward)
//	READY_TO_FORWARD → FORWARDING → FORWARDED (MarkCreatorForwardingForwarding + MarkCreatorForwardingForwarded)
//	POLLING → RETRY_WAIT        (MarkCreatorForwardingRetry)
//	any leasable → FAILED       (MarkCreatorForwardingFailed)
//	any leasable → BLOCKED      (MarkCreatorForwardingBlocked)
//	AtomicForwardAndEnqueue     — READY_TO_FORWARD → FORWARDING → FORWARDED + Job+Task+TaskSpec in one tx
//	MarkCreatorForwardingReadySync      — sync handler path: PENDING/POLLING → READY_TO_FORWARD (no lease CAS)
//	MarkCreatorForwardingEnqueueRetry   — enqueue-phase retry: FORWARDING/READY_TO_FORWARD → RETRY_WAIT
//	EnsureForwarded            — repair path: any non-terminal → FORWARDED (idempotent)
//	ExpiredCreatorForwardingLeases    — SELECT-only scan for zombie-reclaim candidates
//	ListReadyToForward         — SELECT-only scan for enqueue candidates
package store

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// ── State Transitions ──────────────────────────────────────────────────

// MarkCreatorForwardingReadyToForward transitions a POLLING forwarding to
// READY_TO_FORWARD after the remote creator has completed and the payload
// has been persisted. CAS guard on (forwarding_id, status=POLLING, locked_by,
// lease_id). Releases the lease so another runner can pick up the forwarding
// for the enqueue step.
func (s *SQLiteStore) MarkCreatorForwardingReadyToForward(ctx context.Context, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadyToForward: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingReadyToForward begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		payloadJSON, payloadSHA256, now,
		forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingReadyToForward: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingForwarding transitions a READY_TO_FORWARD forwarding
// to FORWARDING (short-lived enqueue gate). CAS on (forwarding_id,
// status=READY_TO_FORWARD). By this point the forwarding has no lease holder.
func (s *SQLiteStore) MarkCreatorForwardingForwarding(ctx context.Context, forwardingID string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingForwarding: empty forwarding_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDING', updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'READY_TO_FORWARD'`,
		now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: MarkCreatorForwardingForwarding: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingForwarded marks a FORWARDING record as FORWARDED
// and stamps target_job_id. This is the terminal success state.
// CAS on (forwarding_id, status=FORWARDING).
func (s *SQLiteStore) MarkCreatorForwardingForwarded(ctx context.Context, forwardingID, targetJobID string) error {
	if forwardingID == "" || targetJobID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingForwarded: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED', target_job_id = ?,
		     forwarded_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'FORWARDING'`,
		targetJobID, now, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: MarkCreatorForwardingForwarded: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingRetry moves a POLLING forwarding to RETRY_WAIT with
// the next attempt scheduled after a backoff delay. Sets last_error_code
// and last_error_message for diagnostics. CAS on (forwarding_id,
// status=POLLING, locked_by, lease_id).
func (s *SQLiteStore) MarkCreatorForwardingRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingRetry: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingRetry begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingRetry: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingFailed moves a leasable forwarding to FAILED
// (permanent failure, max attempts exhausted). Full CAS on (forwarding_id,
// status IN leasable states, locked_by, lease_id) — only the current lease
// holder may declare terminal failure, preventing a race where a preempted
// runner overwrites a row that another runner has already claimed.
//
// When the caller is not a lease holder (e.g. the row is in RETRY_WAIT with
// no lock), pass empty strings for runnerID and leaseID — the CAS degrades
// to forwarding_id + status only.
func (s *SQLiteStore) MarkCreatorForwardingFailed(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: empty forwarding_id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingFailed begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FAILED',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')
		   AND (? = '' OR locked_by = ?)
		   AND (? = '' OR lease_id = ?)`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
		runnerID, runnerID,
		leaseID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingFailed: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingCancelled moves a leasable forwarding to CANCELLED
// (operator or client initiated cancellation). Same full-CAS semantics as
// MarkCreatorForwardingFailed: (forwarding_id, status, locked_by, lease_id).
// When the caller is not a lease holder (e.g. the row is in PENDING with
// no lock), pass empty strings for runnerID and leaseID.
func (s *SQLiteStore) MarkCreatorForwardingCancelled(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingCancelled: empty forwarding_id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingCancelled begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'CANCELLED',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')
		   AND (? = '' OR locked_by = ?)
		   AND (? = '' OR lease_id = ?)`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
		runnerID, runnerID,
		leaseID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingCancelled: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingBlocked moves a leasable forwarding to BLOCKED
// (operator intervention required). Same full-CAS semantics as
// MarkCreatorForwardingFailed: (forwarding_id, status, locked_by, lease_id).
func (s *SQLiteStore) MarkCreatorForwardingBlocked(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: empty forwarding_id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingBlocked begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'BLOCKED',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')
		   AND (? = '' OR locked_by = ?)
		   AND (? = '' OR lease_id = ?)`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
		runnerID, runnerID,
		leaseID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingBlocked: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// ── Atomic Enqueue + Forward ───────────────────────────────────────────

// AtomicForwardAndEnqueue combines the Job+Task+TaskSpec creation AND the
// forwarding status update into a single SQLite transaction. This guarantees
// that a crash between the enqueue and the FORWARDED marking cannot leave a
// forwarded Job with the forwarding row still in FORWARDING, or vice versa.
//
// The transaction:
//  1. CAS: READY_TO_FORWARD → FORWARDING (claim the row)
//  2. INSERT Job, Task, TaskSpec (same semantics as CreateJobWithTask)
//  3. CAS: FORWARDING → FORWARDED (set target_job_id = job.ID)
//
// If the initial CAS fails (another runner claimed the row), the
// transaction rolls back and ErrTransitionConflict is returned without
// any side effects.
func (s *SQLiteStore) AtomicForwardAndEnqueue(
	ctx context.Context,
	forwardingID string,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
) error {
	if forwardingID == "" || job == nil || job.ID == "" {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. CAS: READY_TO_FORWARD → FORWARDING
	claimResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDING', updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'READY_TO_FORWARD'`,
		now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue claim: %w", err)
	}
	affected, _ := claimResult.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// 2. Delegate Job+Task+TaskSpec creation to the canonical single-writer
	//    path (CreateJobWithTaskTx) so the SQL lives in exactly one place.
	creator := NewAtomicJobTaskCreator(s)
	if err := creator.CreateJobWithTaskTx(ctx, tx, job, taskSpec, priority); err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue create job+task: %w", err)
	}

	// 3. CAS: FORWARDING → FORWARDED
	forwardResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED', target_job_id = ?,
		     forwarded_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'FORWARDING'`,
		job.ID, now, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue forward: %w", err)
	}
	affected, _ = forwardResult.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: FORWARDING→FORWARDED CAS failed")
	}

	return tx.Commit()
}

// MarkCreatorForwardingReadySync transitions a PENDING/POLLING forwarding to
// READY_TO_FORWARD WITHOUT a (locked_by, lease_id) CAS. This is the
// synchronous handler path: the HTTP request INSERTed a fresh PENDING row
// (no lease) and immediately needs to promote it for the atomic enqueue step.
//
// Diff vs MarkCreatorForwardingReadyToForward: the latter is the legitimate
// runner lease-holder promotion (CAS on qualifier+lease_id pair). The sync
// path has no lease — using a CAS that requires one would never match. So
// the sync method uses a relaxed guard: forwarding_id + status in
// (PENDING, POLLING) only. Safe because the sync caller just INSERTed the
// row in the same logical operation (no other runner can have claimed it
// yet: PENDING = claimable, POLLING = lock/unlikely-immediately-after-insert).
//
// Returns ErrTransitionConflict if the row is not in a promotable state
// (already READY_TO_FORWARD, FORWARDED, FAILED, BLOCKED, etc.).
func (s *SQLiteStore) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadySync: empty forwarding_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING')`,
		payloadJSON, payloadSHA256, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: MarkCreatorForwardingReadySync: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
// This is the enqueue-phase analog of MarkCreatorForwardingRetry (which
// handles the POLLING phase). CAS on (forwarding_id, status IN enqueue
// states). Clears lock/lease fields.
func (s *SQLiteStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: empty forwarding_id")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('FORWARDING', 'READY_TO_FORWARD')`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// ── Idempotent Repair ──────────────────────────────────────────────────

// EnsureForwarded is the repair-path idempotency primitive. It stamps
// (status='FORWARDED', target_job_id=jobID) on a forwarding row that is
// in any non-terminal state (PENDING, POLLING, RETRY_WAIT, READY_TO_FORWARD,
// FORWARDING). This repairs the "Job exists but forwarding row is stuck"
// desync that occurs when a crash interrupts the AtomicForwardAndEnqueue
// transaction after the Job INSERT but before the FORWARDING→FORWARDED CAS.
//
// Semantics:
//   - If the row is already FORWARDED with the same target_job_id → nil (no-op).
//   - If the row is already FORWARDED with a different target_job_id →
//     ErrTransitionConflict (divergent forwarding, operator intervention).
//   - If the row is in FAILED or BLOCKED → ErrTransitionConflict (terminal,
//     cannot repair).
//   - If the row is in any leasable state → stamp FORWARDED + target_job_id.
//
// This method is the concrete implementation of the ForwardingRepository
// interface method declared in creatorflow/resolver.go. The resolver calls
// it from the idempotency fast-path (Job already exists) to repair the
// forwarding row so it matches the Job state.
func (s *SQLiteStore) EnsureForwarded(ctx context.Context, forwardingID, jobID string) error {
	if forwardingID == "" || jobID == "" {
		return fmt.Errorf("store: EnsureForwarded: missing required fields")
	}

	now := time.Now().UTC().Format(time.RFC3339)

	// First, check if already FORWARDED with the same job.
	var existingJobID string
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(target_job_id, '') FROM creator_forwardings WHERE forwarding_id = ?`,
		forwardingID,
	).Scan(&existingJobID)
	if err != nil {
		return fmt.Errorf("store: EnsureForwarded lookup: %w", err)
	}
	if existingJobID == jobID {
		// Already forwarded to the same job — idempotent no-op.
		return nil
	}
	if existingJobID != "" {
		// Already forwarded to a DIFFERENT job — divergent, refuse.
		return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s already has target_job_id=%s, requested=%s",
			ErrTransitionConflict, forwardingID, existingJobID, jobID)
	}

	// Stamp FORWARDED on any non-terminal state.
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED',
		     target_job_id = ?,
		     forwarded_at = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status NOT IN ('FORWARDED', 'FAILED', 'CANCELLED', 'BLOCKED')`,
		jobID, now, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: EnsureForwarded: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		// Re-SELECT to distinguish "another caller won the race and
		// stamped FORWARDED with a different job_id" from "row is in
		// FAILED/BLOCKED". The error message must be precise so the
		// operator can diagnose the root cause.
		var finalStatus, finalJobID string
		if reErr := s.db.QueryRowContext(ctx,
			`SELECT status, COALESCE(target_job_id, '') FROM creator_forwardings WHERE forwarding_id = ?`,
			forwardingID,
		).Scan(&finalStatus, &finalJobID); reErr != nil {
			return fmt.Errorf("store: EnsureForwarded: %w: re-SELECT failed for forwarding %s: %v",
				ErrTransitionConflict, forwardingID, reErr)
		}
		if finalStatus == "FORWARDED" && finalJobID == jobID {
			// Idempotent success via race: another caller completed the
			// same repair between our SELECT and UPDATE. Return nil.
			return nil
		}
		if finalStatus == "FORWARDED" && finalJobID != "" && finalJobID != jobID {
			return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s already FORWARDED with target_job_id=%s, requested=%s (race lost)",
				ErrTransitionConflict, forwardingID, finalJobID, jobID)
		}
		return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s is in terminal state %s",
			ErrTransitionConflict, forwardingID, finalStatus)
	}
	return nil
}

// ── Recovery / Sweep ────────────────────────────────────────────────────

// ExpiredCreatorForwardingLeases returns forwarding records whose lease has
// expired (zombie reclaim candidates). SELECT-only — the caller is expected
// to re-claim via ClaimCreatorForwardings or transition via Mark* methods.
func (s *SQLiteStore) ExpiredCreatorForwardingLeases(ctx context.Context, nowRFC3339 string, limit int) ([]CreatorForwarding, error) {
	if nowRFC3339 == "" {
		return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases: nowRFC3339 required")
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id,
		        COALESCE(source_status, ''),
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE status = 'POLLING'
		   AND lease_expires_at IS NOT NULL AND lease_expires_at <> ''
		   AND lease_expires_at < ?
		 ORDER BY lease_expires_at ASC
		 LIMIT ?`,
		nowRFC3339, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases: %w", err)
	}
	defer rows.Close()

	var result []CreatorForwarding
	for rows.Next() {
		var cf CreatorForwarding
		if err := rows.Scan(
			&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
			&cf.TargetExecutorID, &cf.TargetJobID,
			&cf.PayloadJSON, &cf.PayloadSHA256,
			&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
			&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases scan: %w", err)
		}
		result = append(result, cf)
	}
	return result, rows.Err()
}

// ListReadyToForward returns forwardings in READY_TO_FORWARD state that are
// ready to be enqueued. These have no lease holder — the forwarding service
// should claim them implicitly via MarkCreatorForwardingForwarding.
func (s *SQLiteStore) ListReadyToForward(ctx context.Context, limit int) ([]CreatorForwarding, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id,
		        COALESCE(source_status, ''),
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE status = 'READY_TO_FORWARD'
		   AND payload_json IS NOT NULL AND payload_json <> ''
		 ORDER BY created_at ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: ListReadyToForward: %w", err)
	}
	defer rows.Close()

	var result []CreatorForwarding
	for rows.Next() {
		var cf CreatorForwarding
		if err := rows.Scan(
			&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
			&cf.TargetExecutorID, &cf.TargetJobID,
			&cf.PayloadJSON, &cf.PayloadSHA256,
			&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
			&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			return nil, fmt.Errorf("store: ListReadyToForward scan: %w", err)
		}
		result = append(result, cf)
	}
	return result, rows.Err()
}
