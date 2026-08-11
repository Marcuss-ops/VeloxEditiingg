// Package store provides the SQLite persistence layer for forwarding state.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// State transition methods for creator_forwardings.
// The CAS and lease guards below are the state-machine boundary; preserve them when editing.

// ── State Transitions ──────────────────────────────────────────────────

// RecordCreatorForwardingPoll updates the poll-tracking fields on a
// creator_forwardings row without changing its status. It is lease-fenced:
// only the current runner/lease pair may record a poll while the lease is
// still valid. A failed CAS returns ErrLeaseLost and leaves next_poll_at
// untouched so a stale runner cannot reschedule the current owner's work.
func (s *SQLiteStore) RecordCreatorForwardingPoll(ctx context.Context, forwardingID, runnerID, leaseID, remoteStatus string, nextPollAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RecordCreatorForwardingPoll: missing required fields")
	}
	now := time.Now().UTC()
	nowISO := now.Format(time.RFC3339)
	nextISO := ""
	if !nextPollAt.IsZero() {
		nextISO = nextPollAt.UTC().Format(time.RFC3339)
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET poll_attempts = poll_attempts + 1,
		     last_polled_at = ?,
		     last_remote_status = ?,
		     next_poll_at = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?
		   AND lease_expires_at > ?`,
		nowISO, remoteStatus, nextISO, nowISO,
		forwardingID, runnerID, leaseID, nowISO,
	)
	if err != nil {
		return wrapDBInfrastructure("RecordCreatorForwardingPoll exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "RecordCreatorForwardingPoll")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return fmt.Errorf("store: RecordCreatorForwardingPoll: %w", ErrLeaseLost)
	}
	return nil
}

// MarkCreatorForwardingReadyToForward transitions a POLLING forwarding to
// READY_TO_FORWARD after the remote creator has completed and the payload
// has been persisted. CAS guard on (forwarding_id, status=POLLING, locked_by,
// lease_id). The runner lease remains attached through the enqueue transaction
// so a stale runner cannot rewrite the row after another runner takes over.
func (s *SQLiteStore) MarkCreatorForwardingReadyToForward(ctx context.Context, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadyToForward: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingReadyToForward begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     last_remote_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?
		   AND lease_expires_at > ?`,
		payloadJSON, payloadSHA256, now,
		forwardingID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingReadyToForward exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingReadyToForward")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingReadyToForward commit", err)
	}
	return nil
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
		return wrapDBInfrastructure("MarkCreatorForwardingForwarding exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingForwarding")
	if rowsErr != nil {
		return rowsErr
	}
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
		return wrapDBInfrastructure("MarkCreatorForwardingForwarded exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingForwarded")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingRetry moves a POLLING forwarding to RETRY_WAIT with
// the next attempt scheduled after a backoff delay. Sets last_error_code,
// last_error_message and last_error_class for diagnostics. CAS on
// (forwarding_id, status=POLLING, locked_by, lease_id).
func (s *SQLiteStore) MarkCreatorForwardingRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingRetry: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingRetry begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?, last_error_class = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?
		   AND lease_expires_at > ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), nullIfEmpty(errorClass), now,
		forwardingID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingRetry exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingRetry")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingRetry commit", err)
	}
	return nil
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
func (s *SQLiteStore) MarkCreatorForwardingFailed(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: empty forwarding_id")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingFailed begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FAILED',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?, last_error_class = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')
		   AND (? = '' OR (locked_by = ? AND lease_id = ? AND lease_expires_at > ?))`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), nullIfEmpty(errorClass), now, forwardingID,
		runnerID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingFailed exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingFailed")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingFailed commit", err)
	}
	return nil
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
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingCancelled: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingCancelled begin", err)
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
		return wrapDBInfrastructure("MarkCreatorForwardingCancelled exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingCancelled")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingCancelled commit", err)
	}
	return nil
}

// MarkCreatorForwardingBlocked moves a leasable forwarding to BLOCKED
// (operator intervention required). When runnerID/leaseID are supplied,
// the CAS also requires an unexpired lease; empty identifiers preserve the
// operator/non-lease repair path.
func (s *SQLiteStore) MarkCreatorForwardingBlocked(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: empty forwarding_id")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingBlocked begin", err)
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
		   AND (? = '' OR (locked_by = ? AND lease_id = ? AND lease_expires_at > ?))`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
		runnerID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingBlocked exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingBlocked")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingBlocked commit", err)
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
	if errors.Is(err, sql.ErrNoRows) {
		return ErrCreatorForwardingNoRow
	}
	if err != nil {
		return wrapDBInfrastructure("EnsureForwarded lookup", err)
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
		return wrapDBInfrastructure("EnsureForwarded exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "EnsureForwarded")
	if rowsErr != nil {
		return rowsErr
	}
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
			if errors.Is(reErr, sql.ErrNoRows) {
				return ErrCreatorForwardingNoRow
			}
			return wrapDBInfrastructure("EnsureForwarded re-SELECT", reErr)
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
