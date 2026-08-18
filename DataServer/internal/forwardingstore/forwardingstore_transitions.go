package forwardingstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/storecore"
)

// RecordCreatorForwardingPoll updates the poll-tracking fields on a
// creator_forwardings row without changing its status. It is lease-fenced:
// only the current runner/lease pair may record a poll while the lease is
// still valid. A failed CAS returns ErrLeaseLost and leaves next_poll_at
// untouched so a stale runner cannot reschedule the current owner's work.
func (s *SQLiteForwardingStore) RecordCreatorForwardingPoll(ctx context.Context, forwardingID, runnerID, leaseID, remoteStatus string, nextPollAt time.Time) error {
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
		return storecore.WrapDBInfrastructure("RecordCreatorForwardingPoll exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "RecordCreatorForwardingPoll")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return fmt.Errorf("store: RecordCreatorForwardingPoll: %w", storecore.ErrLeaseLost)
	}
	return nil
}

// MarkCreatorForwardingReadyToForward transitions a POLLING forwarding to
// READY_TO_FORWARD after the remote creator has completed and the payload
// has been persisted. CAS guard on (forwarding_id, status=POLLING, locked_by,
// lease_id). The runner lease remains attached through the enqueue transaction
// so a stale runner cannot rewrite the row after another runner takes over.
func (s *SQLiteForwardingStore) MarkCreatorForwardingReadyToForward(ctx context.Context, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadyToForward: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingReadyToForward begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()
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
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingReadyToForward exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingReadyToForward")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingReadyToForward commit", err)
	}
	return nil
}

// MarkCreatorForwardingForwarding transitions a READY_TO_FORWARD forwarding
// to FORWARDING (short-lived enqueue gate). CAS on (forwarding_id,
// status=READY_TO_FORWARD). By this point the forwarding has no lease holder.
func (s *SQLiteForwardingStore) MarkCreatorForwardingForwarding(ctx context.Context, forwardingID string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingForwarding: empty forwarding_id")
	}
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDING', updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'READY_TO_FORWARD'`,
		now, forwardingID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingForwarding exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingForwarding")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingForwarded marks a FORWARDING record as FORWARDED
// and stamps target_job_id. This is the terminal success state.
// CAS on (forwarding_id, status=FORWARDING).
func (s *SQLiteForwardingStore) MarkCreatorForwardingForwarded(ctx context.Context, forwardingID, targetJobID string) error {
	if forwardingID == "" || targetJobID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingForwarded: missing required fields")
	}
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED', target_job_id = ?,
		     forwarded_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'FORWARDING'`,
		targetJobID, now, now, forwardingID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingForwarded exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingForwarded")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingRetry moves a POLLING forwarding to RETRY_WAIT with
// the next attempt scheduled after a backoff delay. Sets last_error_code,
// last_error_message and last_error_class for diagnostics. CAS on
// (forwarding_id, status=POLLING, locked_by, lease_id).
func (s *SQLiteForwardingStore) MarkCreatorForwardingRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingRetry: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingRetry begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()
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
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingRetry exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingRetry")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingRetry commit", err)
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
func (s *SQLiteForwardingStore) MarkCreatorForwardingFailed(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: empty forwarding_id")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingFailed begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()
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
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingFailed exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingFailed")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingFailed commit", err)
	}
	return nil
}

// MarkCreatorForwardingCancelled moves a leasable forwarding to CANCELLED
// (operator or client initiated cancellation). Same full-CAS semantics as
// MarkCreatorForwardingFailed: (forwarding_id, status, locked_by, lease_id).
// When the caller is not a lease holder (e.g. the row is in PENDING with
// no lock), pass empty strings for runnerID and leaseID.
func (s *SQLiteForwardingStore) MarkCreatorForwardingCancelled(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingCancelled: empty forwarding_id")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingCancelled: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingCancelled begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()
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
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingCancelled exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingCancelled")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingCancelled commit", err)
	}
	return nil
}

// MarkCreatorForwardingCancelledForClient cancels a leasable forwarding on
// the client-initiated path, scoped by external_client_id so a caller can
// only cancel its own rows. Unlike MarkCreatorForwardingCancelled there is
// no lease CAS: a client does not hold a runner lease, so the guard is the
// (forwarding_id, external_client_id) pair plus the non-terminal status
// window. A miss (wrong client, terminal status, or unknown id) returns
// storecore.ErrCreatorForwardingNoRow.
func (s *SQLiteForwardingStore) MarkCreatorForwardingCancelledForClient(ctx context.Context, forwardingID, clientID, errorCode, errorMsg string) error {
	if forwardingID == "" || strings.TrimSpace(clientID) == "" {
		return storecore.ErrCreatorForwardingNoRow
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'CANCELLED', locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?, updated_at = ?
		 WHERE forwarding_id = ? AND external_client_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), nowRFC3339(), forwardingID, strings.TrimSpace(clientID))
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingCancelledForClient exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingCancelledForClient")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrCreatorForwardingNoRow
	}
	return nil
}

// MarkCreatorForwardingBlocked moves a leasable forwarding to BLOCKED
// (operator intervention required). When runnerID/leaseID are supplied,
// the CAS also requires an unexpired lease; empty identifiers preserve the
// operator/non-lease repair path.
func (s *SQLiteForwardingStore) MarkCreatorForwardingBlocked(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: empty forwarding_id")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingBlocked begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()
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
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingBlocked exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingBlocked")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingBlocked commit", err)
	}
	return nil
}

// EnsureForwarded is the repair-path idempotency primitive. It stamps
// (status='FORWARDED', target_job_id=jobID) on a forwarding row that is
// in any non-terminal state (PENDING, POLLING, RETRY_WAIT, READY_TO_FORWARD,
// FORWARDING). This repairs the "Job exists but forwarding row is stuck"
// desync that occurs when a crash interrupts the AtomicForwardAndEnqueue
// transaction after the Job INSERT but before the FORWARDING→FORWARDED CAS.
func (s *SQLiteForwardingStore) EnsureForwarded(ctx context.Context, forwardingID, jobID string) error {
	if forwardingID == "" || jobID == "" {
		return fmt.Errorf("store: EnsureForwarded: missing required fields")
	}

	now := nowRFC3339()

	// First, check if already FORWARDED with the same job.
	var existingJobID string
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(target_job_id, '') FROM creator_forwardings WHERE forwarding_id = ?`,
		forwardingID,
	).Scan(&existingJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return storecore.ErrCreatorForwardingNoRow
	}
	if err != nil {
		return storecore.WrapDBInfrastructure("EnsureForwarded lookup", err)
	}
	if existingJobID == jobID {
		// Already forwarded to the same job — idempotent no-op.
		return nil
	}
	if existingJobID != "" {
		// Already forwarded to a DIFFERENT job — divergent, refuse.
		return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s already has target_job_id=%s, requested=%s",
			storecore.ErrTransitionConflict, forwardingID, existingJobID, jobID)
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
		return storecore.WrapDBInfrastructure("EnsureForwarded exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "EnsureForwarded")
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
				return storecore.ErrCreatorForwardingNoRow
			}
			return storecore.WrapDBInfrastructure("EnsureForwarded re-SELECT", reErr)
		}
		if finalStatus == "FORWARDED" && finalJobID == jobID {
			// Idempotent success via race: another caller completed the
			// same repair between our SELECT and UPDATE. Return nil.
			return nil
		}
		if finalStatus == "FORWARDED" && finalJobID != "" && finalJobID != jobID {
			return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s already FORWARDED with target_job_id=%s, requested=%s (race lost)",
				storecore.ErrTransitionConflict, forwardingID, finalJobID, jobID)
		}
		return fmt.Errorf("store: EnsureForwarded: %w: forwarding %s is in terminal state %s",
			storecore.ErrTransitionConflict, forwardingID, finalStatus)
	}
	return nil
}
