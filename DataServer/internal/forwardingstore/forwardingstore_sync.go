package forwardingstore

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/storecore"
)

// MarkCreatorForwardingReadySync transitions a PENDING/POLLING forwarding to
// READY_TO_FORWARD WITHOUT a (locked_by, lease_id) CAS. This is the
// synchronous handler path: the HTTP request INSERTed a fresh PENDING row
// (no lease) and immediately needs to promote it for the atomic enqueue step.
//
// Diff vs MarkCreatorForwardingReadyToForward: the latter is the legitimate
// runner lease-holder promotion (CAS on qualifier+lease_id pair). The sync
// path has no lease — using a CAS that requires one would never match. The
// sync method therefore matches the forwarding ID, a promotable status, and
// the absence of ownership fields. If a runner claims the row between INSERT
// and promotion, the sync path gets ErrTransitionConflict and cannot clear or
// overwrite the runner's ownership.
func (s *SQLiteForwardingStore) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadySync: empty forwarding_id")
	}
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING')
		   AND COALESCE(locked_by, '') = ''
		   AND COALESCE(lease_id, '') = ''
		   AND COALESCE(lease_expires_at, '') = ''`,
		payloadJSON, payloadSHA256, now, forwardingID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingReadySync exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingReadySync")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
// This is the enqueue-phase analog of MarkCreatorForwardingRetry (which
// handles the POLLING phase). The transition is owned by the active lease;
// a stale runner must not be able to rewrite a row claimed by another runner.
func (s *SQLiteForwardingStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: missing forwarding_id, runner_id or lease_id")
	}

	now := nowRFC3339()
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('FORWARDING', 'READY_TO_FORWARD')
		   AND locked_by = ?
		   AND lease_id = ?
		   AND lease_expires_at > ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID, runnerID, leaseID, now,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("MarkCreatorForwardingEnqueueRetry exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "MarkCreatorForwardingEnqueueRetry")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}
