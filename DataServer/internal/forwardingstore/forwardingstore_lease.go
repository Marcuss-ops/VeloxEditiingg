package forwardingstore

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/storecore"
)

// ClaimCreatorForwardings atomically claims up to `batch` claimable forwarding
// records for a runner. It matches:
//   - PENDING / RETRY_WAIT where next_attempt_at IS NULL OR <= now
//   - POLLING with lease_expires_at < now (zombie reclaim)
//
// Each claim sets status=POLLING, locked_by=runnerID, a DISTINCT lease_id per
// record, lease_expires_at=now+lease, and attempt_count++ — all inside a
// single transaction.
//
// Returns typed CreatorForwardingLease values for the runner to dispatch.
func (s *SQLiteForwardingStore) ClaimCreatorForwardings(ctx context.Context, runnerID, leaseProvisionalPrefix string, lease time.Duration, batch int) ([]forwardingcontract.CreatorForwardingLease, error) {
	if batch <= 0 {
		batch = 1
	}
	if leaseProvisionalPrefix == "" {
		leaseProvisionalPrefix = "cf"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	leaseExpires := now.Add(lease)
	leaseExpiresISO := leaseExpires.Format(time.RFC3339)
	nowISO := now.Format(time.RFC3339)
	provisionalLeaseID := fmt.Sprintf("%s_%s_%d_batch", leaseProvisionalPrefix, runnerID, now.UnixNano())

	// Atomic claim: flip status='POLLING' on up to `batch` claimable rows.
	rows, err := tx.QueryContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'POLLING',
		     locked_by = ?,
		     lease_id = ?,
		     lease_expires_at = ?,
		     next_attempt_at = '',
		     attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE forwarding_id IN (
		   SELECT forwarding_id FROM creator_forwardings
		   WHERE (
		         (status IN ('PENDING', 'RETRY_WAIT')
		          AND (next_attempt_at = '' OR next_attempt_at IS NULL OR next_attempt_at <= ?))
		         OR
		         (status = 'POLLING'
		          AND lease_expires_at IS NOT NULL
		          AND lease_expires_at <> ''
		          AND lease_expires_at < ?)
		       )
		     ORDER BY created_at ASC
		   LIMIT ?
		 )
		 RETURNING forwarding_id, source_provider, source_job_id,
		           target_executor_id, attempt_count,
		           COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		           COALESCE(intake_source, '')`,
		runnerID, provisionalLeaseID, leaseExpiresISO, nowISO,
		nowISO, nowISO, batch,
	)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings: UPDATE+RETURNING", err)
	}

	type claimedRow struct {
		forwardingID, sourceProvider, sourceJobID, targetExecutorID string
		attemptCount                                                int
		payloadJSON, payloadSHA256                                  string
		intakeSource                                                string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.forwardingID, &c.sourceProvider, &c.sourceJobID,
			&c.targetExecutorID, &c.attemptCount,
			&c.payloadJSON, &c.payloadSHA256,
			&c.intakeSource); err != nil {
			return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings: scan claimed row", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings: rows iteration", err)
	}
	if len(claimed) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings: commit empty batch", err)
		}
		return nil, nil
	}

	// Re-stamp each claimed row with its OWN lease_id.
	out := make([]forwardingcontract.CreatorForwardingLease, 0, len(claimed))
	for _, c := range claimed {
		forwardingLeaseID := "cf_" + uuid.NewString()
		leaseRes, err := tx.ExecContext(ctx,
			`UPDATE creator_forwardings
			 SET lease_id = ?
			 WHERE forwarding_id = ?
			   AND locked_by = ?
			   AND lease_id = ?`,
			forwardingLeaseID, c.forwardingID, runnerID, provisionalLeaseID,
		)
		if err != nil {
			return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings: per-record lease stamp", err)
		}
		n, rowsErr := storecore.ReadRowsAffected(leaseRes, "ClaimCreatorForwardings per-record lease stamp")
		if rowsErr != nil {
			return nil, rowsErr
		}
		if n != 1 {
			return nil, fmt.Errorf("ClaimCreatorForwardings: per-record lease stamp affected=%d forwarding=%s", n, c.forwardingID)
		}

		out = append(out, forwardingcontract.CreatorForwardingLease{
			ForwardingID:     c.forwardingID,
			RunnerID:         runnerID,
			LeaseID:          forwardingLeaseID,
			LeaseExpires:     leaseExpires,
			AttemptCount:     c.attemptCount,
			SourceProvider:   c.sourceProvider,
			SourceJobID:      c.sourceJobID,
			TargetExecutorID: c.targetExecutorID,
			IntakeSource:     c.intakeSource,
			PayloadJSON:      c.payloadJSON,
			PayloadSHA256:    c.payloadSHA256,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimCreatorForwardings commit", err)
	}
	return out, nil
}

// RenewCreatorForwardingLease extends the lease on a POLLING forwarding record.
// CAS guard verifies (forwarding_id, status=POLLING, locked_by, lease_id) to
// prevent stale renewals. Returns ErrTransitionConflict if the guard fails.
func (s *SQLiteForwardingStore) RenewCreatorForwardingLease(ctx context.Context, forwardingID, runnerID, leaseID string, newExpiry time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewCreatorForwardingLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		iso, now, forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("RenewCreatorForwardingLease exec", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(result, "RenewCreatorForwardingLease")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}
