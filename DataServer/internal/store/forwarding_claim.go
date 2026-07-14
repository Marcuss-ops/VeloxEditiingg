// Package store / forwarding_claim.go
//
// Lease-based CLAIM path for the creator_forwardings table. Split out
// of store_creator_forwardings_lease.go as part of the per-concern
// refactor (claim / renew / transitions). The atomic claim is the
// most complex method in the forwarding lease surface — it issues a
// single UPDATE+RETURNING that flips status='POLLING' on up to `batch`
// claimable rows, then re-stamps each claimed row with its OWN lease_id
// in a second pass.
//
// State transitions enforced here:
//
//	PENDING / RETRY_WAIT → POLLING     (ClaimCreatorForwardings, this file)
//	POLLING → READY_TO_FORWARD          (MarkCreatorForwardingReadyToForward, transitions file)
//	READY_TO_FORWARD → FORWARDING → FORWARDED (MarkCreatorForwardingForwarding/Forwarded, transitions file)
//	POLLING → RETRY_WAIT                (MarkCreatorForwardingRetry, transitions file)
//	any leasable → FAILED               (MarkCreatorForwardingFailed, transitions file)
//	any leasable → BLOCKED              (MarkCreatorForwardingBlocked, transitions file)
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ── Claim ────────────────────────────────────────────────────────────────

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
func (s *SQLiteStore) ClaimCreatorForwardings(ctx context.Context, runnerID, leaseProvisionalPrefix string, lease time.Duration, batch int) ([]CreatorForwardingLease, error) {
	if batch <= 0 {
		batch = 1
	}
	if leaseProvisionalPrefix == "" {
		leaseProvisionalPrefix = "cf"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("ClaimCreatorForwardings begin: %w", err)
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
		           COALESCE(payload_json, ''), COALESCE(payload_sha256, '')`,
		runnerID, provisionalLeaseID, leaseExpiresISO, nowISO,
		nowISO, nowISO, batch,
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimCreatorForwardings: UPDATE+RETURNING: %w", err)
	}

	type claimedRow struct {
		forwardingID, sourceProvider, sourceJobID, targetExecutorID string
		attemptCount                                                int
		payloadJSON, payloadSHA256                                  string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.forwardingID, &c.sourceProvider, &c.sourceJobID,
			&c.targetExecutorID, &c.attemptCount,
			&c.payloadJSON, &c.payloadSHA256); err != nil {
			return nil, fmt.Errorf("ClaimCreatorForwardings: scan claimed row: %w", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ClaimCreatorForwardings: rows iteration: %w", err)
	}
	if len(claimed) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("ClaimCreatorForwardings: commit empty batch: %w", err)
		}
		return nil, nil
	}

	// Re-stamp each claimed row with its OWN lease_id.
	out := make([]CreatorForwardingLease, 0, len(claimed))
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
			return nil, fmt.Errorf("ClaimCreatorForwardings: per-record lease stamp: %w", err)
		}
		if n, _ := leaseRes.RowsAffected(); n != 1 {
			return nil, fmt.Errorf("ClaimCreatorForwardings: per-record lease stamp affected=%d forwarding=%s", n, c.forwardingID)
		}

		out = append(out, CreatorForwardingLease{
			ForwardingID:     c.forwardingID,
			RunnerID:         runnerID,
			LeaseID:          forwardingLeaseID,
			LeaseExpires:     leaseExpires,
			AttemptCount:     c.attemptCount,
			SourceProvider:   c.sourceProvider,
			SourceJobID:      c.sourceJobID,
			TargetExecutorID: c.targetExecutorID,
			PayloadJSON:      c.payloadJSON,
			PayloadSHA256:    c.payloadSHA256,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("ClaimCreatorForwardings commit: %w", err)
	}
	return out, nil
}
