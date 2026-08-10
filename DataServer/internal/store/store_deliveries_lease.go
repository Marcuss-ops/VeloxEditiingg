package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"velox-server/internal/sqliteerr"
	"velox-server/internal/statemachine"
)

// ── Typed lease methods (PR4e) ───────────────────────────────────────────────

// ClaimDeliveries atomically claims up to `batch` claimable deliveries for a
// runner. It matches:
//   - PENDING / RETRY_WAIT with next_attempt_at IS NULL OR <= now
//   - RUNNING with lease_expires_at < now (zombie reclaim)
//
// Each claim sets status=RUNNING, locked_by=runnerID, a DISTINCT lease_id per
// delivery, lease_expires_at=now+lease, attempt_count++, and inserts a
// delivery_attempts audit row — all inside a single tx.
//
// The per-delivery lease_id matters: every subsequent state change
// (Renew/Mark*) is CAS-guarded on (delivery_id, locked_by, lease_id). If the
// whole batch shared one lease_id, a crash mid-batch would let a reclaiming
// runner impersonate the original on every sibling delivery via the shared
// lease. A unique lease per delivery scopes a reclaimed/stolen lease to a
// single row, and lets the runner fail one delivery without affecting the
// others' lease authority.
//
// Returns typed DeliveryLease values for the runner to dispatch.
//
// NOT refactored to TxManager.RunInTx yet: the multi-statement shape
// (UPDATE+RETURNING + per-row sub-SELECT + per-row sub-CAS UPDATE +
// per-row INSERT) would inflate the closure body to ~30 lines and
// obscure the per-row carve-out. Future refactor: extract a
// `claimOneDelivery(tx, claimedRow)` helper and call it from inside
// RunInTx.
func (s *SQLiteStore) ClaimDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]DeliveryLease, error) {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "PENDING", "RUNNING", ""); err != nil {
		return nil, fmt.Errorf("ClaimDeliveries: %w", err)
	}
	if batch <= 0 {
		batch = 1
	}
	operationStarted := time.Now()
	beginStarted := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	waitMS := float64(time.Since(beginStarted).Microseconds()) / 1000
	busy := sqliteerr.IsBusy(err)
	defer func() {
		transactionMS := float64(time.Since(operationStarted).Microseconds()) / 1000
		s.observeDBTransaction(waitMS, transactionMS, busy, busy && transactionMS >= 9000, false, 1, 0)
	}()
	if err != nil {
		return nil, wrapDBInfrastructure("ClaimDeliveries begin", err)
	}
	s.observeDBOperation(true)
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	leaseExpires := now.Add(lease)
	leaseExpiresISO := leaseExpires.Format(time.RFC3339)
	nowISO := now.Format(time.RFC3339)
	// Provisional batch lease_id used only for the atomic status flip; each
	// claimed row is then re-stamped with its own unique lease_id below so
	// no two deliveries in the batch share a lease.
	provisionalLeaseID := fmt.Sprintf("dl_%s_%d_batch", runnerID, now.UnixNano())

	// Atomic claim: flip status='RUNNING' on up to `batch` claimable rows
	// in one UPDATE+RETURNING. The subquery matches:
	//   1. PENDING/RETRY_WAIT where next_attempt_at is NULL or in the past
	//   2. RUNNING where the lease has expired (zombie reclaim)
	rows, err := tx.QueryContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'RUNNING',
		     locked_by = ?,
		     lease_id = ?,
		     lease_expires_at = ?,
			     next_attempt_at = NULL,
			     queued_at = COALESCE(queued_at, COALESCE(next_attempt_at, created_at)),
		     attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE delivery_id IN (
		   SELECT eligible.delivery_id
		   FROM (
		     SELECT jd.delivery_id,
		            a.job_id AS parent_job_id,
		            jd.created_at AS delivery_created_at,				ROW_NUMBER() OVER (
					PARTITION BY a.job_id
					ORDER BY COALESCE(jd.next_attempt_at, jd.created_at) ASC,
					         jd.created_at ASC,
					         jd.delivery_id ASC
				) AS parent_rank,
				COALESCE(jd.next_attempt_at, jd.created_at) AS eligible_at
		     FROM job_deliveries jd
		       JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		       JOIN artifacts a ON a.id = jd.artifact_id
		     WHERE (
		           (jd.status IN ('PENDING', 'RETRY_WAIT')
		            AND (jd.next_attempt_at IS NULL OR jd.next_attempt_at <= ?))
		           OR
		           (jd.status = 'RUNNING'
		            AND jd.lease_expires_at IS NOT NULL
		            AND jd.lease_expires_at < ?)
		         )
		       AND dd.enabled = 1
		       AND a.status = 'READY'
		       AND a.verified_at IS NOT NULL
		   ) AS eligible			ORDER BY eligible.parent_rank ASC,
				         eligible.eligible_at ASC,
				         eligible.parent_job_id ASC,
				         eligible.delivery_created_at ASC,
				         eligible.delivery_id ASC
		   LIMIT ?
		   )		RETURNING delivery_id, artifact_id, destination_id, attempt_count, max_attempts, COALESCE(queued_at, created_at)`,
		runnerID, provisionalLeaseID, leaseExpiresISO, nowISO,
		nowISO, nowISO, batch,
	)
	if err != nil {
		return nil, wrapDBInfrastructure("ClaimDeliveries: UPDATE+RETURNING", err)
	}

	type claimedRow struct {
		deliveryID, artifactID, destID string
		attemptCount                   int
		maxAttempts                    int
		createdAt                      string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.deliveryID, &c.artifactID, &c.destID, &c.attemptCount, &c.maxAttempts, &c.createdAt); err != nil {
			return nil, wrapDBInfrastructure("ClaimDeliveries: scan claimed row", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("ClaimDeliveries: rows iteration", err)
	}
	if len(claimed) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, wrapDBInfrastructure("ClaimDeliveries: commit empty batch", err)
		}
		return nil, nil
	}

	// Hydrate provider/config for each claimed row and insert audit rows.
	out := make([]DeliveryLease, 0, len(claimed))
	for _, c := range claimed {
		var provider, configJSON string
		err := tx.QueryRowContext(ctx,
			`SELECT dd.provider, COALESCE(dd.configuration_json, '')
			 FROM delivery_destinations dd WHERE dd.destination_id = ?`,
			c.destID,
		).Scan(&provider, &configJSON)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrDeliveryNoRow
			}
			return nil, wrapDBInfrastructure("ClaimDeliveries: hydrate destination", err)
		}

		// Re-stamp this delivery with its OWN lease_id, overwriting the
		// provisional batch lease. CAS on (delivery_id, locked_by, lease_id)
		// means the provisionalLeaseID would otherwise be the only value
		// every sibling delivery shares — making a single stolen/crashed
		// lease valid against the whole batch. A unique lease per row
		// isolates that risk. Still inside the claim tx so atomicity holds.
		deliveryLeaseID := "dl_" + uuid.NewString()
		leaseRes, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET lease_id = ?
			 WHERE delivery_id = ?
			   AND locked_by = ?
			   AND lease_id = ?`,
			deliveryLeaseID, c.deliveryID, runnerID, provisionalLeaseID,
		)
		if err != nil {
			return nil, wrapDBInfrastructure("ClaimDeliveries: per-delivery lease stamp", err)
		}
		if n, _ := leaseRes.RowsAffected(); n != 1 {
			return nil, fmt.Errorf("ClaimDeliveries: per-delivery lease stamp affected=%d delivery=%s", n, c.deliveryID)
		}

		// Insert a delivery_attempts row tracking this claim.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts
			 (attempt_number, status, result,
			  started_at, completed_at, error_message, worker_id, delivery_id)
			 VALUES (?, 'in_progress', '{}', ?, NULL, NULL, ?, ?)`,
			c.attemptCount, nowISO, nullIfEmpty(runnerID), c.deliveryID,
		)
		if err != nil {
			return nil, wrapDBInfrastructure("ClaimDeliveries: attempts INSERT", err)
		}

		queuedAt, _ := time.Parse(time.RFC3339Nano, c.createdAt)
		if queuedAt.IsZero() {
			queuedAt, _ = time.Parse(time.RFC3339, c.createdAt)
		}
		out = append(out, DeliveryLease{
			DeliveryID:    c.deliveryID,
			RunnerID:      runnerID,
			LeaseID:       deliveryLeaseID,
			LeaseExpires:  leaseExpires,
			AttemptNumber: c.attemptCount,
			// Phase 5.5: per-delivery retry_budget from
			// job_deliveries.max_attempts (set from
			// job_delivery_plans.retry_budget at INSERT time).
			// The DeliveryRunner overrides its runner-wide
			// MaxAttempts on a per-delivery basis. 0 falls
			// back to the runner default.
			MaxAttempts:   c.maxAttempts,
			Provider:      provider,
			ConfigJSON:    configJSON,
			ArtifactID:    c.artifactID,
			DestinationID: c.destID,
			QueuedAt:      queuedAt,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("ClaimDeliveries commit", err)
	}
	return out, nil
}

// RenewDeliveryLease extends the lease on a RUNNING delivery. The CAS guard
// verifies (delivery_id, status=RUNNING, locked_by, lease_id) to prevent
// stale renewals. Returns ErrTransitionConflict if the guard fails.
//
// NOT refactored to TxManager.RunInTx: a single CAS UPDATE with no
// sub-operations has no need for explicit tx wrapping. RunInTx is
// overkill for the single-statement atomic primitive; the value
// (BeginTx/Commit/Rollback dedup) is exactly zero here.
func (s *SQLiteStore) RenewDeliveryLease(ctx context.Context, deliveryID, runnerID, leaseID string, newExpiry time.Time) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewDeliveryLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		iso, now, deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return wrapDBInfrastructure("RenewDeliveryLease exec", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}
