package deliverystore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/sqliteerr"
	"velox-server/internal/statemachine"
	"velox-server/internal/storecore"

	"github.com/google/uuid"
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
func (w *SQLiteDeliveryStore) ClaimDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]DeliveryLease, error) {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "PENDING", "RUNNING", ""); err != nil {
		return nil, fmt.Errorf("ClaimDeliveries: %w", err)
	}
	if batch <= 0 {
		batch = 1
	}
	operationStarted := time.Now()
	beginStarted := time.Now()
	tx, err := w.db.BeginTx(ctx, nil)
	waitMS := float64(time.Since(beginStarted).Microseconds()) / 1000
	busy := sqliteerr.IsBusy(err)
	defer func() {
		transactionMS := float64(time.Since(operationStarted).Microseconds()) / 1000
		w.observeDBTransaction(waitMS, transactionMS, busy, busy && transactionMS >= 9000, false, 1, 0)
	}()
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimDeliveries begin", err)
	}
	w.observeDBOperation(true)
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
				   )		RETURNING delivery_id, artifact_id, publication_id, destination_id, attempt_count, max_attempts, COALESCE(queued_at, created_at)`,
		runnerID, provisionalLeaseID, leaseExpiresISO, nowISO,
		nowISO, nowISO, batch,
	)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: UPDATE+RETURNING", err)
	}

	type claimedRow struct {
		deliveryID, artifactID, publicationID, destID string
		attemptCount                                  int
		maxAttempts                                   int
		createdAt                                     string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.deliveryID, &c.artifactID, &c.publicationID, &c.destID, &c.attemptCount, &c.maxAttempts, &c.createdAt); err != nil {
			return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: scan claimed row", err)
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: rows iteration", err)
	}
	if len(claimed) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: commit empty batch", err)
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
				return nil, storecore.ErrDeliveryNoRow
			}
			return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: hydrate destination", err)
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
			return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: per-delivery lease stamp", err)
		}
		n, err := storecore.ReadRowsAffected(leaseRes, "ClaimDeliveries per-delivery lease stamp")
		if err != nil {
			return nil, err
		}
		if n != 1 {
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
			return nil, storecore.WrapDBInfrastructure("ClaimDeliveries: attempts INSERT", err)
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
			MaxAttempts:   c.maxAttempts,
			Provider:      provider,
			ConfigJSON:    configJSON,
			ArtifactID:    c.artifactID,
			PublicationID: c.publicationID,
			DestinationID: c.destID,
			QueuedAt:      queuedAt,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ClaimDeliveries commit", err)
	}
	return out, nil
}

// RenewDeliveryLease extends the lease on a RUNNING delivery. The CAS guard
// verifies (delivery_id, status=RUNNING, locked_by, lease_id) to prevent
// stale renewals. Returns ErrTransitionConflict if the guard fails.
func (w *SQLiteDeliveryStore) RenewDeliveryLease(ctx context.Context, deliveryID, runnerID, leaseID string, newExpiry time.Time) error {
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewDeliveryLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := nowRFC3339()
	result, err := w.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE delivery_id = ?
		   AND status = 'RUNNING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		iso, now, deliveryID, runnerID, leaseID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("RenewDeliveryLease exec", err)
	}
	affected, err := storecore.ReadRowsAffected(result, "RenewDeliveryLease")
	if err != nil {
		return err
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}
