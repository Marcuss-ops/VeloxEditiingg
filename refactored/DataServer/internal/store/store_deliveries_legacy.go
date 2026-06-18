package store

import (
	"context"
	"fmt"
	"time"
)

// ── Legacy methods (kept for backward compat during migration) ───────────────

// ClaimPendingDeliveries is the legacy untyped claim path. Deprecated: use
// ClaimDeliveries instead for typed DeliveryLease returns.
func (s *SQLiteStore) ClaimPendingDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]map[string]interface{}, error) {
	if batch <= 0 {
		batch = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	leaseAt := now.Add(lease).Format(time.RFC3339)
	nowISO := now.Format(time.RFC3339)
	leaseID := fmt.Sprintf("dl_%s_%d", runnerID, now.UnixNano())

	rows, err := tx.QueryContext(ctx,
		`UPDATE job_deliveries
		 SET status = 'RUNNING',
		     locked_by = ?,
		     lease_id = ?,
		     lease_expires_at = ?,
		     next_attempt_at = NULL,
		     attempt_count = attempt_count + 1,
		     updated_at = ?
		 WHERE delivery_id IN (
		   SELECT jd.delivery_id FROM job_deliveries jd
		     JOIN delivery_destinations dd ON dd.destination_id = jd.destination_id
		     JOIN artifacts a ON a.id = jd.artifact_id
		   WHERE jd.status IN ('PENDING', 'RETRY_WAIT')
		     AND (jd.next_attempt_at IS NULL OR jd.next_attempt_at <= ?)
		     AND dd.enabled = 1
		     AND a.status = 'READY'
		     AND a.verified_at IS NOT NULL
		   ORDER BY jd.created_at ASC
		   LIMIT ?
		 )
		 RETURNING delivery_id, artifact_id, destination_id`,
		runnerID, leaseID, leaseAt, nowISO,
		nowISO, batch,
	)
	if err != nil {
		return nil, fmt.Errorf("ClaimPendingDeliveries: UPDATE+RETURNING: %w", err)
	}

	type claimedRow struct {
		deliveryID, artifactID, destID string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.deliveryID, &c.artifactID, &c.destID); err != nil {
			continue
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		_ = tx.Commit()
		return nil, nil
	}

	out := make([]map[string]interface{}, 0, len(claimed))
	for _, c := range claimed {
		var provider, configJSON string
		err := tx.QueryRowContext(ctx,
			`SELECT dd.provider, COALESCE(dd.configuration_json, '')
			 FROM delivery_destinations dd WHERE dd.destination_id = ?`,
			c.destID,
		).Scan(&provider, &configJSON)
		if err != nil {
			return nil, fmt.Errorf("ClaimPendingDeliveries: hydrate destination: %w", err)
		}
		var nextAttempt int64
		row := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(attempt_number), 0) FROM delivery_attempts WHERE delivery_id = ?`,
			c.deliveryID,
		)
		_ = row.Scan(&nextAttempt)
		nextAttempt++

		// delivery_target_id is NULL — FK has been migrated to delivery_id.
		res, err := tx.ExecContext(ctx,
			`INSERT INTO delivery_attempts
			 (delivery_target_id, attempt_number, status, result,
			  started_at, completed_at, error_message, worker_id, delivery_id)
			 VALUES (NULL, ?, 'in_progress', '{}', ?, NULL, NULL, ?, ?)`,
			nextAttempt, nowISO, nullIfEmpty(runnerID), c.deliveryID,
		)
		if err != nil {
			return nil, fmt.Errorf("ClaimPendingDeliveries: attempts INSERT: %w", err)
		}
		attemptID, _ := res.LastInsertId()

		out = append(out, map[string]interface{}{
			"delivery_id":      c.deliveryID,
			"artifact_id":      c.artifactID,
			"destination_id":   c.destID,
			"provider":         provider,
			"configuration":    configJSON,
			"attempt_id":       attemptID,
			"attempt_number":   nextAttempt,
			"lease_expires_at": leaseAt,
			"runner_id":        runnerID,
			"lease_id":         leaseID,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateDeliveryAttempt finalizes the in-flight attempt.
func (s *SQLiteStore) UpdateDeliveryAttempt(ctx context.Context, deliveryID, status, resultURL, errMsg string) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateDeliveryAttempt: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = ?, result = COALESCE(NULLIF(?, ''), result),
		     completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		status, resultURL, now, nullIfEmpty(errMsg), deliveryID, deliveryID,
	)
	return err
}

// UpdateJobDeliveryStatus moves a job_deliveries row to a new status.
func (s *SQLiteStore) UpdateJobDeliveryStatus(ctx context.Context, deliveryID, status string) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateJobDeliveryStatus: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries SET status = ?, updated_at = ? WHERE delivery_id = ?`,
		status, now, deliveryID,
	)
	return err
}

// UpdateJobDeliveryStatusWithBackoff is the retry-aware variant.
func (s *SQLiteStore) UpdateJobDeliveryStatusWithBackoff(ctx context.Context, deliveryID, status string, nextAttemptAt time.Time) error {
	if deliveryID == "" || status == "" {
		return fmt.Errorf("store: UpdateJobDeliveryStatusWithBackoff: missing required fields")
	}
	iso := nextAttemptAt.UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET status = ?, updated_at = ?
		 WHERE delivery_id = ?`,
		status, iso, deliveryID,
	)
	return err
}

// UpdateJobDeliveryRemote stamps remote_id/remote_url on a SUCCEEDED job_delivery.
func (s *SQLiteStore) UpdateJobDeliveryRemote(ctx context.Context, deliveryID, remoteID, remoteURL string) error {
	if deliveryID == "" {
		return fmt.Errorf("store: UpdateJobDeliveryRemote: missing deliveryID")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_deliveries
		 SET remote_id = COALESCE(NULLIF(?, ''), remote_id),
		     remote_url = COALESCE(NULLIF(?, ''), remote_url),
		     status = CASE WHEN ? IN ('SUCCESS', 'SUCCEEDED') THEN 'SUCCEEDED' ELSE status END,
		     updated_at = ?
		 WHERE delivery_id = ?`,
		remoteID, remoteURL, remoteURL, now, deliveryID,
	)
	return err
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
