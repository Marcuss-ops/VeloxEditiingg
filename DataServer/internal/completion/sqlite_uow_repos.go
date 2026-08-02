package completion

// sqlite_uow_repos.go: the small repository implementations of the
// SQLite-backed UnitOfWork — TaskAttemptRepository, TaskRepository,
// JobFinalizationRepository, DeliveryRepository, OutboxRepository.
// Split out of sqlite_uow.go; the factory + wiring live in
// sqlite_uow.go and the attempt_commits repo in
// sqlite_uow_attempt_commit.go.

import (
	"context"
	"fmt"
)

// ────────────────────────────────────────────────────────────────────────
// TaskAttemptRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteTaskAttemptRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceeded transitions task_attempts to SUCCEEDED CAS-gated on
// (attempt_id, worker_id, lease_id) AND status NOT IN terminal.
func (r *sqliteTaskAttemptRepo) MarkSucceeded(ctx context.Context, attemptID, workerID, leaseID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE task_attempts
		    SET status = 'SUCCEEDED', completed_at = COALESCE(completed_at, ?),
		        report_version = report_version + 1, updated_at = ?
		  WHERE id = ? AND worker_id = ? AND lease_id = ?
		    AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
		nowStr, nowStr, attemptID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("completion.TaskAttemptRepository.MarkSucceeded: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// TaskRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteTaskRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceeded transitions tasks to SUCCEEDED + stamps the winning
// attempt metadata for the canonical (task_id, attempt_id, worker_id,
// lease_id) tuple, status IN ('RUNNING','LEASED').
func (r *sqliteTaskRepo) MarkSucceeded(ctx context.Context, taskID, attemptID, workerID, leaseID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE tasks
		    SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		        winning_attempt_id = ?, winning_attempt_committed_at = ?,
		        winning_attempt_terminal_pending = 0, revision = revision + 1
		  WHERE task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
		    AND status IN ('RUNNING','LEASED')`,
		nowStr, nowStr, attemptID, nowStr,
		taskID, attemptID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("completion.TaskRepository.MarkSucceeded: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// JobFinalizationRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteJobRepo struct {
	u *sqliteUnitOfWork
}

// MarkSucceededIfTasksDone flips the job to SUCCEEDED only when every
// sibling task is also SUCCEEDED. 0 rows affected is benign when a
// task is still pending (the CAS guard on NOT EXISTS is the contract).
func (r *sqliteJobRepo) MarkSucceededIfTasksDone(ctx context.Context, jobID, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`UPDATE jobs
		    SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?,
		        revision = revision + 1
		  WHERE job_id = ? AND status IN ('RUNNING','AWAITING_ARTIFACT')
		    AND NOT EXISTS (
		        SELECT 1 FROM tasks t
		         WHERE t.job_id = ? AND t.status != 'SUCCEEDED'
		    )`,
		nowStr, nowStr, jobID, jobID,
	)
	if err != nil {
		return fmt.Errorf("completion.JobFinalizationRepository.MarkSucceededIfTasksDone: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// DeliveryRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteDeliveryRepo struct {
	u *sqliteUnitOfWork
}

// InsertDeliveriesForJob computes the (final-video artifact × destination)
// product and idempotently INSERTs job_deliveries rows. Auxiliary outputs
// such as engine progress sidecars are committed artifacts, but they are
// not publishable media and must never enter the delivery queue.
func (r *sqliteDeliveryRepo) InsertDeliveriesForJob(ctx context.Context, jobID, nowStr string) error {
	rows, err := r.u.tx.QueryContext(ctx,
		`SELECT a.id, dd.destination_id
		   FROM artifacts a
		   CROSS JOIN delivery_destinations dd
		  WHERE a.job_id = ?
		    AND a.status = 'READY'
		    AND (a.output_kind = 'final_video'
		         OR (a.output_kind = '' AND a.type IN ('video', 'final_video')))
		    AND dd.enabled = 1`,
		jobID,
	)
	if err != nil {
		return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob: cross-join: %w", err)
	}
	defer rows.Close()

	type destKey struct{ Art, Dst string }
	seen := make(map[destKey]bool)
	for rows.Next() {
		var art, dst string
		if scanErr := rows.Scan(&art, &dst); scanErr != nil || art == "" || dst == "" {
			continue
		}
		k := destKey{art, dst}
		if seen[k] {
			continue
		}
		seen[k] = true
		id := "jbd_comp_" + art + "_" + dst
		if _, err := r.u.tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO job_deliveries (
			    delivery_id, artifact_id, destination_id, status, idempotency_key,
			    created_at, updated_at
			) VALUES (?, ?, ?, 'PENDING', ?, ?, ?)`,
			id, art, dst, art+"_"+dst, nowStr, nowStr,
		); err != nil {
			return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob INSERT %s:%s: %w", art, dst, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("completion.DeliveryRepository.InsertDeliveriesForJob rows: %w", err)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// OutboxRepository
// ────────────────────────────────────────────────────────────────────────

type sqliteOutboxRepo struct {
	u *sqliteUnitOfWork
}

// InsertEvent idempotently INSERTs an outbox row with the supplied
// event_id (UNIQUE on the primary key absorbs duplicates).
func (r *sqliteOutboxRepo) InsertEvent(ctx context.Context, eventID, aggregateType, aggregateID, eventType, payloadJSON, nowStr string) error {
	_, err := r.u.tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO outbox_events (
		    event_id, aggregate_type, aggregate_id, event_type, payload_json,
		    status, available_at, attempt_count, created_at
		) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, 0, ?)`,
		eventID, aggregateType, aggregateID, eventType, payloadJSON,
		nowStr, nowStr,
	)
	if err != nil {
		return fmt.Errorf("completion.OutboxRepository.InsertEvent: %w", err)
	}
	return nil
}
