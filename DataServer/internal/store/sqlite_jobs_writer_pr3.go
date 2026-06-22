package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/outbox"
)

// ── PR 3 — fully-transactional lifecycle methods ────────────────────────
//
// These primitives exist alongside the legacy methods so callers
// can migrate incrementally. Each opens its own *sql.Tx, performs the
// state change + history INSERT + event INSERT (+ optional outbox INSERT),
// and commits in one shot. A failure at ANY step rolls back the whole
// tx so the canon "events absent if update rolls back / update absent if
// event fails" property holds (PR 3 spec §test-event-rollback).

func (r *SQLiteJobRepository) nowStr(cmdTime time.Time) string {
	if !cmdTime.IsZero() {
		return cmdTime.UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

// insertEventTx writes a job_events row inside an existing transaction.
// Linked to the SQLiteStore.LogJobEvent SQL shape so behaviour matches
// the post-commit path exactly.
func (r *SQLiteJobRepository) insertEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload map[string]interface{}) error {
	raw, err := json.Marshal(map[string]interface{}{
		"event":     eventType,
		"job_id":    jobID,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_events (timestamp, job_id, event, raw_json) VALUES (?, ?, ?, ?)`,
		time.Now().UTC().Format(time.RFC3339), jobID, eventType, string(raw),
	)
	return err
}

func (r *SQLiteJobRepository) insertHistoryTx(ctx context.Context, tx *sql.Tx, jobID, status, workerID, message string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]interface{}{
		"status":    status,
		"timestamp": now,
		"worker_id": workerID,
		"message":   message,
	})
	_, err := tx.ExecContext(ctx,
		`INSERT INTO job_history (job_id, status, worker_id, message, raw_json, event_ts) VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, status, workerID, message, string(raw), now,
	)
	if err != nil {
		// Some migrations used different job_history column sets; fall back
		// to a minimal row that uses the columns we know exist.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO job_history (job_id, status, raw_json, event_ts) VALUES (?, ?, ?, ?)`,
			jobID, status, string(raw), now,
		)
	}
	return err
}

// PR3Start performs the LEASED → RUNNING transition with full CAS tuple,
// plus history INSERT + JOB_STARTED event INSERT in one tx.
func (r *SQLiteJobRepository) PR3Start(ctx context.Context, cmd StartCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("PR3Start: empty jobID")
	}
	if cmd.WorkerID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("PR3Start: missing worker/lease identity")
	}
	now := r.nowStr(cmd.Now)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PR3Start begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: assigned_to + lease_id columns dropped — CAS uses attempt + revision only.
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'RUNNING',
		       started_at = ?,
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = ?
		   AND UPPER(COALESCE(status, '')) = 'LEASED'
		   AND COALESCE(attempt, 0) = ?
		   AND COALESCE(revision, 0) = ?`,
		now, now,
		cmd.JobID, cmd.Attempt, cmd.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("PR3Start UPDATE: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("PR3Start rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("PR3Start %s: %w", cmd.JobID, ErrTransitionConflict)
	}
	if err := r.insertHistoryTx(ctx, tx, cmd.JobID, "RUNNING", cmd.WorkerID, "Job started"); err != nil {
		return fmt.Errorf("PR3Start history: %w", err)
	}
	if err := r.insertEventTx(ctx, tx, cmd.JobID, "job_started", map[string]interface{}{
		"worker_id": cmd.WorkerID,
		"lease_id":  cmd.LeaseID,
		"attempt":   cmd.Attempt,
	}); err != nil {
		return fmt.Errorf("PR3Start event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PR3Start commit: %w", err)
	}
	return nil
}

// PR3RenewLease runs the single-WHERE UPDATE described in the spec and,
// when cmd.EmitEvent is true, the LEASE_RENEWED event INSERT in the same tx.
//
// CAS contract: cmd.ExpectedRevision is honoured. Spec test
// "Renew con revision vecchia → conflict" is driven by callers that
// read the current revision pre-renewal then submit a stale value here.
// SkipRevisionCAS is reserved for one-off migrations that explicitly opt
// in to bypassing the CAS check; the default zero value keeps revision
// CAS active.
func (r *SQLiteJobRepository) PR3RenewLease(ctx context.Context, cmd RenewLeaseCommand) error {
	if cmd.JobID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("PR3RenewLease: missing jobID or leaseID")
	}
	if !cmd.SkipRevisionCAS && cmd.ExpectedRevision < 0 {
		return fmt.Errorf("PR3RenewLease: ExpectedRevision must be non-negative (got %d)", cmd.ExpectedRevision)
	}
	now := r.nowStr(cmd.Now)
	leaseExpiry := cmd.LeaseExpiry.UTC().Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PR3RenewLease begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: assigned_to + lease_id columns dropped — CAS uses revision only.
	// lease_expiry column dropped; lease expiry is tracked in job_attempts.
	query := `UPDATE jobs
		   SET updated_at = ?,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = ?
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING')`
	args := []interface{}{
		now,
		cmd.JobID,
	}
	if !cmd.SkipRevisionCAS {
		query += ` AND COALESCE(revision, 0) = ?`
		args = append(args, cmd.ExpectedRevision)
	}

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("PR3RenewLease UPDATE: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("PR3RenewLease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("PR3RenewLease %s: %w", cmd.JobID, ErrTransitionConflict)
	}
	if cmd.EmitEvent {
		if err := r.insertEventTx(ctx, tx, cmd.JobID, "lease_renewed", map[string]interface{}{
			"worker_id":        cmd.WorkerID,
			"lease_id":         cmd.LeaseID,
			"lease_expiry":     leaseExpiry,
			"lease_expires_at": leaseExpiry,
		}); err != nil {
			return fmt.Errorf("PR3RenewLease event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PR3RenewLease commit: %w", err)
	}
	return nil
}

// PR3RecordRenderFinished moves RUNNING → RENDER_FINISHED with the full
// CAS tuple and writes render_finished history + event in one tx.
// Idempotent: already-RENDER_FINISHED is a no-op.
//
// Revision is read inside the transaction (not from the caller) to avoid
// TOCTOU races: a concurrent LeaseRenewal may bump the revision between
// the caller's lookupJobCASFields read and this CAS UPDATE. By reading
// revision under the same snapshot that gates the UPDATE, we close the
// window entirely.
func (r *SQLiteJobRepository) PR3RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("PR3RecordRenderFinished: empty jobID")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PR3RecordRenderFinished begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotency guard + read current revision inside the tx so the
	// CAS UPDATE uses the latest committed value — no TOCTOU window.
	var current string
	var currentRev int
	if err := tx.QueryRowContext(ctx,
		`SELECT UPPER(COALESCE(status,'')), COALESCE(revision, 0) FROM jobs WHERE job_id = ?`,
		cmd.JobID,
	).Scan(&current, &currentRev); err != nil {
		if err == sql.ErrNoRows {
			if cErr := tx.Commit(); cErr == nil {
				return nil
			}
		}
		return fmt.Errorf("PR3RecordRenderFinished status: %w", err)
	}
	if current == "RENDER_FINISHED" || current == "SUCCEEDED" {
		return tx.Commit()
	}

	now := r.nowStr(cmd.FinishedAt)
	// PR #9: assigned_to + lease_id columns dropped — CAS uses attempt + revision only.
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'RENDER_FINISHED',
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1,
		       started_at = COALESCE(started_at, ?)
		 WHERE job_id = ?
		   AND UPPER(COALESCE(status, '')) = 'RUNNING'
		   AND COALESCE(attempt, 0) = ?
		   AND COALESCE(revision, 0) = ?`,
		now, now,
		cmd.JobID, cmd.AttemptNumber, currentRev,
	)
	if err != nil {
		return fmt.Errorf("PR3RecordRenderFinished UPDATE: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("PR3RecordRenderFinished %s: %w", cmd.JobID, ErrTransitionConflict)
	}
	if err := r.insertHistoryTx(ctx, tx, cmd.JobID, "RENDER_FINISHED", cmd.WorkerID, "Render finished, awaiting artifact"); err != nil {
		return fmt.Errorf("PR3RecordRenderFinished history: %w", err)
	}
	if err := r.insertEventTx(ctx, tx, cmd.JobID, "render_finished", map[string]interface{}{
		"worker_id": cmd.WorkerID,
		"lease_id":  cmd.LeaseID,
		"attempt":   cmd.AttemptNumber,
	}); err != nil {
		return fmt.Errorf("PR3RecordRenderFinished event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PR3RecordRenderFinished commit: %w", err)
	}
	return nil
}

// PR3Fail marks a job FAILED or RETRY_WAIT depending on Retryable and
// retry budget. Single-tx shape: UPDATE jobs, UPDATE job_attempts,
// INSERT history, INSERT event, INSERT outbox.
func (r *SQLiteJobRepository) PR3Fail(ctx context.Context, cmd FailCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("PR3Fail: empty jobID")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PR3Fail begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: retry_count column dropped. Use attempt as proxy for retry count.
	var attemptCount, maxRetries int
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(attempt, 0), COALESCE(max_retries, 0) FROM jobs WHERE job_id = ?`,
		cmd.JobID,
	).Scan(&attemptCount, &maxRetries); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("PR3Fail %s: %w", cmd.JobID, ErrTransitionConflict)
		}
		return fmt.Errorf("PR3Fail retry budget lookup: %w", err)
	}

	now := r.nowStr(cmd.Now)
	nextStatus := JobStatusFailed
	eventType := "job_failed"
	outboxEvent := "JOB_FAILED"
	historyMessage := "Job failed: " + cmd.ErrorMessage
	if cmd.Retryable && attemptCount < maxRetries {
		nextStatus = JobStatusRetryWait
		eventType = "job_retry_scheduled"
		outboxEvent = "JOB_RETRY_SCHEDULED"
		historyMessage = "Job retry scheduled: " + cmd.ErrorMessage
	}

	// PR #9: retry_count, lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = ?,
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1,
		       last_error = ?,
		       error_message = ?,
		       failed_at = ?,
		       failed_by = ?,
		       claimed_at = '',
		       assigned_at = ''
		 WHERE job_id = ?
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING', 'RENDER_FINISHED', 'AWAITING_ARTIFACT')
		   AND COALESCE(revision, 0) = ?`,
		string(nextStatus), now, cmd.ErrorMessage, cmd.ErrorMessage, now, cmd.WorkerID,
		cmd.JobID, cmd.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("PR3Fail UPDATE: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("PR3Fail %s: %w", cmd.JobID, ErrTransitionConflict)
	}

	if err := r.insertHistoryTx(ctx, tx, cmd.JobID, string(nextStatus), cmd.WorkerID, historyMessage); err != nil {
		return fmt.Errorf("PR3Fail history: %w", err)
	}
	if err := r.insertEventTx(ctx, tx, cmd.JobID, eventType, map[string]interface{}{
		"worker_id":  cmd.WorkerID,
		"lease_id":   cmd.LeaseID,
		"attempt":    cmd.AttemptNumber,
		"error_code": cmd.ErrorCode,
		"error":      cmd.ErrorMessage,
		"retryable":  cmd.Retryable,
	}); err != nil {
		return fmt.Errorf("PR3Fail event: %w", err)
	}

	// Update latest job_attempts status to FAILED / FAILED_RETRYABLE.
	// cleanup/remove-job-attempts-runtime: removed the
	// `UPDATE job_attempts SET status = ?` write path. Per-attempt
	// identity now lives on task_attempts (canonical layer; this
	// method's failure path is driven by task completion, not legacy
	// job_attempts). The corresponding task_attempts update is owned
	// by taskingestion.Ingest (TransitionTaskToTerminalAtomic) which
	// has already observed the worker's TaskResult by the time
	// PR3Fail is called.

	payload, _ := json.Marshal(map[string]interface{}{
		"job_id":     cmd.JobID,
		"error":      cmd.ErrorMessage,
		"error_code": cmd.ErrorCode,
		"worker_id":  cmd.WorkerID,
		"attempt":    cmd.AttemptNumber,
		"retryable":  cmd.Retryable,
	})
	if err := r.store.emitOutbox(ctx, tx, outbox.InsertParams{
		AggregateType: "job",
		AggregateID:   cmd.JobID,
		EventType:     outboxEvent,
		Payload:       payload,
	}); err != nil {
		return fmt.Errorf("PR3Fail outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PR3Fail commit: %w", err)
	}
	return nil
}

// PR3Cancel transitions a job to CANCELLED. Idempotent on terminal states.
//
// Spec note: this is a worker-ID optional transition (cancel can be
// initiated by the orchestrator without a worker identity) so the
// WorkerID field is best-effort.
func (r *SQLiteJobRepository) PR3Cancel(ctx context.Context, cmd CancelCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("PR3Cancel: empty jobID")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("PR3Cancel begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotent on terminal states.
	var current string
	if err := tx.QueryRowContext(ctx, `SELECT UPPER(COALESCE(status,'')) FROM jobs WHERE job_id = ?`, cmd.JobID).Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("PR3Cancel %s: not found", cmd.JobID)
		}
		return fmt.Errorf("PR3Cancel status: %w", err)
	}
	switch current {
	case "CANCELLED":
		return tx.Commit()
	case "SUCCEEDED", "FAILED":
		return fmt.Errorf("PR3Cancel %s: cannot cancel terminal job (%s)", cmd.JobID, current)
	}

	now := r.nowStr(cmd.Now)
	// PR #9: lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
	var res sql.Result
	if cmd.WorkerID != "" {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
		   SET status = 'CANCELLED',
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1,
		       claimed_at = '',
		       assigned_at = ''
			 WHERE job_id = ?
			   AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
			   AND COALESCE(revision, 0) = ?`,
			now, cmd.JobID, cmd.ExpectedRevision,
		)
	} else {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
		   SET status = 'CANCELLED',
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1,
		       claimed_at = '',
		       assigned_at = ''
			 WHERE job_id = ?
			   AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			now, cmd.JobID,
		)
	}
	if err != nil {
		return fmt.Errorf("PR3Cancel UPDATE: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("PR3Cancel %s: %w", cmd.JobID, ErrTransitionConflict)
	}
	if err := r.insertHistoryTx(ctx, tx, cmd.JobID, "CANCELLED", cmd.WorkerID, "Cancelled: "+cmd.Reason); err != nil {
		return fmt.Errorf("PR3Cancel history: %w", err)
	}
	if err := r.insertEventTx(ctx, tx, cmd.JobID, "job_cancelled", map[string]interface{}{
		"reason":    cmd.Reason,
		"worker_id": cmd.WorkerID,
	}); err != nil {
		return fmt.Errorf("PR3Cancel event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("PR3Cancel commit: %w", err)
	}
	return nil
}

// PR3RequeueExpiredLeases processes up to `limit` zombie leases in ONE tx
// and returns a per-job RequeueResult slice. Each job's UPDATE + history
// INSERT + event + outbox (only when status transitions to FAILED) live
// in the same transaction; a single failure during the loop rolls back
// all already-processed rows. This guarantees no orphan events / half-
// flipped statuses if the loop crashes mid-flight.
func (r *SQLiteJobRepository) PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	nowStr := now.UTC().Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("PR3RequeueExpiredLeases begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// PR #9: lease_expiry column dropped from jobs. Use job_attempts.started_at
	// as proxy for lease expiry (stale leases = attempts started > 30 min ago).
	cutoff := now.Add(-30 * time.Minute).UTC().Format(time.RFC3339)
	rows, err := tx.QueryContext(ctx,
		`SELECT j.job_id, COALESCE(j.revision, 0), COALESCE(j.attempt, 0),
		        COALESCE(j.max_retries, 0), '',
		        UPPER(COALESCE(j.status, ''))
		 FROM jobs j
		 JOIN job_attempts ja ON ja.job_id = j.job_id
		        AND ja.id = (SELECT id FROM job_attempts WHERE job_id = j.job_id ORDER BY id DESC LIMIT 1)
		 WHERE UPPER(COALESCE(j.status, '')) IN ('LEASED', 'RUNNING')
		   AND ja.started_at IS NOT NULL
		   AND ja.started_at != ''
		   AND ja.started_at < ?
		 ORDER BY ja.started_at ASC
		 LIMIT ?`,
		cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("PR3RequeueExpiredLeases SELECT: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		jobID      string
		revision   int
		attemptCnt int
		maxRetries int
		leaseID    string
		current    JobStatus
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.jobID, &c.revision, &c.attemptCnt, &c.maxRetries, &c.leaseID, &c.current); err != nil {
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("PR3RequeueExpiredLeases rows: %w", err)
	}
	rows.Close()

	var results []RequeueResult
	for _, c := range candidates {
		// Decide next status: retry budget left → PENDING (clean requeue),
		// retry budget exhausted → FAILED.
		willRetry := c.attemptCnt < c.maxRetries
		next := JobStatusPending
		reason := "expired_lease_retry"
		eventType := "lease_expired_requeue"
		if !willRetry {
			next = JobStatusFailed
			reason = "expired_lease_no_retries_left"
			eventType = "lease_expired_failed"
		}

		// PR #9: retry_count, lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
		res, err := tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = ?,
		       claimed_at = '',
		       assigned_at = '',
		       updated_at = ?,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = ?
			   AND UPPER(COALESCE(status, '')) = ?
			   AND COALESCE(revision, 0) = ?`,
			string(next), nowStr, c.jobID, c.current, c.revision,
		)
		if err != nil {
			return nil, fmt.Errorf("PR3RequeueExpiredLeases UPDATE %s: %w", c.jobID, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			// Skipped by a concurrent transition; that is fine, just
			// record it so the operator sees the picture.
			results = append(results, RequeueResult{
				JobID:          c.jobID,
				PreviousStatus: c.current,
				NewStatus:      c.current,
				Reason:         "skipped_concurrent_transition",
				Attempt:        c.attemptCnt,
			})
			continue
		}

		if err := r.insertHistoryTx(ctx, tx, c.jobID, string(next), "", reason); err != nil {
			return nil, fmt.Errorf("PR3RequeueExpiredLeases history: %w", err)
		}
		if err := r.insertEventTx(ctx, tx, c.jobID, eventType, map[string]interface{}{
			"attempt":     c.attemptCnt,
			"max_retries": c.maxRetries,
			"new_status":  string(next),
			"reason":      reason,
		}); err != nil {
			return nil, fmt.Errorf("PR3RequeueExpiredLeases event: %w", err)
		}
		if next == JobStatusFailed {
			payload, _ := json.Marshal(map[string]interface{}{
				"job_id":  c.jobID,
				"reason":  reason,
				"trigger": "lease_expired",
			})
			if err := r.store.emitOutbox(ctx, tx, outbox.InsertParams{
				AggregateType: "job",
				AggregateID:   c.jobID,
				EventType:     "JOB_FAILED",
				Payload:       payload,
			}); err != nil {
				return nil, fmt.Errorf("PR3RequeueExpiredLeases outbox: %w", err)
			}
		}

		results = append(results, RequeueResult{
			JobID:          c.jobID,
			PreviousStatus: c.current,
			NewStatus:      next,
			Reason:         reason,
			Attempt:        c.attemptCnt,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("PR3RequeueExpiredLeases commit: %w", err)
	}
	return results, nil
}
