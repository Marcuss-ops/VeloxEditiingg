// Package store / store_creator_forwardings_lease.go
//
// Lease-based claim, renew, and state-transition methods for the
// creator_forwardings table. Mirrors the delivery lease pattern
// (store_deliveries_lease.go) so the runner dispatch loop reuses
// the same mental model.
//
// State transitions enforced here:
//
//	PENDING → POLLING           (ClaimCreatorForwardings)
//	POLLING → READY_TO_FORWARD  (MarkCreatorForwardingReadyToForward)
//	READY_TO_FORWARD → FORWARDING → FORWARDED (MarkCreatorForwardingForwarded)
//	POLLING → RETRY_WAIT        (MarkCreatorForwardingRetry)
//	any leasable → FAILED       (MarkCreatorForwardingFailed)
//	any leasable → BLOCKED      (MarkCreatorForwardingBlocked)
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
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
		attemptCount                                                 int
		payloadJSON, payloadSHA256                                   string
	}
	var claimed []claimedRow
	for rows.Next() {
		var c claimedRow
		if err := rows.Scan(&c.forwardingID, &c.sourceProvider, &c.sourceJobID,
			&c.targetExecutorID, &c.attemptCount,
			&c.payloadJSON, &c.payloadSHA256); err != nil {
			continue
		}
		claimed = append(claimed, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ClaimCreatorForwardings: rows iteration: %w", err)
	}
	if len(claimed) == 0 {
		_ = tx.Commit()
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

// ── Renew ────────────────────────────────────────────────────────────────

// RenewCreatorForwardingLease extends the lease on a POLLING forwarding record.
// CAS guard verifies (forwarding_id, status=POLLING, locked_by, lease_id) to
// prevent stale renewals. Returns ErrTransitionConflict if the guard fails.
func (s *SQLiteStore) RenewCreatorForwardingLease(ctx context.Context, forwardingID, runnerID, leaseID string, newExpiry time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewCreatorForwardingLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
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
		return fmt.Errorf("store: RenewCreatorForwardingLease: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// ── State Transitions ──────────────────────────────────────────────────

// MarkCreatorForwardingReadyToForward transitions a POLLING forwarding to
// READY_TO_FORWARD after the remote creator has completed and the payload
// has been persisted. CAS guard on (forwarding_id, status=POLLING, locked_by,
// lease_id). Releases the lease so another runner can pick up the forwarding
// for the enqueue step.
func (s *SQLiteStore) MarkCreatorForwardingReadyToForward(ctx context.Context, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadyToForward: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingReadyToForward begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		payloadJSON, payloadSHA256, now,
		forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingReadyToForward: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
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
		return fmt.Errorf("store: MarkCreatorForwardingForwarding: %w", err)
	}
	affected, _ := result.RowsAffected()
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
		return fmt.Errorf("store: MarkCreatorForwardingForwarded: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingRetry moves a POLLING forwarding to RETRY_WAIT with
// the next attempt scheduled after a backoff delay. Sets last_error_code
// and last_error_message for diagnostics. CAS on (forwarding_id,
// status=POLLING, locked_by, lease_id).
func (s *SQLiteStore) MarkCreatorForwardingRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingRetry: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingRetry begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingRetry: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingFailed moves a leasable forwarding to FAILED
// (permanent failure, max attempts exhausted). CAS on (forwarding_id,
// status IN leasable states). Clears lock/lease so no zombie claim lingers.
func (s *SQLiteStore) MarkCreatorForwardingFailed(ctx context.Context, forwardingID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingFailed: empty forwarding_id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingFailed begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	result, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FAILED',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingFailed: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// MarkCreatorForwardingBlocked moves a leasable forwarding to BLOCKED
// (operator intervention required). Same semantics as MarkCreatorForwardingFailed
// but the status signals that a human must unblock the record.
func (s *SQLiteStore) MarkCreatorForwardingBlocked(ctx context.Context, forwardingID, errorCode, errorMsg string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingBlocked: empty forwarding_id")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingBlocked begin: %w", err)
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
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT', 'READY_TO_FORWARD', 'FORWARDING')`,
		nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("MarkCreatorForwardingBlocked: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	return tx.Commit()
}

// ── Atomic Enqueue + Forward ───────────────────────────────────────────

// AtomicForwardAndEnqueue combines the Job+Task+TaskSpec creation AND the
// forwarding status update into a single SQLite transaction. This guarantees
// that a crash between the enqueue and the FORWARDED marking cannot leave a
// forwarded Job with the forwarding row still in FORWARDING, or vice versa.
//
// The transaction:
//  1. CAS: READY_TO_FORWARD → FORWARDING (claim the row)
//  2. INSERT Job, Task, TaskSpec (same semantics as CreateJobWithTask)
//  3. CAS: FORWARDING → FORWARDED (set target_job_id = job.ID)
//
// If the initial CAS fails (another runner claimed the row), the
// transaction rolls back and ErrTransitionConflict is returned without
// any side effects.
func (s *SQLiteStore) AtomicForwardAndEnqueue(
	ctx context.Context,
	forwardingID string,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
) error {
	if forwardingID == "" || job == nil || job.ID == "" {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: missing required fields")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. CAS: READY_TO_FORWARD → FORWARDING
	claimResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDING', updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'READY_TO_FORWARD'`,
		now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue claim: %w", err)
	}
	affected, _ := claimResult.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}

	// 2. Insert Job, Task, TaskSpec (same pattern as CreateJobWithTask)
	jobPayload := "{}"
	if job.Payload != "" {
		jobPayload = job.Payload
	}

	req := job.Requirements
	_, err = tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries,
			video_name, project_id,
			created_at, updated_at, migrated_at,
			request_json, result_json, revision,
			run_id, job_run_id,
			job_required_resource_class, job_required_temporal_mode,
			job_required_deterministic, job_required_cacheable,
			job_required_min_bandwidth_mbps
		) VALUES (?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?,
		          ?, ?, ?, ?,
		          ?)`,
		job.ID, job.MaxRetries, job.VideoName, job.ProjectID,
		now, now, now,
		jobPayload,
		job.RunID, job.RunID,
		req.ResourceClass, req.TemporalMode,
		req.Deterministic, req.Cacheable,
		req.MinBandwidthMbps,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue job insert: %w", err)
	}

	taskID := uuid.NewString()
	_, err = tx.ExecContext(ctx,
		`INSERT INTO tasks (
			task_id, job_id, project_id, render_plan_id,
			executor_id, executor_version, status, priority,
			revision, attempt_count, worker_id, lease_id,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, 0, 0, '', '', ?, ?)`,
		taskID, job.ID, job.ProjectID,
		taskSpec.RenderPlanID(),
		taskSpec.ExecutorID, taskSpec.Version,
		priority, now, now,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue task insert: %w", err)
	}

	if taskSpec != nil {
		specHash := taskSpec.MustSpecHash()
		specPayloadJSON, _ := marshalSpecPayload(taskSpec)
		_, err = tx.ExecContext(ctx,
			`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			taskID, taskSpec.Version, specHash, taskSpec.ExecutorID, specPayloadJSON, now,
		)
		if err != nil {
			return fmt.Errorf("AtomicForwardAndEnqueue task spec insert: %w", err)
		}
	}

	// 3. CAS: FORWARDING → FORWARDED
	forwardResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED', target_job_id = ?,
		     forwarded_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'FORWARDING'`,
		job.ID, now, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("AtomicForwardAndEnqueue forward: %w", err)
	}
	affected, _ = forwardResult.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: FORWARDING→FORWARDED CAS failed")
	}

	return tx.Commit()
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
// This is the enqueue-phase analog of MarkCreatorForwardingRetry (which
// handles the POLLING phase). CAS on (forwarding_id, status IN enqueue
// states). Clears lock/lease fields.
func (s *SQLiteStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: empty forwarding_id")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
		     last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('FORWARDING', 'READY_TO_FORWARD')`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// ── Recovery / Sweep ────────────────────────────────────────────────────

// ExpiredCreatorForwardingLeases returns forwarding records whose lease has
// expired (zombie reclaim candidates). SELECT-only — the caller is expected
// to re-claim via ClaimCreatorForwardings or transition via Mark* methods.
func (s *SQLiteStore) ExpiredCreatorForwardingLeases(ctx context.Context, nowRFC3339 string, limit int) ([]CreatorForwarding, error) {
	if nowRFC3339 == "" {
		return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases: nowRFC3339 required")
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id,
		        COALESCE(source_status, ''),
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE status = 'POLLING'
		   AND lease_expires_at IS NOT NULL AND lease_expires_at <> ''
		   AND lease_expires_at < ?
		 ORDER BY lease_expires_at ASC
		 LIMIT ?`,
		nowRFC3339, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases: %w", err)
	}
	defer rows.Close()

	var result []CreatorForwarding
	for rows.Next() {
		var cf CreatorForwarding
		if err := rows.Scan(
			&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
			&cf.TargetExecutorID, &cf.TargetJobID,
			&cf.PayloadJSON, &cf.PayloadSHA256,
			&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			continue
		}
		result = append(result, cf)
	}
	return result, rows.Err()
}

// ListReadyToForward returns forwardings in READY_TO_FORWARD state that are
// ready to be enqueued. These have no lease holder — the forwarding service
// should claim them implicitly via MarkCreatorForwardingForwarding.
func (s *SQLiteStore) ListReadyToForward(ctx context.Context, limit int) ([]CreatorForwarding, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id,
		        COALESCE(source_status, ''),
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE status = 'READY_TO_FORWARD'
		   AND payload_json IS NOT NULL AND payload_json <> ''
		 ORDER BY created_at ASC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: ListReadyToForward: %w", err)
	}
	defer rows.Close()

	var result []CreatorForwarding
	for rows.Next() {
		var cf CreatorForwarding
		if err := rows.Scan(
			&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
			&cf.TargetExecutorID, &cf.TargetJobID,
			&cf.PayloadJSON, &cf.PayloadSHA256,
			&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			continue
		}
		result = append(result, cf)
	}
	return result, rows.Err()
}
