// Package store provides the SQLite persistence layer for forwarding state.
package store

import (
	"context"
	"fmt"
)

// Read-only forwarding queries for recovery and enqueue discovery.

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
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        COALESCE(last_error_class, ''),
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
			&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			return nil, fmt.Errorf("store: ExpiredCreatorForwardingLeases scan: %w", err)
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
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        COALESCE(last_error_class, ''),
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
			&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt,
			&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
			&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
			&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
		); err != nil {
			return nil, fmt.Errorf("store: ListReadyToForward scan: %w", err)
		}
		result = append(result, cf)
	}
	return result, rows.Err()
}
