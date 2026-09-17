package forwardingstore

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/storecore"
)

// GetCreatorForwarding returns a single forwarding by ID, or
// ErrCreatorForwardingNoRow when missing.
func (s *SQLiteForwardingStore) GetCreatorForwarding(ctx context.Context, forwardingID string) (*forwardingcontract.CreatorForwarding, error) {
	if forwardingID == "" {
		return nil, fmt.Errorf("store: GetCreatorForwarding: empty forwarding_id")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings WHERE forwarding_id = ?`, forwardingID)
	return scanCreatorForwarding(row)
}

// GetCreatorForwardingBySourceForClient returns a forwarding only when the
// unique source tuple is owned by externalClientID. A mismatch is hidden as
// ErrCreatorForwardingNoRow.
func (s *SQLiteForwardingStore) GetCreatorForwardingBySourceForClient(ctx context.Context, provider, sourceJobID, executorID, externalClientID string) (*forwardingcontract.CreatorForwarding, error) {
	if strings.TrimSpace(provider) == "" || strings.TrimSpace(sourceJobID) == "" || strings.TrimSpace(executorID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	query := `SELECT forwarding_id, COALESCE(external_client_id, ''), source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''), COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ? AND external_client_id = ?
		 LIMIT 1`
	return scanCreatorForwardingWithExternalClient(s.db.QueryRowContext(ctx, query, provider, sourceJobID, executorID, externalClientID))
}

// GetCreatorForwardingByIDForClient returns a forwarding only when it is
// owned by externalClientID. A missing row and a row owned by another client
// intentionally return the same ErrCreatorForwardingNoRow result so callers
// can expose an indistinguishable 404.
func (s *SQLiteForwardingStore) GetCreatorForwardingByIDForClient(ctx context.Context, forwardingID, externalClientID string) (*forwardingcontract.CreatorForwarding, error) {
	if strings.TrimSpace(forwardingID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	query := `SELECT forwarding_id, COALESCE(external_client_id, ''),
		        source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE forwarding_id = ? AND external_client_id = ?
		 LIMIT 1`
	return scanCreatorForwardingWithExternalClient(s.db.QueryRowContext(ctx, query, forwardingID, externalClientID))
}

// GetCreatorForwardingByRemoteJobForClient is the ownership-scoped variant
// of GetCreatorForwardingByRemoteJob used by pipeline-run read/action paths.
// It preserves the same indistinguishable-miss contract as the ID lookup.
func (s *SQLiteForwardingStore) GetCreatorForwardingByRemoteJobForClient(ctx context.Context, provider, sourceJobID, externalClientID string) (*forwardingcontract.CreatorForwarding, error) {
	if strings.TrimSpace(provider) == "" || strings.TrimSpace(sourceJobID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, storecore.ErrCreatorForwardingNoRow
	}
	query := `SELECT forwarding_id, COALESCE(external_client_id, ''),
		        source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE source_provider = ? AND source_job_id = ? AND external_client_id = ?
		 ORDER BY created_at DESC LIMIT 1`
	return scanCreatorForwardingWithExternalClient(s.db.QueryRowContext(ctx, query, provider, sourceJobID, externalClientID))
}

// GetCreatorForwardingBySource looks up a forwarding by the unique
// (source_provider, source_job_id, target_executor_id) key.
func (s *SQLiteForwardingStore) GetCreatorForwardingBySource(ctx context.Context, provider, sourceJobID, executorID string) (*forwardingcontract.CreatorForwarding, error) {
	if provider == "" || sourceJobID == "" || executorID == "" {
		return nil, fmt.Errorf("store: GetCreatorForwardingBySource: missing required fields")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		provider, sourceJobID, executorID)
	return scanCreatorForwarding(row)
}

// GetCreatorForwardingByRemoteJob finds the durable handoff without requiring
// the caller to know which executor was selected for the remote result.
func (s *SQLiteForwardingStore) GetCreatorForwardingByRemoteJob(ctx context.Context, provider, sourceJobID string) (*forwardingcontract.CreatorForwarding, error) {
	if provider == "" || sourceJobID == "" {
		return nil, fmt.Errorf("store: GetCreatorForwardingByRemoteJob: missing required fields")
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT forwarding_id, source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE source_provider = ? AND source_job_id = ?
		 ORDER BY created_at DESC LIMIT 1`, provider, sourceJobID)
	return scanCreatorForwarding(row)
}

// GetCreatorForwardingByTargetJobID looks up the canonical forwarding
// row stamped with target_job_id = :id. When externalClientID is non-empty,
// the lookup is ownership-scoped; a missing row and a row owned by another
// client both return ErrCreatorForwardingNoRow so callers can expose the
// same 404 envelope without an existence oracle. This is the read surface
// for the polling endpoint GET /api/v1/jobs/:id — by the time the resolver
// commits a request, target_job_id is populated and stable.
func (s *SQLiteForwardingStore) GetCreatorForwardingByTargetJobID(ctx context.Context, targetJobID, externalClientID string) (*forwardingcontract.CreatorForwarding, error) {
	if strings.TrimSpace(targetJobID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, storecore.ErrCreatorForwardingNoRow
	}

	query := `SELECT forwarding_id, COALESCE(external_client_id, ''),
		        source_provider, source_job_id, source_status,
		        target_executor_id, COALESCE(target_job_id, ''),
		        COALESCE(payload_json, ''), COALESCE(payload_sha256, ''),
		        status, attempt_count, COALESCE(next_attempt_at, ''),
		        poll_attempts, COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''),
		        COALESCE(last_remote_status, ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''), COALESCE(last_error_class, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE target_job_id = ?`
	args := []any{targetJobID, strings.TrimSpace(externalClientID)}
	query += ` AND external_client_id = ? ORDER BY created_at DESC LIMIT 1`

	return scanCreatorForwardingWithExternalClient(s.db.QueryRowContext(ctx, query, args...))
}

// ExpiredCreatorForwardingLeases returns forwarding records whose lease has
// expired (zombie reclaim candidates). SELECT-only — the caller is expected
// to re-claim via ClaimCreatorForwardings or transition via Mark* methods.
func (s *SQLiteForwardingStore) ExpiredCreatorForwardingLeases(ctx context.Context, nowRFC3339 string, limit int) ([]forwardingcontract.CreatorForwarding, error) {
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
		return nil, storecore.WrapDBInfrastructure("ExpiredCreatorForwardingLeases query", err)
	}
	defer rows.Close()

	var result []forwardingcontract.CreatorForwarding
	for rows.Next() {
		var cf forwardingcontract.CreatorForwarding
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
			return nil, storecore.WrapDBInfrastructure("ExpiredCreatorForwardingLeases scan", err)
		}
		result = append(result, cf)
	}
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ExpiredCreatorForwardingLeases rows", err)
	}
	return result, nil
}

// ListReadyToForward returns forwardings in READY_TO_FORWARD state that are
// ready to be enqueued. These have no lease holder — the forwarding service
// should claim them implicitly via MarkCreatorForwardingForwarding.
func (s *SQLiteForwardingStore) ListReadyToForward(ctx context.Context, limit int) ([]forwardingcontract.CreatorForwarding, error) {
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
		return nil, storecore.WrapDBInfrastructure("ListReadyToForward query", err)
	}
	defer rows.Close()

	var result []forwardingcontract.CreatorForwarding
	for rows.Next() {
		var cf forwardingcontract.CreatorForwarding
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
			return nil, storecore.WrapDBInfrastructure("ListReadyToForward scan", err)
		}
		result = append(result, cf)
	}
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ListReadyToForward rows", err)
	}
	return result, nil
}
