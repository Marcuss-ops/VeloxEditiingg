package store

import (
	"context"
	"fmt"
	"strings"
)

// store_creator_forwardings_query.go owns the creator_forwardings read
// paths: by-ID, by-source (with and without ownership scoping), by-remote-job
// and by-target-job lookups. Write paths live in store_creator_forwardings_write.go.

// GetCreatorForwarding returns a single forwarding by ID, or
// ErrCreatorForwardingNoRow when missing.
func (s *SQLiteStore) GetCreatorForwarding(ctx context.Context, forwardingID string) (*CreatorForwarding, error) {
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
func (s *SQLiteStore) GetCreatorForwardingBySourceForClient(ctx context.Context, provider, sourceJobID, executorID, externalClientID string) (*CreatorForwarding, error) {
	if strings.TrimSpace(provider) == "" || strings.TrimSpace(sourceJobID) == "" || strings.TrimSpace(executorID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, ErrCreatorForwardingNoRow
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
func (s *SQLiteStore) GetCreatorForwardingByIDForClient(ctx context.Context, forwardingID, externalClientID string) (*CreatorForwarding, error) {
	if strings.TrimSpace(forwardingID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, ErrCreatorForwardingNoRow
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
func (s *SQLiteStore) GetCreatorForwardingByRemoteJobForClient(ctx context.Context, provider, sourceJobID, externalClientID string) (*CreatorForwarding, error) {
	if strings.TrimSpace(provider) == "" || strings.TrimSpace(sourceJobID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, ErrCreatorForwardingNoRow
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
func (s *SQLiteStore) GetCreatorForwardingBySource(ctx context.Context, provider, sourceJobID, executorID string) (*CreatorForwarding, error) {
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
func (s *SQLiteStore) GetCreatorForwardingByRemoteJob(ctx context.Context, provider, sourceJobID string) (*CreatorForwarding, error) {
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
//
// Lookup invariants:
//   - target_job_id is populated when status advances past FORWARDING
//     (the AtomicJobTaskCreator block in creatorflow.Resolver writes
//     it in the same tx that creates jobs + tasks + task_specs).
//   - Pre-FORWARDED and FAILED/CANCELLED states leave target_job_id
//     NULL on some classic races; in that case this helper returns
//     ErrCreatorForwardingNoRow, mirroring the storage layer's "no row"
//     contract. Callers can interpret this as "polling target not yet
//     materialized" (recommended: 404 with retry-after hint).
//
// Performance: a B-tree index on target_job_id (migration 102)
// guarantees an O(log N) lookup under operator-scale table growth.
// Without the index, every poll is a full-table scan.
func (s *SQLiteStore) GetCreatorForwardingByTargetJobID(ctx context.Context, targetJobID, externalClientID string) (*CreatorForwarding, error) {
	if strings.TrimSpace(targetJobID) == "" || strings.TrimSpace(externalClientID) == "" {
		return nil, ErrCreatorForwardingNoRow
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
