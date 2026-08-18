package store

import (
	"context"
)

// store_creator_forwardings_query.go delegates the creator_forwardings read
// paths to the internal/forwardingstore leaf. The SQL lives in the leaf; this
// file keeps the historical store method names for existing callers.

// GetCreatorForwarding returns a single forwarding by ID, or
// ErrCreatorForwardingNoRow when missing.
func (s *SQLiteStore) GetCreatorForwarding(ctx context.Context, forwardingID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwarding(ctx, forwardingID)
}

// GetCreatorForwardingBySourceForClient returns a forwarding only when the
// unique source tuple is owned by externalClientID.
func (s *SQLiteStore) GetCreatorForwardingBySourceForClient(ctx context.Context, provider, sourceJobID, executorID, externalClientID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingBySourceForClient(ctx, provider, sourceJobID, executorID, externalClientID)
}

// GetCreatorForwardingByIDForClient returns a forwarding only when it is
// owned by externalClientID.
func (s *SQLiteStore) GetCreatorForwardingByIDForClient(ctx context.Context, forwardingID, externalClientID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingByIDForClient(ctx, forwardingID, externalClientID)
}

// GetCreatorForwardingByRemoteJobForClient is the ownership-scoped variant
// of GetCreatorForwardingByRemoteJob used by pipeline-run read/action paths.
func (s *SQLiteStore) GetCreatorForwardingByRemoteJobForClient(ctx context.Context, provider, sourceJobID, externalClientID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingByRemoteJobForClient(ctx, provider, sourceJobID, externalClientID)
}

// GetCreatorForwardingBySource looks up a forwarding by the unique
// (source_provider, source_job_id, target_executor_id) key.
func (s *SQLiteStore) GetCreatorForwardingBySource(ctx context.Context, provider, sourceJobID, executorID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingBySource(ctx, provider, sourceJobID, executorID)
}

// GetCreatorForwardingByRemoteJob finds the durable handoff without requiring
// the caller to know which executor was selected for the remote result.
func (s *SQLiteStore) GetCreatorForwardingByRemoteJob(ctx context.Context, provider, sourceJobID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingByRemoteJob(ctx, provider, sourceJobID)
}

// GetCreatorForwardingByTargetJobID looks up the canonical forwarding
// row stamped with target_job_id = :id.
func (s *SQLiteStore) GetCreatorForwardingByTargetJobID(ctx context.Context, targetJobID, externalClientID string) (*CreatorForwarding, error) {
	return s.forwarding.GetCreatorForwardingByTargetJobID(ctx, targetJobID, externalClientID)
}
