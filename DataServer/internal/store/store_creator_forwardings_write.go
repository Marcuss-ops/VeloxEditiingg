package store

import (
	"context"
)

// store_creator_forwardings_write.go delegates the creator_forwardings write
// paths (idempotent insert + payload upsert) to the internal/forwardingstore
// leaf. The SQL/CAS lives in the leaf; this file keeps the historical store
// method names for existing callers.

// InsertCreatorForwarding persists a new forwarding record. Idempotent on
// (source_provider, source_job_id, target_executor_id) via INSERT OR IGNORE
// enforced by the UNIQUE index.
func (s *SQLiteStore) InsertCreatorForwarding(ctx context.Context, cf *CreatorForwarding) (*InsertCreatorForwardingResult, error) {
	return s.forwarding.InsertCreatorForwarding(ctx, cf)
}

// UpsertCreatorForwardingPayload updates payload_json and payload_sha256
// on an existing forwarding (typically when the remote creator completes).
func (s *SQLiteStore) UpsertCreatorForwardingPayload(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	return s.forwarding.UpsertCreatorForwardingPayload(ctx, forwardingID, payloadJSON, payloadSHA256)
}
