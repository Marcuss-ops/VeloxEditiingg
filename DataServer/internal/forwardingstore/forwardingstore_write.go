package forwardingstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/storecore"
)

// InsertCreatorForwarding persists a new forwarding record. Idempotent on
// (source_provider, source_job_id, target_executor_id) via INSERT OR IGNORE
// enforced by the UNIQUE index.
//
// Returns an InsertCreatorForwardingResult:
//   - Created=true, Forwarding=cf when the row was newly inserted.
//   - Created=false, Forwarding=<existing row> when the UNIQUE key
//     already existed (idempotent duplicate). The existing row is
//     looked up by (source_provider, source_job_id, target_executor_id)
//     and returned so callers always receive the persisted state.
func (s *SQLiteForwardingStore) InsertCreatorForwarding(ctx context.Context, cf *forwardingcontract.CreatorForwarding) (*forwardingcontract.InsertCreatorForwardingResult, error) {
	if cf.ForwardingID == "" || cf.SourceProvider == "" || cf.SourceJobID == "" || cf.TargetExecutorID == "" {
		return nil, fmt.Errorf("store: InsertCreatorForwarding: missing required fields (forwarding_id, source_provider, source_job_id, target_executor_id)")
	}
	now := nowRFC3339()
	if cf.CreatedAt == "" {
		cf.CreatedAt = now
	}
	if cf.UpdatedAt == "" {
		cf.UpdatedAt = now
	}
	if cf.Status == "" {
		cf.Status = string(forwardingcontract.CFStatusPending)
	}

	// Only target_job_id is nullable (TEXT without NOT NULL). All other
	// TEXT columns are NOT NULL DEFAULT '' so they must receive the Go
	// string directly — nullIfEmpty would produce nil (SQL NULL), which
	// violates the NOT NULL constraint on SQLite.
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO creator_forwardings
		 (forwarding_id, external_client_id, source_provider, source_job_id, source_status,
		  target_executor_id, target_job_id, payload_json, payload_sha256,
		  status, attempt_count, next_attempt_at,
		  poll_attempts, next_poll_at, last_polled_at, last_remote_status,
		  locked_by, lease_id, lease_expires_at,
		  last_error_code, last_error_message, last_error_class,
		  intake_source,
		  created_at, updated_at, forwarded_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cf.ForwardingID, nullIfEmpty(cf.ExternalClientID), cf.SourceProvider, cf.SourceJobID, cf.SourceStatus,
		cf.TargetExecutorID,
		nullIfEmpty(cf.TargetJobID),
		cf.PayloadJSON,
		cf.PayloadSHA256,
		cf.Status, cf.AttemptCount,
		cf.NextAttemptAt,
		cf.PollAttempts, cf.NextPollAt, cf.LastPolledAt, cf.LastRemoteStatus,
		cf.LockedBy, cf.LeaseID,
		cf.LeaseExpiresAt,
		cf.LastErrorCode, cf.LastErrorMessage, cf.LastErrorClass,
		cf.IntakeSource,
		cf.CreatedAt, cf.UpdatedAt,
		cf.ForwardedAt,
	)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("InsertCreatorForwarding exec", err)
	}

	affected, err := storecore.ReadRowsAffected(res, "InsertCreatorForwarding rows affected")
	if err != nil {
		return nil, err
	}
	if affected == 1 {
		return &forwardingcontract.InsertCreatorForwardingResult{Created: true, Forwarding: cf}, nil
	}

	// Duplicate — look up the existing row by its UNIQUE key. For M2M
	// submissions, do not return a row owned by another client.
	var existing *forwardingcontract.CreatorForwarding
	if strings.TrimSpace(cf.ExternalClientID) != "" {
		existing, err = s.GetCreatorForwardingBySourceForClient(ctx, cf.SourceProvider, cf.SourceJobID, cf.TargetExecutorID, cf.ExternalClientID)
		if errors.Is(err, storecore.ErrCreatorForwardingNoRow) {
			return nil, storecore.ErrCreatorForwardingOwnershipConflict
		}
	} else {
		existing, err = s.GetCreatorForwardingBySource(ctx, cf.SourceProvider, cf.SourceJobID, cf.TargetExecutorID)
	}
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("InsertCreatorForwarding duplicate lookup", err)
	}
	return &forwardingcontract.InsertCreatorForwardingResult{Created: false, Forwarding: existing}, nil
}

// UpsertCreatorForwardingPayload updates payload_json and payload_sha256
// on an existing forwarding (typically when the remote creator completes).
// CAS guard on forwarding_id + leasable status prevents clobbering a row
// that has already been forwarded or failed.
func (s *SQLiteForwardingStore) UpsertCreatorForwardingPayload(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: UpsertCreatorForwardingPayload: empty forwarding_id")
	}
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET payload_json = ?, payload_sha256 = ?, source_status = 'completed',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT')`,
		payloadJSON, payloadSHA256, now, forwardingID,
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("UpsertCreatorForwardingPayload exec", err)
	}
	n, err := storecore.ReadRowsAffected(result, "UpsertCreatorForwardingPayload rows affected")
	if err != nil {
		return err
	}
	if n == 0 {
		return storecore.ErrTransitionConflict
	}
	return nil
}
