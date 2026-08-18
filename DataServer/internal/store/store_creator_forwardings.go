// store_creator_forwardings.go owns the creator_forwardings row/lease shapes
// and the row scanners. The status vocabulary and its terminal semantics now
// live in the canonical internal/forwardingcontract leaf and are re-exported
// below as a compatibility facade; the sentinel errors are re-exported from
// internal/storecore via db_errors.go. Write paths live in
// store_creator_forwardings_write.go, read paths in
// store_creator_forwardings_query.go, and queue metrics in
// store_creator_forwardings_metrics.go.
//
// Status vocabulary:
//
//	PENDING          — forwarding record created, no runner has claimed it yet.
//	POLLING          — claimed by a runner, actively checking remote status.
//	READY_TO_FORWARD — remote creator has completed; payload ready to enqueue.
//	FORWARDING       — enqueue in progress (short-lived).
//	RETRY_WAIT       — enqueue failed; waiting for backoff before retry.
//	FORWARDED        — Job + Task + TaskSpec created; target_job_id populated.
//	FAILED           — terminal failure after max attempts exhausted.
//	BLOCKED          — operator intervention required (e.g., invalid payload).
//
// Lease design:
//   - locked_by + lease_id + lease_expires_at protect against concurrent runners.
//   - A runner with an expired lease can be preempted by another runner.
//   - RenewLease must be called periodically (leaseDuration/3) during POLLING.
package store

import (
	"database/sql"
	"time"

	"velox-server/internal/forwardingcontract"
)

// ── Types ───────────────────────────────────────────────────────────────

// CreatorForwardingStatus is a type alias for the canonical
// forwardingcontract.CreatorForwardingStatus. It exists so existing callers
// importing store.CreatorForwardingStatus continue to compile while the type
// is unified at compile time with the forwarding leaf. New code should import
// forwardingcontract directly.
type CreatorForwardingStatus = forwardingcontract.CreatorForwardingStatus

const (
	CFStatusPending        = forwardingcontract.CFStatusPending
	CFStatusPolling        = forwardingcontract.CFStatusPolling
	CFStatusReadyToForward = forwardingcontract.CFStatusReadyToForward
	CFStatusForwarding     = forwardingcontract.CFStatusForwarding
	CFStatusRetryWait      = forwardingcontract.CFStatusRetryWait
	CFStatusForwarded      = forwardingcontract.CFStatusForwarded
	CFStatusFailed         = forwardingcontract.CFStatusFailed
	CFStatusCancelled      = forwardingcontract.CFStatusCancelled
	CFStatusBlocked        = forwardingcontract.CFStatusBlocked
)

// CreatorForwarding is the typed view of a creator_forwardings row.
type CreatorForwarding struct {
	ForwardingID     string `json:"forwarding_id"`
	ExternalClientID string `json:"external_client_id,omitempty"`
	SourceProvider   string `json:"source_provider"`
	SourceJobID      string `json:"source_job_id"`
	SourceStatus     string `json:"source_status"`
	TargetExecutorID string `json:"target_executor_id"`
	TargetJobID      string `json:"target_job_id,omitempty"`
	PayloadJSON      string `json:"payload_json"`
	PayloadSHA256    string `json:"payload_sha256"`
	Status           string `json:"status"`
	AttemptCount     int    `json:"attempt_count"`
	NextAttemptAt    string `json:"next_attempt_at,omitempty"`
	PollAttempts     int    `json:"poll_attempts"`
	NextPollAt       string `json:"next_poll_at,omitempty"`
	LastPolledAt     string `json:"last_polled_at,omitempty"`
	LastRemoteStatus string `json:"last_remote_status,omitempty"`
	LockedBy         string `json:"locked_by,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	LastErrorCode    string `json:"last_error_code,omitempty"`
	LastErrorMessage string `json:"last_error_message,omitempty"`
	LastErrorClass   string `json:"last_error_class,omitempty"`
	IntakeSource     string `json:"intake_source,omitempty"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	ForwardedAt      string `json:"forwarded_at,omitempty"`
}

// CreatorForwardingLease is the typed return from ClaimCreatorForwardings.
// Every field is populated by the atomic UPDATE+RETURNING and is required
// by the runner to poll, renew, and complete the forwarding.
type CreatorForwardingLease struct {
	ForwardingID     string
	RunnerID         string
	LeaseID          string
	LeaseExpires     time.Time
	AttemptCount     int
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	IntakeSource     string
	PayloadJSON      string
	PayloadSHA256    string
}

// ErrCreatorForwardingNoRow / ErrCreatorForwardingOwnershipConflict (the
// shared forwarding sentinels) are re-exported from internal/storecore via
// db_errors.go.

type creatorForwardingRowScanner interface {
	Scan(dest ...any) error
}

func scanCreatorForwarding(row creatorForwardingRowScanner) (*CreatorForwarding, error) {
	var cf CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt, &cf.LastRemoteStatus,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, wrapDBInfrastructure("scan creator forwarding", err)
	}
	return &cf, nil
}

// scanCreatorForwardingWithExternalClient is kept separate from the legacy
// scanner because older forwarding queries intentionally select the original
// column set. New ownership-sensitive reads select external_client_id
// explicitly and normalize NULL legacy rows to the empty string.
func scanCreatorForwardingWithExternalClient(row creatorForwardingRowScanner) (*CreatorForwarding, error) {
	var cf CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.ExternalClientID,
		&cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.PollAttempts, &cf.NextPollAt, &cf.LastPolledAt, &cf.LastRemoteStatus,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage, &cf.LastErrorClass,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, wrapDBInfrastructure("scan creator forwarding with client", err)
	}
	return &cf, nil
}

// InsertCreatorForwardingResult is returned by InsertCreatorForwarding to
// distinguish between a new insert (Created=true, Forwarding set) and an
// idempotent duplicate (Created=false, Forwarding returns the existing row
// looked up by the UNIQUE key).
type InsertCreatorForwardingResult struct {
	Created    bool
	Forwarding *CreatorForwarding
}
