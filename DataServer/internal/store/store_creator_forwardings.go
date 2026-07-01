// Package store / store_creator_forwardings.go
//
// Typed repository for the creator_forwardings table (migration 055).
// Mirrors the delivery lease pattern (store_deliveries.go +
// store_deliveries_lease.go) so the claim/lease/renew/transition
// vocabulary is familiar to every dev who has already worked on
// the delivery runner.
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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ── Types ───────────────────────────────────────────────────────────────

// CreatorForwardingStatus is the canonical status enumeration for a
// creator_forwardings row. The type alias is string so callers can
// write literal status constants; typed constants (below) are the
// prefered reference in production code.
type CreatorForwardingStatus string

const (
	CFStatusPending        CreatorForwardingStatus = "PENDING"
	CFStatusPolling        CreatorForwardingStatus = "POLLING"
	CFStatusReadyToForward CreatorForwardingStatus = "READY_TO_FORWARD"
	CFStatusForwarding     CreatorForwardingStatus = "FORWARDING"
	CFStatusRetryWait      CreatorForwardingStatus = "RETRY_WAIT"
	CFStatusForwarded      CreatorForwardingStatus = "FORWARDED"
	CFStatusFailed         CreatorForwardingStatus = "FAILED"
	CFStatusBlocked        CreatorForwardingStatus = "BLOCKED"
)

// IsTerminal returns true for statuses that will never transition again.
func (s CreatorForwardingStatus) IsTerminal() bool {
	return s == CFStatusForwarded || s == CFStatusFailed || s == CFStatusBlocked
}

// IsLeasable returns true for statuses a runner can claim.
func (s CreatorForwardingStatus) IsLeasable() bool {
	return s == CFStatusPending || s == CFStatusRetryWait || s == CFStatusPolling
}

// CreatorForwarding is the typed view of a creator_forwardings row.
type CreatorForwarding struct {
	ForwardingID     string `json:"forwarding_id"`
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
	LockedBy         string `json:"locked_by,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`
	LeaseExpiresAt   string `json:"lease_expires_at,omitempty"`
	LastErrorCode    string `json:"last_error_code,omitempty"`
	LastErrorMessage string `json:"last_error_message,omitempty"`
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
	PayloadJSON      string
	PayloadSHA256    string
}

// ErrCreatorForwardingNoRow is returned when a lookup misses.
var ErrCreatorForwardingNoRow = errors.New("store: creator forwarding row not found")

// ── CRUD ────────────────────────────────────────────────────────────────

// InsertCreatorForwarding persists a new forwarding record. Idempotent on
// (source_provider, source_job_id, target_executor_id) via INSERT OR IGNORE
// enforced by the UNIQUE index.
func (s *SQLiteStore) InsertCreatorForwarding(cf *CreatorForwarding) error {
	if cf.ForwardingID == "" || cf.SourceProvider == "" || cf.SourceJobID == "" || cf.TargetExecutorID == "" {
		return fmt.Errorf("store: InsertCreatorForwarding: missing required fields (forwarding_id, source_provider, source_job_id, target_executor_id)")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if cf.CreatedAt == "" {
		cf.CreatedAt = now
	}
	if cf.UpdatedAt == "" {
		cf.UpdatedAt = now
	}
	if cf.Status == "" {
		cf.Status = string(CFStatusPending)
	}

	// Only target_job_id is nullable (TEXT without NOT NULL). All other
	// TEXT columns are NOT NULL DEFAULT '' so they must receive the Go
	// string directly — nullIfEmpty would produce nil (SQL NULL), which
	// violates the NOT NULL constraint on SQLite.
	_, err := s.db.ExecContext(context.Background(),
		`INSERT OR IGNORE INTO creator_forwardings
		 (forwarding_id, source_provider, source_job_id, source_status,
		  target_executor_id, target_job_id, payload_json, payload_sha256,
		  status, attempt_count, next_attempt_at,
		  locked_by, lease_id, lease_expires_at,
		  last_error_code, last_error_message,
		  created_at, updated_at, forwarded_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cf.ForwardingID, cf.SourceProvider, cf.SourceJobID, cf.SourceStatus,
		cf.TargetExecutorID,
		nullIfEmpty(cf.TargetJobID),
		cf.PayloadJSON,
		cf.PayloadSHA256,
		cf.Status, cf.AttemptCount,
		cf.NextAttemptAt,
		cf.LockedBy, cf.LeaseID,
		cf.LeaseExpiresAt,
		cf.LastErrorCode, cf.LastErrorMessage,
		cf.CreatedAt, cf.UpdatedAt,
		cf.ForwardedAt,
	)
	return err
}

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
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings WHERE forwarding_id = ?`, forwardingID)
	var cf CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, fmt.Errorf("store: GetCreatorForwarding: %w", err)
	}
	return &cf, nil
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
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, ''),
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, ''),
		        created_at, updated_at, COALESCE(forwarded_at, '')
		 FROM creator_forwardings
		 WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		provider, sourceJobID, executorID)
	var cf CreatorForwarding
	err := row.Scan(
		&cf.ForwardingID, &cf.SourceProvider, &cf.SourceJobID, &cf.SourceStatus,
		&cf.TargetExecutorID, &cf.TargetJobID,
		&cf.PayloadJSON, &cf.PayloadSHA256,
		&cf.Status, &cf.AttemptCount, &cf.NextAttemptAt,
		&cf.LockedBy, &cf.LeaseID, &cf.LeaseExpiresAt,
		&cf.LastErrorCode, &cf.LastErrorMessage,
		&cf.CreatedAt, &cf.UpdatedAt, &cf.ForwardedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrCreatorForwardingNoRow
	}
	if err != nil {
		return nil, fmt.Errorf("store: GetCreatorForwardingBySource: %w", err)
	}
	return &cf, nil
}

// UpsertCreatorForwardingPayload updates payload_json and payload_sha256
// on an existing forwarding (typically when the remote creator completes).
// CAS guard on forwarding_id + leasable status prevents clobbering a row
// that has already been forwarded or failed.
func (s *SQLiteStore) UpsertCreatorForwardingPayload(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: UpsertCreatorForwardingPayload: empty forwarding_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET payload_json = ?, payload_sha256 = ?, source_status = 'completed',
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING', 'RETRY_WAIT')`,
		payloadJSON, payloadSHA256, now, forwardingID,
	)
	if err != nil {
		return fmt.Errorf("store: UpsertCreatorForwardingPayload: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// defaultForwardingLeaseTTL is the lease TTL written by
// ClaimCreatorForwardings. 5 minutes matches the delivery runner's
// default and is short enough to recover quickly from runner crashes.
const defaultForwardingLeaseTTL = 5 * time.Minute
