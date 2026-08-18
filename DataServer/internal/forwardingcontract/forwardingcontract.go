// Package forwardingcontract owns the creator_forwardings domain model — the
// status vocabulary, its terminal semantics, and the typed row/lease/result
// shapes. It is a leaf in the dependency graph (like deliverycontract and
// jobs): it imports nothing from internal/store or internal/forwarding, so the
// business layers (creatorflow, forwarding) and the persistence layer
// (internal/store, and the forwardingstore leaf) can name these types without
// an import cycle.
//
// internal/store re-exports every symbol below as a compatibility facade so
// existing call sites keep the store.CreatorForwarding / store.CFStatus*
// spelling while the canonical definition lives here.
package forwardingcontract

import "time"

// CreatorForwardingStatus is the canonical status enumeration for a
// creator_forwardings row. It is a string-based type so callers can write
// literal status values where the typed constants (below) are not required.
type CreatorForwardingStatus string

const (
	CFStatusPending        CreatorForwardingStatus = "PENDING"
	CFStatusPolling        CreatorForwardingStatus = "POLLING"
	CFStatusReadyToForward CreatorForwardingStatus = "READY_TO_FORWARD"
	CFStatusForwarding     CreatorForwardingStatus = "FORWARDING"
	CFStatusRetryWait      CreatorForwardingStatus = "RETRY_WAIT"
	CFStatusForwarded      CreatorForwardingStatus = "FORWARDED"
	CFStatusFailed         CreatorForwardingStatus = "FAILED"
	CFStatusCancelled      CreatorForwardingStatus = "CANCELLED"
	CFStatusBlocked        CreatorForwardingStatus = "BLOCKED"
)

// IsTerminal returns true for statuses that will never transition again.
func (s CreatorForwardingStatus) IsTerminal() bool {
	return s == CFStatusForwarded || s == CFStatusFailed || s == CFStatusCancelled || s == CFStatusBlocked
}

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

// InsertCreatorForwardingResult is returned by InsertCreatorForwarding to
// distinguish between a new insert (Created=true, Forwarding set) and an
// idempotent duplicate (Created=false, Forwarding returns the existing row
// looked up by the UNIQUE key).
type InsertCreatorForwardingResult struct {
	Created    bool
	Forwarding *CreatorForwarding
}
