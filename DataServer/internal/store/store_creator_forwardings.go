// store_creator_forwardings.go is the creator_forwardings compatibility
// facade: the domain model (status vocabulary + row/lease shapes) now lives in
// the canonical internal/forwardingcontract leaf and the SQLite SQL/CAS lives
// in the internal/forwardingstore leaf. Everything below is re-exported so
// existing callers keep the store.CreatorForwarding / store.CFStatus*
// spelling; the sentinel errors are re-exported from internal/storecore via
// db_errors.go. New code should import forwardingcontract / forwardingstore
// directly.
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

import "velox-server/internal/forwardingcontract"

// ── Types (canonical definitions in internal/forwardingcontract) ────────────

// CreatorForwardingStatus is a type alias for the canonical
// forwardingcontract.CreatorForwardingStatus. It exists so existing callers
// importing store.CreatorForwardingStatus continue to compile while the type
// is unified at compile time with the forwarding leaf.
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

// CreatorForwarding is a type alias for the canonical
// forwardingcontract.CreatorForwarding row shape.
type CreatorForwarding = forwardingcontract.CreatorForwarding

// CreatorForwardingLease is a type alias for the canonical
// forwardingcontract.CreatorForwardingLease lease shape.
type CreatorForwardingLease = forwardingcontract.CreatorForwardingLease

// InsertCreatorForwardingResult is a type alias for the canonical
// forwardingcontract.InsertCreatorForwardingResult.
type InsertCreatorForwardingResult = forwardingcontract.InsertCreatorForwardingResult

// ErrCreatorForwardingNoRow / ErrCreatorForwardingOwnershipConflict (the
// shared forwarding sentinels) are re-exported from internal/storecore via
// db_errors.go.
