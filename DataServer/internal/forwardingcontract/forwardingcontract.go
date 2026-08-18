// Package forwardingcontract owns the creator_forwardings domain model — the
// status vocabulary and its terminal semantics. It is a leaf in the
// dependency graph (like deliverycontract and jobs): it imports nothing from
// internal/store or internal/forwarding, so the business layers
// (creatorflow, forwarding) and the persistence layer (internal/store, and a
// future forwardingstore leaf) can name these types without an import cycle.
//
// internal/store re-exports every symbol below as a compatibility facade so
// existing call sites keep the store.CreatorForwardingStatus /
// store.CFStatus* spelling while the canonical definition lives here.
package forwardingcontract

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
