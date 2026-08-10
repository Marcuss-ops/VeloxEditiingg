package deliveries

// DeliveryStatus is the canonical lifecycle of one delivery attempt.
// It is deliberately separate from JobStatus, ArtifactState, and
// PublicationStatus: a delivery can fail after its artifact is READY,
// without invalidating the artifact.
type DeliveryStatus string

// DeliveryAttemptState is retained as a source-compatible alias.
type DeliveryAttemptState = DeliveryStatus

const (
	DeliveryPending     DeliveryAttemptState = "PENDING"
	DeliveryRunning     DeliveryAttemptState = "RUNNING"
	DeliveryRetryWait   DeliveryAttemptState = "RETRY_WAIT"
	DeliverySucceeded   DeliveryAttemptState = "SUCCEEDED"
	DeliveryFailed      DeliveryAttemptState = "FAILED"
	DeliveryBlockedAuth DeliveryAttemptState = "BLOCKED_AUTH"
	DeliveryCancelled   DeliveryAttemptState = "CANCELLED"
)

// Valid reports whether s is a known persisted delivery status.
func (s DeliveryStatus) Valid() bool {
	switch s {
	case DeliveryPending, DeliveryRunning, DeliveryRetryWait, DeliverySucceeded, DeliveryFailed, DeliveryBlockedAuth, DeliveryCancelled:
		return true
	default:
		return false
	}
}

func (s DeliveryStatus) IsTerminal() bool {
	return s == DeliverySucceeded || s == DeliveryFailed || s == DeliveryBlockedAuth || s == DeliveryCancelled
}

// DeliveryState is the canonical lifecycle of a delivery aggregate.
// Delivery attempts use the same persisted values but remain conceptually
// separate from the artifact they deliver.
type DeliveryState = DeliveryStatus

// Status is retained as a source-compatible alias for existing store-facing
// callers. New delivery code should use DeliveryState.
type Status = DeliveryState

const (
	StatusPending     = DeliveryPending
	StatusRunning     = DeliveryRunning
	StatusRetryWait   = DeliveryRetryWait
	StatusSucceeded   = DeliverySucceeded
	StatusFailed      = DeliveryFailed
	StatusBlockedAuth = DeliveryBlockedAuth
	StatusCancelled   = DeliveryCancelled
)
