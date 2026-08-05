package deliveries

// DeliveryAttemptState is the canonical lifecycle of one delivery attempt.
// It is deliberately separate from ArtifactState: a delivery can fail after
// its artifact is already READY, without invalidating the artifact.
type DeliveryAttemptState string

const (
	DeliveryPending     DeliveryAttemptState = "PENDING"
	DeliveryRunning     DeliveryAttemptState = "RUNNING"
	DeliveryRetryWait   DeliveryAttemptState = "RETRY_WAIT"
	DeliverySucceeded   DeliveryAttemptState = "SUCCEEDED"
	DeliveryFailed      DeliveryAttemptState = "FAILED"
	DeliveryBlockedAuth DeliveryAttemptState = "BLOCKED_AUTH"
	DeliveryCancelled   DeliveryAttemptState = "CANCELLED"
)

func (s DeliveryAttemptState) IsTerminal() bool {
	return s == DeliverySucceeded || s == DeliveryFailed || s == DeliveryBlockedAuth || s == DeliveryCancelled
}

// DeliveryState is the canonical lifecycle of a delivery aggregate.
// Delivery attempts use the same persisted values but remain conceptually
// separate from the artifact they deliver.
type DeliveryState = DeliveryAttemptState

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
