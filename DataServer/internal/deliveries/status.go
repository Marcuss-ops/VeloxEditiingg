package deliveries

import "velox-server/internal/deliverycontract"

// The delivery lifecycle status now lives in the leaf deliverycontract
// package so both the delivery runner and the store layer can name the type
// without the import cycle that store → deliveries would introduce. The
// aliases below preserve the deliveries.* source surface for existing
// callers (including statusboundary) while the canonical definition lives in
// deliverycontract.

type DeliveryStatus = deliverycontract.DeliveryStatus

// DeliveryAttemptState is retained as a source-compatible alias.
type DeliveryAttemptState = deliverycontract.DeliveryStatus

const (
	DeliveryPending     = deliverycontract.DeliveryPending
	DeliveryRunning     = deliverycontract.DeliveryRunning
	DeliveryRetryWait   = deliverycontract.DeliveryRetryWait
	DeliverySucceeded   = deliverycontract.DeliverySucceeded
	DeliveryFailed      = deliverycontract.DeliveryFailed
	DeliveryBlockedAuth = deliverycontract.DeliveryBlockedAuth
	DeliveryCancelled   = deliverycontract.DeliveryCancelled
)

// DeliveryState is the canonical lifecycle of a delivery aggregate.
// Delivery attempts use the same persisted values but remain conceptually
// separate from the artifact they deliver.
type DeliveryState = deliverycontract.DeliveryStatus

// Status is retained as a source-compatible alias for existing store-facing
// callers. New delivery code should use DeliveryState.
type Status = deliverycontract.DeliveryStatus

const (
	StatusPending     = deliverycontract.DeliveryPending
	StatusRunning     = deliverycontract.DeliveryRunning
	StatusRetryWait   = deliverycontract.DeliveryRetryWait
	StatusSucceeded   = deliverycontract.DeliverySucceeded
	StatusFailed      = deliverycontract.DeliveryFailed
	StatusBlockedAuth = deliverycontract.DeliveryBlockedAuth
	StatusCancelled   = deliverycontract.DeliveryCancelled
)
