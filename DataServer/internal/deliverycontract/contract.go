// Package deliverycontract contains the minimal delivery routing contract shared
// by the resolver and artifact finalization packages. Keeping it independent
// prevents an import cycle between those domain packages.
package deliverycontract

import (
	"context"
	"errors"
	"strings"
)

// ErrNoExplicitPlan identifies a delivery operation without a per-job plan.
var ErrNoExplicitPlan = errors.New("deliveries: no explicit delivery plan")

// ErrResolverNotConfigured identifies a resolver whose durable backing store
// was not wired. It is distinct from ErrNoExplicitPlan: the latter is a valid
// query result for a render-only job, while this error means the resolver
// could not perform the query at all.
var ErrResolverNotConfigured = errors.New("deliveries: plan resolver not configured")

// DeliveryDestination is the finalization projection of one explicit target.
type DeliveryDestination struct {
	DestinationID string
	PublicationID string
	MaxAttempts   int
}

// DeliveryPlanResolver resolves the explicit targets for one artifact.
type DeliveryPlanResolver interface {
	ResolveDestinations(context.Context, string, string) ([]DeliveryDestination, error)
}

// DeliveryStatus is the canonical lifecycle of one delivery attempt. It is
// deliberately separate from JobStatus, ArtifactState, and PublicationStatus:
// a delivery can fail after its artifact is READY without invalidating the
// artifact. It lives in this leaf package (not deliveries) so both the delivery
// runner and the store layer can name the type without the import cycle that
// store → deliveries would introduce.
type DeliveryStatus string

const (
	DeliveryPending     DeliveryStatus = "PENDING"
	DeliveryRunning     DeliveryStatus = "RUNNING"
	DeliveryRetryWait   DeliveryStatus = "RETRY_WAIT"
	DeliverySucceeded   DeliveryStatus = "SUCCEEDED"
	DeliveryFailed      DeliveryStatus = "FAILED"
	DeliveryBlockedAuth DeliveryStatus = "BLOCKED_AUTH"
	DeliveryCancelled   DeliveryStatus = "CANCELLED"
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

// IsTerminal reports whether s is a terminal delivery status (no further
// retries or state transitions).
func (s DeliveryStatus) IsTerminal() bool {
	return s == DeliverySucceeded || s == DeliveryFailed || s == DeliveryBlockedAuth || s == DeliveryCancelled
}

// StatusFromExternal maps an untrusted external delivery observation
// (provider callback or reconciliation poll) to the canonical delivery
// lifecycle status. The input is normalized (trimmed + lower-cased) so casing
// and surrounding whitespace never change the verdict. Unknown observations
// map to DeliveryRunning: a terminal projection is applied only when the
// observation is explicit, so a malformed or mid-flight observation can never
// prematurely terminate a delivery.
//
// This is the single authoritative mapping between the external status
// vocabulary and DeliveryStatus. Both the InstaEdit callback persistence
// (internal/store) and the delivery runner's reconciliation sweep
// (internal/deliveries) route through it rather than maintaining separate
// copies of the same mapping.
func StatusFromExternal(status string) DeliveryStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "published", "publication_completed", "completed":
		return DeliverySucceeded
	case "failed", "dead_letter":
		return DeliveryFailed
	case "blocked_auth":
		return DeliveryBlockedAuth
	case "retry_wait", "rate_limited":
		return DeliveryRetryWait
	case "cancel_requested", "cancelled":
		return DeliveryCancelled
	default:
		return DeliveryRunning
	}
}
