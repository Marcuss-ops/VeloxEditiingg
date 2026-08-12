// Package deliverycontract contains the minimal delivery routing contract shared
// by the resolver and artifact finalization packages. Keeping it independent
// prevents an import cycle between those domain packages.
package deliverycontract

import (
	"context"
	"errors"
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
	MaxAttempts   int
}

// DeliveryPlanResolver resolves the explicit targets for one artifact.
type DeliveryPlanResolver interface {
	ResolveDestinations(context.Context, string, string) ([]DeliveryDestination, error)
}
