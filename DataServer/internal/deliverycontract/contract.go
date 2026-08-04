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

// DeliveryDestination is the finalization projection of one explicit target.
type DeliveryDestination struct {
	DestinationID string
	MaxAttempts   int
}

// DeliveryPlanResolver resolves the explicit targets for one artifact.
type DeliveryPlanResolver interface {
	ResolveDestinations(context.Context, string, string) ([]DeliveryDestination, error)
}
