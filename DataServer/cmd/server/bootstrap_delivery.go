package main

import (
	"velox-server/internal/deliveries"
)

// deliveryDeps carries the delivery-runner reference for the supervisor.
// The actual construction happens in buildModules (which wires
// YouTube + Drive providers), but the type is declared here so the
// supervisor can register it.
type deliveryDeps struct {
	Runner *deliveries.DeliveryRunner
}
