package store

import sharedtelemetry "velox-shared/telemetry"

// executionEventRegistration is retained as a small master-side adapter for
// existing persistence tests and callers. The source of truth is the shared
// TelemetryEventCatalog; no master-local event list is allowed.
type executionEventRegistration struct {
	Origin string
	Scope  string
}

func isRegisteredExecutionEvent(component, action string) bool {
	_, ok := sharedtelemetry.Catalog.Lookup(component, action)
	return ok
}

func canonicalExecutionEventRegistration(component, action string) (executionEventRegistration, bool) {
	spec, ok := sharedtelemetry.Catalog.Lookup(component, action)
	if !ok {
		return executionEventRegistration{}, false
	}
	return executionEventRegistration{Origin: spec.Origin, Scope: spec.Scope}, true
}
