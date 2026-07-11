// Package outbox — completeness test.
//
// This file enforces the production-wiring contract declared in
// production.go. Three properties are asserted:
//
//  1. Every event_type emitted in production must register exactly one
//     handler in ProductionRegistry.
//
//  2. ProductionRegistry must NOT register handlers for event_types no
//     current producer emits (stale handlers rot silently otherwise).
//
//  3. Registry.MustRegister's duplicate-detection (panic on collision)
//     already enforces "exactly one" inside a single registry — this
//     test verifies the production registry respects that contract at
//     construction time.
//
// Today: Test #1 fails for "JOB_FAILED" because the handler hasn't
// landed yet. Tests #2 and #3 pass on an empty registry. PR-2 (outbox
// cleanup) will register the JOB_FAILED handler; Test #1 starts
// passing and an additional valid registration keeps Tests #2 + #3
// green.
package outbox_test

import (
	"sort"
	"testing"

	"velox-server/internal/outbox"
)

// TestProductionRegistry_AllKnownEventsHaveHandler asserts the canonical
// contract on the dispatcher architecture:
//
//	for every event_type listed in KnownEventTypes, ProductionRegistry()
//	must register exactly one Handler (Lookup returns non-nil).
//
// The failing message names the offending event_type and the canonical
// remediation so the gap is self-explanatory for operators and future
// maintainers.
func TestProductionRegistry_AllKnownEventsHaveHandler(t *testing.T) {
	known := outbox.KnownEventTypes
	if len(known) == 0 {
		t.Fatal("KnownEventTypes is empty — completeness check is disabled. " +
			"Add the canonical event_types emitted by every EmitOutboxTx caller " +
			"OR delete this assertion and accept the silent breakage. " +
			"See internal/outbox/production.go for the contract.")
	}
	reg := outbox.ProductionRegistry()
	if reg == nil {
		t.Fatal("ProductionRegistry() returned nil; expected a non-nil registry.")
	}
	for _, et := range known {
		if _, err := reg.Lookup(et); err != nil {
			t.Errorf("completeness gap: emitted event_type %q has NO handler in "+
				"ProductionRegistry(). Either register "+
				"outbox.MustRegisterFunc(reg, %q, ...) "+
				"in internal/outbox/production.go or remove the producer that "+
				"emits %q. See internal/outbox/production.go for the canonical "+
				"registration location.", et, et, et)
		}
	}
}

// TestProductionRegistry_NoStaleHandlers asserts the inverse property:
// ProductionRegistry must NOT contain handlers for event_types that no
// current producer emits. Stale handlers rot silently (dispatcher never
// picks them up; production wiring quietly loses auditability).
func TestProductionRegistry_NoStaleHandlers(t *testing.T) {
	active := make(map[string]bool, len(outbox.KnownEventTypes))
	for _, et := range outbox.KnownEventTypes {
		active[et] = true
	}
	reg := outbox.ProductionRegistry()
	var stale []string
	for _, et := range reg.Types() {
		if !active[et] {
			stale = append(stale, et)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("stale handlers: ProductionRegistry registers handlers for "+
			"event_types no current producer emits: %v. Either remove the "+
			"obsolete outbox.MustRegister(...) or add it to KnownEventTypes if "+
			"a producer was just added.", stale)
	}
}

// TestProductionRegistry_ExactlyOneHandlerPerEvent asserts the
// "exactly one" half of the user's specification. Registry.MustRegister
// already panics on duplicate registration, so this test recovers any
// panic from ProductionRegistry and converts it into a clean failure.
//
// In the current empty-registry state the function returns silently,
// so the test passes vacuously; once PR-2 wires handlers, the
// duplicate-detection contract is asserted at the production boundary.
func TestProductionRegistry_ExactlyOneHandlerPerEvent(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ProductionRegistry() panicked during construction — "+
				"likely a duplicate outbox.MustRegister(...) for the same "+
				"event_type: %v", r)
		}
	}()
	_ = outbox.ProductionRegistry()
}
