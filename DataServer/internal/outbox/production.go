// Package outbox — canonical production surface for the registry.
//
// This file declares the contract that links producers (callers of
// EmitOutboxTx / outbox.Store.Insert) to the registry wired in
// cmd/server. The completeness test in completeness_test.go asserts
// every entry in KnownEventTypes has a registered handler in
// ProductionRegistry, and no handler exists for event_types nobody
// emits. Together these properties guarantee the dispatcher's
// "no handler → FAILED" branch is reachable ONLY for events that have
// no real handler — a clear signal in operator logs, not a silent
// drift.
//
// Discipline contract — see completeness_test.go for the assertions:
//
//  1. To add a producer: add the new event_type to KnownEventTypes
//     AND register a handler via MustRegisterFunc in ProductionRegistry.
//     Either alone causes the completeness test to fail.
//
//  2. To remove a producer: drop the event_type from KnownEventTypes
//     AND remove any now-stale MustRegisterFunc. Either alone causes
//     the completeness test to fail (the inverse direction) — the
//     "no stale handler" assertion catches forgotten dead handlers.
//
//  3. DrainLegacyEvents handles historic event_types intentionally
//     drained on every boot. They are NOT in KnownEventTypes because
//     no current producer emits them; the completeness test must NOT
//     flag them as missing.
package outbox

import (
	"context"
)

// KnownEventTypes is the canonical, hand-maintained list of every
// event_type a production producer may emit (via EmitOutboxTx or
// outbox.Store.Insert).
//
// Last manual inventory at PR-2 (outbox completeness test landed):
//
//   • jobs_repository_shared.go:193 (baseJobRepository.Fail)
//     emits "JOB_FAILED".
//
// When you add a producer:
//   - Add the event_type here.
//   - Register a handler via MustRegisterFunc in ProductionRegistry.
//
// When you remove a producer:
//   - Drop the event_type here.
//   - Remove any now-stale MustRegisterFunc (if present).
//
// A "no handler" failure in the test names the exact event_type and
// the file/line that needs editing — see completeness_test.go.
var KnownEventTypes = []string{
	"JOB_FAILED",
}

// ProductionRegistry returns the canonical *Registry that the
// supervisor's OutboxDispatcher is wired to at boot.
//
// Bootstrap (cmd/server/buildAssets) calls this instead of
// outbox.NewRegistry() so the production wiring is auditable from
// one place. The exhaustive contracts in completeness_test.go define
// what "canonical" means in testable terms.
//
// Today's state: empty registry (PR-2 prep). The dispatcher marks
// every emitted event as FAILED via the "no handler → MarkFailed"
// path. PR-2 (outbox cleanup) wires real handlers here.
func ProductionRegistry() *Registry {
	reg := NewRegistry()
	// PR-2 future wiring goes here:
	//
	//   MustRegisterFunc(reg, "JOB_FAILED", func(ctx context.Context, e Event) error {
	//       var p struct {
	//           JobID string `json:"job_id"`
	//       }
	//       if err := ParsePayload(e, &p); err != nil {
	//           return err
	//       }
	//       // ...handler logic...
	//       return nil
	//   })
	//
	// Keep registrations above the comment so the production
	// inventory is greppable from a single location.
	return reg
}

// MustRegisterFunc is a thin convenience for production code that has a
// closure-shaped handler rather than a full struct implementing the
// Handler interface. Mirrors Registry.MustRegister on top of HandlerFunc.
//
// The nil-apply check is unique vs. Registry.Register (which only guards
// against a nil Handler struct, not against a HandlerFunc whose Apply
// closure is nil). The empty-eventType check duplicates Registry.Register
// but yields a friendlier panic message at the production wiring site.
func MustRegisterFunc(r *Registry, eventType string, apply func(ctx context.Context, e Event) error) {
	if eventType == "" {
		panic("outbox.MustRegisterFunc: empty eventType")
	}
	if apply == nil {
		panic("outbox.MustRegisterFunc: nil apply closure")
	}
	r.MustRegister(HandlerFunc{
		Type:  eventType,
		Apply: apply,
	})
}
