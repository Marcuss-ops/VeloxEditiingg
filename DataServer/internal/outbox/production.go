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
	"fmt"
	"log"

	"velox-server/internal/alerts"
)

// KnownEventTypes is the canonical, hand-maintained list of every
// event_type a production producer may emit (via EmitOutboxTx or
// outbox.Store.Insert).
//
// Last manual inventory at PR-OUTBOX-HANDLER:
//
//   • jobs_repository_shared.go Fail/FailWithCode emit "JOB_FAILED"
//     with payload { job_id, error_code, error }.
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

// ── Alert sink injection ─────────────────────────────────────────────────
//
// The JOB_FAILED handler in ProductionRegistry constructs an Alert and
// forwards it to the configured Notifier. The notifier is provided by
// the composition root (cmd/server/bootstrap_alerts.go) via
// SetAlertNotifier BEFORE the OutboxDispatcher goroutine starts.
//
// The package-level var is read once per dispatcher cycle (handler
// invocation), so a SetAlertNotifier call after boot requires a
// supervisor restart to take effect. That is intentional: alert
// routing is a hot-reload-sensitive surface (an alert from before
// a sink swap could end up at the new sink), so we keep the
// configuration change explicit.

// defaultAlertNotifier is the safe-zero default. Until SetAlertNotifier
// is called at boot, every JOB_FAILED alert is silently dropped. This
// is preferable to a panic in production code — the dispatcher must
// not crash because alert routing is optional at the wire-format level.
var defaultAlertNotifier alerts.Notifier = alerts.NopNotifier{}

// SetAlertNotifier overrides the production alert sink. Called once
// from the composition root BEFORE the OutboxDispatcher goroutine
// starts. Calling SetAlertNotifier with nil resets to the no-op
// default.
func SetAlertNotifier(n alerts.Notifier) {
	if n == nil {
		defaultAlertNotifier = alerts.NopNotifier{}
		return
	}
	defaultAlertNotifier = n
}

// AlertNotifier returns the currently wired sink. ProductionRegistry
// reads it via this accessor so tests can swap sinks by calling
// SetAlertNotifier without rebuilding the registry.
func AlertNotifier() alerts.Notifier { return defaultAlertNotifier }

// ── ProductionRegistry ────────────────────────────────────────────────────

// ProductionRegistry returns the canonical *Registry that the
// supervisor's OutboxDispatcher is wired to at boot.
//
// Bootstrap (cmd/server/buildAssets) calls this instead of
// outbox.NewRegistry() so the production wiring is auditable from
// one place. The exhaustive contracts in completeness_test.go define
// what "canonical" means in testable terms.
//
// Today: registers a real handler for JOB_FAILED (PR-OUTBOX-HANDLER).
// The handler decodes the canonical payload {job_id, error_code,
// error} and forwards an Alert to the wired Notifier.
func ProductionRegistry() *Registry {
	reg := NewRegistry()

	// PR-OUTBOX-HANDLER: real handler for JOB_FAILED.
	//
	// Payload contract (declarative — actual producer is
	// jobs_repository_shared.go Fail / FailWithCode):
	//
	//   { "job_id":     string (job primary key),
	//     "error_code": string (one of JOB_FAILED_GENERIC, OUTBOX_NOT_WIRED,
	//                          TERMINAL_ALREADY, ... — see FailWithCode callers),
	//     "error":      string (human-readable reason) }
	//
	// Decode failure surfaces as a Permanent HandlerError so the
	// dispatcher's "no retry" path fires (a malformed payload is not
	// a transient condition we want to retry forever). Notification
	// delivery wire-format mismatch (decoder ok, alert wire broken)
	// is logged but reported as nil so a degraded alert path never
	// stalls the dispatch loop.
	MustRegisterFunc(reg, "JOB_FAILED", func(ctx context.Context, e Event) error {
		var p struct {
			JobID     string `json:"job_id"`
			ErrorCode string `json:"error_code"`
			Error     string `json:"error"`
		}
		if err := ParsePayload(e, &p); err != nil {
			// Permanent: malformed payload will not heal on retry.
			return err
		}
		if p.JobID == "" {
			// Permanent: missing required field.
			return Permanent(fmt.Errorf("JOB_FAILED payload missing job_id"))
		}
		alert := alerts.Alert{
			Source:    "outbox.JOB_FAILED",
			Severity:  alerts.SeverityError,
			Subject:   p.JobID,
			Body:      p.Error,
			Tags:      map[string]string{"job_id": p.JobID, "error_code": p.ErrorCode, "event_id": e.EventID},
			Timestamp: e.CreatedAt,
		}
		// Best-effort delivery. Alert path is never authoritative — a
		// transient sink hiccup must not block the dispatcher's
		// "claim next event" loop. We log the error and swallow it
		// so the OutboxDispatcher marks the event PROCESSED even
		// when the alert sink is degraded.
		if err := AlertNotifier().Notify(ctx, alert); err != nil {
			log.Printf("[OUTBOX] alert sink Notify failed for event_id=%s job_id=%s: %v",
				e.EventID, p.JobID, err)
		}
		return nil
	})

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
