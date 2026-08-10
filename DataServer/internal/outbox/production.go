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
	"sync"

	"velox-server/internal/alerts"
)

// KnownEventTypes is the canonical, hand-maintained list of every
// event_type a production producer may emit (via EmitOutboxTx or
// outbox.Store.Insert).
//
// Scope: events registered INSIDE outbox.ProductionRegistry() (the
// package-internal completion exhaustiveness check in
// completeness_test.go). Subsystem-owned handlers registered via
// RegisterHandlerFactory from other packages (e.g. workers emitting
// WORKER_BUNDLE_REBUILD_REQUESTED) are NOT listed here — they are
// validated by their owning package's tests, against the canonical
// production registry at runtime.
//
// Last manual inventory at PR-OUTBOX-HANDLER:
//
//   - jobs_repository_shared.go Fail/FailWithCode emit "JOB_FAILED"
//     with payload { job_id, error_code, error }.
//
// When you add an in-package producer:
//   - Add the event_type here.
//   - Register a handler in buildProductionRegistry via
//     MustRegisterFunc (just above the factory-iteration section).
//
// When you add a subsystem producer:
//   - Define the event_type constant + Handler struct in the
//     subsystem package.
//   - In the subsystem package init(), call
//     outbox.RegisterHandlerFactory(func(reg) reg.MustRegister(handler)).
//   - Add a CompletenessKeystone test in that package that wires
//     outbox.ProductionRegistry() in an isolated test process and
//     asserts the handler is present (see workers/bundle_rebuild_outbox_test.go
//     for the factory-presence assertion pattern — opt-in via a
//     dedicated test in the subsystem package).
//
// A "no handler" failure in the outbox-package completeness test
// names the exact event_type and the file/line that needs editing.
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
var (
	defaultAlertNotifierMu sync.RWMutex
	defaultAlertNotifier   alerts.Notifier = alerts.NopNotifier{}
)

// SetAlertNotifier overrides the production alert sink. Called once
// from the composition root BEFORE the OutboxDispatcher goroutine
// starts. Calling SetAlertNotifier with nil resets to the no-op
// default. The lock also makes late reads safe for tests and future
// reconfiguration without changing the bootstrap semantics.
func SetAlertNotifier(n alerts.Notifier) {
	if n == nil {
		n = alerts.NopNotifier{}
	}
	defaultAlertNotifierMu.Lock()
	defaultAlertNotifier = n
	defaultAlertNotifierMu.Unlock()
}

// AlertNotifier returns the currently wired sink. ProductionRegistry
// reads it via this accessor so tests can swap sinks by calling
// SetAlertNotifier without rebuilding the registry.
func AlertNotifier() alerts.Notifier {
	defaultAlertNotifierMu.RLock()
	n := defaultAlertNotifier
	defaultAlertNotifierMu.RUnlock()
	return n
}

// ── ProductionRegistry ────────────────────────────────────────────────────

// ProductionRegistry returns the canonical *Registry that the
// supervisor's OutboxDispatcher is wired to at boot.
//
// Bootstrap (cmd/server/bootstrap_persistence) calls this once at
// boot and threads the returned *Registry through persistenceDeps.
// All later consumers (buildWorkers registers subsystem handlers,
// buildAssets creates the dispatcher against the same instance)
// operate on a SINGLE shared registry.
//
// The sync.Once cache below is the single-source-of-truth for this
// promise: any *future* call to ProductionRegistry() — from a
// subsystem wishing to add a handler, from a test, from anywhere —
// returns the same instance the dispatcher reads. If two callers
// asked for fresh registries, handlers registered against the
// "second" one would be invisible to the dispatcher (and the
// dispatcher's "no handler → MarkFailed" branch would fire for those
// event types at runtime — exactly the silent-failure mode Phase-5
// bootstrap-persistence wiring was designed to prevent).
//
// Subsystem handlers are added via RegisterHandlerFactories: a
// subsystem package (e.g. workers) pushes a factory at init time,
// and buildProductionRegistry calls every factory to populate the
// returned Registry. This way the dispatcher, the boot-time
// handler registration, and any future "I need the production
// registry" caller all reference the SAME Handler chain.
//
// Today: registers a real handler for JOB_FAILED (PR-OUTBOX-HANDLER).
// The handler decodes the canonical payload {job_id, error_code,
// error} and forwards an Alert to the wired Notifier.
var (
	productionRegOnce     sync.Once
	productionRegCache    *Registry
	productionRegStateMu  sync.Mutex
	productionRegBuilding bool
	productionRegBuilt    bool
	productionRegFailed   bool
	productionRegFailure  any
)

// RegistryFactory contributes one or more handlers to the canonical
// production registry. Push factories via RegisterHandlerFactory at
// init time; buildProductionRegistry calls every factory in
// registration order to build the cached *Registry.
type RegistryFactory func(reg *Registry)

// handlerFactories holds subsystem-registered factory funcs. The
// mutex guards the slice; each factory registers itself once via
// sync.Once (or in itself).
var (
	handlerFactoriesMu sync.Mutex
	handlerFactories   []RegistryFactory
)

// RegisterHandlerFactory appends a factory that will run inside
// buildProductionRegistry. Intended for subsystem packages (e.g.
// workers) that want their handlers wired automatically when the
// registry is first built. Registration is only valid before the first
// ProductionRegistry call; rejecting late registration prevents a factory
// from being silently ignored and keeps the cached registry immutable.
func RegisterHandlerFactory(f RegistryFactory) {
	if f == nil {
		panic("outbox: RegisterHandlerFactory with nil")
	}
	productionRegStateMu.Lock()
	defer productionRegStateMu.Unlock()
	if productionRegBuilt || productionRegBuilding {
		panic("outbox: RegisterHandlerFactory called after ProductionRegistry build started")
	}
	handlerFactoriesMu.Lock()
	handlerFactories = append(handlerFactories, f)
	handlerFactoriesMu.Unlock()
}

// ProductionRegistry returns the cached canonical *Registry. The
// first call invokes buildProductionRegistry; subsequent calls
// return the same instance. Subsystem handlers registered via
// RegisterHandlerFactory are included in the very first call.
func ProductionRegistry() *Registry {
	productionRegStateMu.Lock()
	if productionRegFailed {
		failure := productionRegFailure
		productionRegStateMu.Unlock()
		panic(fmt.Sprintf("outbox: production registry build failed: %v", failure))
	}
	productionRegStateMu.Unlock()

	productionRegOnce.Do(func() {
		productionRegStateMu.Lock()
		productionRegBuilding = true
		productionRegStateMu.Unlock()

		defer func() {
			if recovered := recover(); recovered != nil {
				productionRegStateMu.Lock()
				productionRegBuilding = false
				productionRegFailed = true
				productionRegFailure = recovered
				productionRegStateMu.Unlock()
				panic(recovered)
			}
		}()

		reg := buildProductionRegistry()

		productionRegStateMu.Lock()
		productionRegCache = reg
		productionRegBuilding = false
		productionRegBuilt = true
		productionRegStateMu.Unlock()
	})

	productionRegStateMu.Lock()
	defer productionRegStateMu.Unlock()
	if productionRegFailed {
		panic(fmt.Sprintf("outbox: production registry build failed: %v", productionRegFailure))
	}
	return productionRegCache
}

// buildProductionRegistry creates the canonical *Registry. It runs
// every subsystem factory registered via RegisterHandlerFactory
// AFTER registering the always-present JOB_FAILED handler. The
// JOB_FAILED-always-present ordering ensures the dispatcher's
// boot-time "this event_type is critical" alert path is wired before
// any subsystem opts in.
func buildProductionRegistry() *Registry {
	reg := NewRegistry()

	// Always-present: JOB_FAILED alert routing (PR-OUTBOX-HANDLER).
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
		// Best-effort delivery. Alert path is never authoritative —
		// a transient sink hiccup must not block the dispatcher's
		// "claim next event" loop. We log and swallow so the
		// dispatcher marks the event PROCESSED even when the alert
		// sink is degraded.
		if err := AlertNotifier().Notify(ctx, alert); err != nil {
			log.Printf("[OUTBOX] alert sink Notify failed for event_id=%s job_id=%s: %v",
				e.EventID, p.JobID, err)
		}
		return nil
	})

	// Subsystem-registered factories (added via RegisterHandlerFactory
	// at package-init time). We iterate the list under the lock and
	// snapshot-replay it without the lock to keep critical sections
	// minimal. The factories themselves must be idempotent — the
	// canonical production registry is built exactly once.
	handlerFactoriesMu.Lock()
	factories := append([]RegistryFactory(nil), handlerFactories...)
	handlerFactoriesMu.Unlock()
	for _, f := range factories {
		f(reg)
	}

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
