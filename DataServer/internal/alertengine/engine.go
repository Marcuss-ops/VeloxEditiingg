// Package alertengine provides the compute rule group for the shared alert
// runtime. Its rules remain compute-specific; only the downstream event,
// deduplication, and sink contract is shared with fleet alerts.
package alertengine

import (
	"context"
	"errors"
	"time"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/supervisor"
)

// RuleFunc is a compute rule: returns canonical runtime alert events when
// its condition is breached, or a non-nil error when its data source fails.
// Infrastructure failures MUST propagate to the supervisor — they are
// never converted into "no alert", which would let the alert runtime
// appear healthy while it is actually blind.
type RuleFunc func(ctx context.Context) (*runtimealerts.AlertEvent, error)

// Engine evaluates the compute rule group on a periodic tick.
type Engine struct {
	tick  time.Duration
	rules []RuleFunc

	// Cooldown is the minimum interval between repeated compute events.
	// It configures the shared runtime deduplicator rather than a
	// compute-specific implementation.
	Cooldown time.Duration

	pipeline     *runtimealerts.Pipeline
	runtime      *runtimealerts.Runtime
	errorMetrics runtimealerts.ErrorMetrics
}

var _ runtimealerts.Evaluator = (*Engine)(nil)

// New builds a compute engine with the shared runtime pipeline.
func New(tick time.Duration, notifier runtimealerts.Notifier) *Engine {
	if tick <= 0 {
		tick = 30 * time.Second
	}
	e := &Engine{tick: tick, Cooldown: 5 * time.Minute}
	var sinks []runtimealerts.Sink
	if notifier != nil {
		sinks = append(sinks, &runtimealerts.NotifySink{Notifier: notifier})
	}
	e.pipeline = runtimealerts.NewPipeline(
		runtimealerts.NewCooldownDeduplicator(e.Cooldown),
		sinks...,
	)
	e.runtime = runtimealerts.NewRuntime(e, e.pipeline, tick)
	e.runtime.NormalizeDispatchError = func(err error) error {
		classified := supervisor.ClassifyError(err)
		if supervisor.IsInfrastructure(classified) {
			e.recordError("infrastructure")
			return classified
		}
		e.recordError("isolated_sink")
		return nil
	}
	return e
}

// AddRule registers a compute rule function.
func (e *Engine) AddRule(r RuleFunc) { e.rules = append(e.rules, r) }

// AddSink adds a persistence or notification sink to the shared pipeline.
func (e *Engine) AddSink(sink runtimealerts.Sink) { e.pipeline.AddSink(sink) }

// SetErrorMetrics installs the low-cardinality error metric sink. It is
// optional so existing embedders remain source-compatible. Typed-nil sinks
// are ignored so a partial composition cannot panic on an error path.
func (e *Engine) SetErrorMetrics(sink runtimealerts.ErrorMetrics) {
	if runtimealerts.ErrorMetricsConfigured(sink) {
		e.errorMetrics = sink
		return
	}
	e.errorMetrics = nil
}

func (e *Engine) recordError(category string) {
	if e.errorMetrics != nil {
		e.errorMetrics.RecordAlertEvaluationError("compute", category, 1)
	}
}

// Evaluate returns the canonical runtime events emitted by the independent
// compute rule group.
// Each rule is evaluated independently: alerts from healthy rules are still
// returned even when a sibling rule fails, and all rule errors are joined
// into the returned error so the supervisor can act on consecutive
// infrastructure failures.
func (e *Engine) Evaluate(ctx context.Context) ([]runtimealerts.AlertEvent, error) {
	events := make([]runtimealerts.AlertEvent, 0, len(e.rules))
	var errs []error
	for _, rule := range e.rules {
		if err := ctx.Err(); err != nil {
			return events, err
		}
		alert, err := rule(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// Cancellation is the supervisor's normal shutdown signal,
				// not an infrastructure outage that should trigger retry.
				return events, err
			}
			// Compute rules are aggregate/global evaluations; unlike the
			// fleet engine they have no per-worker isolation boundary.
			// Treat every remaining rule datasource failure as infrastructure
			// so a generic provider error cannot make the engine look healthy.
			e.recordError("infrastructure")
			errs = append(errs, errors.Join(supervisor.ErrInfrastructure, err))
			continue
		}
		if alert == nil {
			continue
		}
		if alert.Group == "" {
			alert.Group = runtimealerts.GroupCompute
		}
		if alert.Subject == "" {
			alert.Subject = alert.RuleID
		}
		if alert.FiredAt.IsZero() {
			alert.FiredAt = time.Now().UTC()
		}
		if alert.EventID == "" {
			alert.EventID = runtimealerts.EventIDFor(*alert)
		}
		events = append(events, *alert)
	}
	return events, errors.Join(errs...)
}

// Run delegates the complete compute lifecycle to the common runtime:
// evaluator → event → dedup → persistence/notifier.
func (e *Engine) Run(ctx context.Context) error {
	if dedup, ok := e.pipeline.Dedup.(*runtimealerts.CooldownDeduplicator); ok {
		dedup.SetCooldown(e.Cooldown)
	}
	return e.runtime.Run(ctx)
}

// evaluateAll is retained for existing package tests and delegates to the
// same common runtime pass used by Run.
func (e *Engine) evaluateAll(ctx context.Context) error {
	if dedup, ok := e.pipeline.Dedup.(*runtimealerts.CooldownDeduplicator); ok {
		dedup.SetCooldown(e.Cooldown)
	}
	return e.runtime.RunOnce(ctx)
}
