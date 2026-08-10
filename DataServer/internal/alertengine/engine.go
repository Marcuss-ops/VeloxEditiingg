// Package alertengine provides the compute rule group for the shared alert
// runtime. Its rules remain compute-specific; only the downstream event,
// deduplication, and sink contract is shared with fleet alerts.
package alertengine

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/supervisor"
)

// RuleFunc is a compute rule: returns an alert when its condition is
// breached, or a non-nil error when the rule's data sources fail.
// Infrastructure failures MUST propagate to the supervisor — they are
// never converted into "no alert", which would let the alert engine
// appear healthy while it is actually blind.
type RuleFunc func(ctx context.Context) (*Alert, error)

// Alert is the legacy compute rule output. Evaluate converts it to the
// shared runtime AlertEvent before deduplication and notification.
type Alert struct {
	EventID     string            `json:"event_id,omitempty"`
	Name        string            `json:"name"`
	Severity    string            `json:"severity"`
	Summary     string            `json:"summary"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
}

// Engine evaluates the compute rule group on a periodic tick.
type Engine struct {
	tick      time.Duration
	rules     []RuleFunc
	notify    Notifier
	mu        sync.Mutex
	lastFired map[string]time.Time
	pending   map[string]struct{}

	// Cooldown is the minimum interval between repeated compute events.
	Cooldown time.Duration

	pipeline     *runtimealerts.Pipeline
	errorMetrics runtimealerts.ErrorMetrics
}

var _ runtimealerts.Evaluator = (*Engine)(nil)

// Notifier is retained as the compute-facing compatibility contract.
type Notifier interface {
	Send(ctx context.Context, alert Alert) error
}

// computeDeduplicator preserves the compute engine's name-based cooldown
// while implementing the shared claim contract. Its lock is held only for
// the claim bookkeeping, never while a notifier performs I/O.
type computeDeduplicator struct{ engine *Engine }

type computeClaim struct {
	engine *Engine
	key    string
	now    time.Time
	once   sync.Once
}

func (c *computeClaim) Commit() {
	c.once.Do(func() {
		c.engine.mu.Lock()
		delete(c.engine.pending, c.key)
		c.engine.lastFired[c.key] = c.now
		c.engine.mu.Unlock()
	})
}

func (c *computeClaim) Release() {
	c.once.Do(func() {
		c.engine.mu.Lock()
		delete(c.engine.pending, c.key)
		c.engine.mu.Unlock()
	})
}

func (d computeDeduplicator) Claim(event runtimealerts.AlertEvent, now time.Time) (runtimealerts.Claim, bool) {
	d.engine.mu.Lock()
	defer d.engine.mu.Unlock()
	key := event.RuleID
	if _, ok := d.engine.pending[key]; ok {
		return nil, false
	}
	if last, ok := d.engine.lastFired[key]; ok && now.Sub(last) < d.engine.Cooldown {
		return nil, false
	}
	d.engine.pending[key] = struct{}{}
	return &computeClaim{engine: d.engine, key: key, now: now}, true
}

type computeNotifierSink struct{ notifier Notifier }

func (s computeNotifierSink) Process(ctx context.Context, event runtimealerts.AlertEvent) error {
	if s.notifier == nil {
		return nil
	}
	if err := s.notifier.Send(ctx, Alert{
		EventID:     event.EventID,
		Name:        event.RuleID,
		Severity:    event.Severity,
		Summary:     event.Summary,
		Description: event.Description,
		Labels:      event.Labels,
		Timestamp:   event.FiredAt,
	}); err != nil {
		return runtimealerts.SinkError{Stage: "notifier", Err: err}
	}
	return nil
}

// New builds a compute engine with the shared runtime pipeline.
func New(tick time.Duration, notifier Notifier) *Engine {
	if tick <= 0 {
		tick = 30 * time.Second
	}
	e := &Engine{
		tick:      tick,
		notify:    notifier,
		lastFired: make(map[string]time.Time),
		pending:   make(map[string]struct{}),
		Cooldown:  5 * time.Minute,
	}
	e.pipeline = runtimealerts.NewPipeline(
		computeDeduplicator{engine: e},
		computeNotifierSink{notifier: notifier},
	)
	return e
}

// AddRule registers a compute rule function.
func (e *Engine) AddRule(r RuleFunc) { e.rules = append(e.rules, r) }

// AddSink adds a persistence or notification sink to the shared pipeline.
// This is the gradual-migration seam for compute integrations beyond the
// legacy webhook notifier.
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

// Evaluate converts the independent compute rule group into shared events.
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
		firedAt := alert.Timestamp
		if firedAt.IsZero() {
			firedAt = time.Now().UTC()
		}
		event := runtimealerts.AlertEvent{
			Group:       runtimealerts.GroupCompute,
			RuleID:      alert.Name,
			Severity:    alert.Severity,
			Subject:     alert.Name,
			Summary:     alert.Summary,
			Description: alert.Description,
			Labels:      alert.Labels,
			FiredAt:     firedAt,
		}
		event.EventID = runtimealerts.EventIDFor(event)
		events = append(events, event)
	}
	return events, errors.Join(errs...)
}

// Run evaluates and dispatches the compute group until cancellation. Sink
// failures are returned to the supervisor instead of being silently logged.
func (e *Engine) Run(ctx context.Context) error {
	log.Printf("[ALERT-ENGINE] starting — tick=%s, rules=%d", e.tick, len(e.rules))
	if err := e.evaluateAll(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[ALERT-ENGINE] exit: %v", ctx.Err())
			return ctx.Err()
		case <-ticker.C:
			if err := e.evaluateAll(ctx); err != nil {
				return err
			}
		}
	}
}

// evaluateAll is retained for existing package tests and now runs the shared
// evaluator → event → dedup → sink pipeline. Rule evaluation errors and sink
// failures are joined so a persistent infra failure reaches the supervisor;
// alerts produced by healthy rules are still dispatched.
func (e *Engine) evaluateAll(ctx context.Context) error {
	events, evalErr := e.Evaluate(ctx)
	dispatchErr := e.pipeline.Dispatch(ctx, events)
	if dispatchErr != nil {
		classified := supervisor.ClassifyError(dispatchErr)
		if supervisor.IsInfrastructure(classified) {
			e.recordError("infrastructure")
			dispatchErr = classified
		} else {
			// Isolated sink/notifier failures are retried by the shared
			// pipeline on the next dispatch, not by restarting the whole
			// runner. Keep the supervisor healthy while recording the loss.
			e.recordError("isolated_sink")
			dispatchErr = nil
		}
	}
	return errors.Join(evalErr, dispatchErr)
}
