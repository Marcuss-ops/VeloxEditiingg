// Package alertengine provides the compute rule group for the shared alert
// runtime. Its rules remain compute-specific; only the downstream event,
// deduplication, and sink contract is shared with fleet alerts.
package alertengine

import (
	"context"
	"log"
	"sync"
	"time"

	runtimealerts "velox-server/internal/alerts"
)

// RuleFunc is the legacy compute rule shape retained during migration.
type RuleFunc func(ctx context.Context) *Alert

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

	pipeline *runtimealerts.Pipeline
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

// Evaluate converts the independent compute rule group into shared events.
func (e *Engine) Evaluate(ctx context.Context) ([]runtimealerts.AlertEvent, error) {
	events := make([]runtimealerts.AlertEvent, 0, len(e.rules))
	for _, rule := range e.rules {
		alert := rule(ctx)
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
	return events, nil
}

// Run evaluates and dispatches the compute group until cancellation. Sink
// failures are returned to the supervisor instead of being silently logged.
func (e *Engine) Run(ctx context.Context) error {
	log.Printf("[ALERT-ENGINE] starting — tick=%s, rules=%d", e.tick, len(e.rules))
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
// evaluator → event → dedup → sink pipeline.
func (e *Engine) evaluateAll(ctx context.Context) error {
	events, err := e.Evaluate(ctx)
	if err != nil {
		return err
	}
	return e.pipeline.Dispatch(ctx, events)
}
