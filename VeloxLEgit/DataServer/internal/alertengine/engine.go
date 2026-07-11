// Package alertengine provides the periodic alert evaluation engine.
// Runs on a 30s tick evaluating configurable rules against the
// observability service and system metrics, logging structured
// alerts and optionally calling webhooks (Slack/Telegram).
//
// Step 6/6 — Velox Metrics Center.
package alertengine

import (
	"context"
	"log"
	"sync"
	"time"
)

// RuleFunc evaluates a single alert rule and returns a triggered alert
// (or nil if the rule is healthy).
type RuleFunc func(ctx context.Context) *Alert

// Alert represents a triggered alert with structured metadata.
type Alert struct {
	Name        string            `json:"name"`
	Severity    string            `json:"severity"`
	Summary     string            `json:"summary"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels,omitempty"`
	Timestamp   time.Time         `json:"timestamp"`
}

// Engine evaluates alert rules on a periodic tick.
type Engine struct {
	tick      time.Duration
	rules     []RuleFunc
	notify    Notifier
	mu        sync.Mutex
	lastFired map[string]time.Time // alert-name → last-fire timestamp

	// Cooldown is the minimum interval between repeated alerts for
	// the same rule name. Default 5 minutes.
	Cooldown time.Duration
}

// Notifier sends alerts to external systems (Slack, Telegram, etc.).
type Notifier interface {
	Send(ctx context.Context, alert Alert) error
}

// New builds an AlertEngine with the given tick interval and optional notifier.
func New(tick time.Duration, notifier Notifier) *Engine {
	if tick <= 0 {
		tick = 30 * time.Second
	}
	return &Engine{
		tick:      tick,
		notify:    notifier,
		lastFired: make(map[string]time.Time),
		Cooldown:  5 * time.Minute,
	}
}

// AddRule registers a rule function to be evaluated on each tick.
func (e *Engine) AddRule(r RuleFunc) {
	e.rules = append(e.rules, r)
}

// Run loops until ctx is done, evaluating all rules on each tick.
// Returns ctx.Err() on graceful shutdown; non-nil error on rule evaluation
// failures that should trigger a restart (via the supervisor).
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
			e.evaluateAll(ctx)
		}
	}
}

func (e *Engine) evaluateAll(ctx context.Context) {
	for _, rule := range e.rules {
		alert := rule(ctx)
		if alert == nil {
			continue
		}
		alert.Timestamp = time.Now().UTC()
		e.fire(ctx, *alert)
	}
}

func (e *Engine) fire(ctx context.Context, alert Alert) {
	e.mu.Lock()
	last, exists := e.lastFired[alert.Name]
	if exists && time.Since(last) < e.Cooldown {
		e.mu.Unlock()
		return // still in cooldown — suppress duplicate
	}
	e.lastFired[alert.Name] = time.Now()
	e.mu.Unlock()

	log.Printf("[ALERT] [%s] %s: %s — %s",
		alert.Severity, alert.Name, alert.Summary, alert.Description)

	if e.notify != nil {
		if err := e.notify.Send(ctx, alert); err != nil {
			log.Printf("[ALERT] notify failed for %s: %v", alert.Name, err)
		}
	}
}
