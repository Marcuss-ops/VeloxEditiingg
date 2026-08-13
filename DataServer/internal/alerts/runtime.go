// Package alerts is the SINGLE alert runtime contract for Velox.
//
//   - One alert model:      AlertEvent — produced by every evaluator.
//   - One evaluation path:  Evaluator → AlertEvent → Deduplicator → Sink.
//   - One dedup:            CooldownDeduplicator (uniform cooldown for
//     every severity; the key is group:rule:subject:severity).
//   - One runtime runner:   Runtime hosts the evaluator lifecycle for each
//     rule group. Compute and fleet keep separate rule catalogs, while both
//     use the same evaluator/event/pipeline contract.
//   - One notification sink: NotifySink adapts AlertEvent to the
//     canonical notification envelope (Alert) and forwards it to the
//     wired alerts.Notifier.
//
// The rule packages (internal/alertengine for compute, and
// internal/fleet/opsalerts for fleet) provide GROUP CONTENT only:
// pure rule closures / snapshot evaluators implementing Evaluator.
// They retain group-specific dedup policy adapters where persistence
// semantics differ, but all side effects flow through Pipeline.
package alerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Group string

const (
	GroupCompute Group = "compute"
	GroupFleet   Group = "fleet"
)

// AlertEvent is THE alert model of the runtime. Every evaluator emits
// AlertEvents; the pipeline deduplicates them and routes them to the
// persistence and notification sinks. AlertEvent deliberately carries
// no per-engine fields: group-specific data (e.g. a fleet worker's
// current_value) lives in Labels.
type AlertEvent struct {
	EventID     string
	Group       Group
	RuleID      string
	Severity    string
	Subject     string
	Summary     string
	Description string
	Labels      map[string]string
	FiredAt     time.Time
}

// EventIDFor derives a stable, label-order-independent event id.
// Identical event content (group, rule, subject, severity, labels,
// fired time) always hashes to the same id — this is what makes the
// persistence sink idempotent by EventID.
func EventIDFor(event AlertEvent) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%s", event.Group, event.RuleID, event.Subject, event.FiredAt.UTC().UnixNano(), event.Severity)
	keys := make([]string, 0, len(event.Labels))
	for key := range event.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(h, "\x00%s=%s", key, event.Labels[key])
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// ── Evaluator: the one evaluation contract ───────────────────────────────

// Evaluator is implemented independently by each rule group (compute,
// fleet). Evaluate returns the group's firing events; infrastructure
// errors MUST be returned (never converted into "no alert") so the
// engine can surface them to the supervisor.
type Evaluator interface {
	Evaluate(context.Context) ([]AlertEvent, error)
}

// EvaluatorFunc adapts a plain function to the Evaluator contract.
type EvaluatorFunc func(context.Context) ([]AlertEvent, error)

// Evaluate implements Evaluator.
func (f EvaluatorFunc) Evaluate(ctx context.Context) ([]AlertEvent, error) { return f(ctx) }

// ── Deduplication ────────────────────────────────────────────────────────

type Claim interface {
	Commit()
	Release()
}

type Deduplicator interface {
	Claim(AlertEvent, time.Time) (Claim, bool)
}

type EventIDProvider interface {
	Prepare(AlertEvent, time.Time) AlertEvent
	EventID(AlertEvent, time.Time) string
}

// DedupKey is the canonical dedup identity: group + rule + subject +
// severity. Severity is part of the key so dual-severity rules
// (disk 85% WARNING vs 95% CRITICAL, cert 15d vs 5d) escalate
// independently, and a severity change re-fires immediately.
type DedupKey struct {
	Group    Group
	RuleID   string
	Subject  string
	Severity string
}

func (k DedupKey) String() string {
	return string(k.Group) + ":" + k.RuleID + ":" + k.Subject + ":" + k.Severity
}

// CooldownDeduplicator is the SINGLE in-memory dedup of the alert
// runtime. It applies one uniform cooldown window to every severity
// (chosen semantics: CRITICAL does NOT bypass the window — the fleet
// dashboard stays fresh through the touch path instead of re-inserting
// rows every tick). The window rolls forward whenever the event is
// touched while still suppressed, so a continuously firing rule
// notifies once per cooldown rather than once per tick.
type CooldownDeduplicator struct {
	mu       sync.Mutex
	cooldown time.Duration
	last     map[DedupKey]time.Time
	pending  map[DedupKey]struct{}
}

// NewCooldownDeduplicator builds the unified dedup. Non-positive
// cooldowns fall back to the 5-minute default.
func NewCooldownDeduplicator(cooldown time.Duration) *CooldownDeduplicator {
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	return &CooldownDeduplicator{cooldown: cooldown, last: make(map[DedupKey]time.Time), pending: make(map[DedupKey]struct{})}
}

// SetCooldown updates the shared cooldown used by subsequent claims. It is
// intentionally safe to call while a runtime is idle between evaluation
// passes, which preserves the legacy compute configuration seam without
// introducing a second deduplicator implementation.
func (d *CooldownDeduplicator) SetCooldown(cooldown time.Duration) {
	if d == nil {
		return
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	d.mu.Lock()
	d.cooldown = cooldown
	d.mu.Unlock()
}

func dedupKeyOf(event AlertEvent) DedupKey {
	return DedupKey{Group: event.Group, RuleID: event.RuleID, Subject: event.Subject, Severity: event.Severity}
}

type cooldownClaim struct {
	dedup *CooldownDeduplicator
	key   DedupKey
	once  sync.Once
}

func (c *cooldownClaim) Commit() {
	c.once.Do(func() {
		c.dedup.mu.Lock()
		delete(c.dedup.pending, c.key)
		c.dedup.last[c.key] = time.Now().UTC()
		c.dedup.mu.Unlock()
	})
}

func (c *cooldownClaim) Release() {
	c.once.Do(func() {
		c.dedup.mu.Lock()
		delete(c.dedup.pending, c.key)
		c.dedup.mu.Unlock()
	})
}

// Claim grants the in-flight claim for a fresh event (no pending claim
// and outside the cooldown window since the last commit).
func (d *CooldownDeduplicator) Claim(event AlertEvent, now time.Time) (Claim, bool) {
	key := dedupKeyOf(event)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.pending[key]; ok {
		return nil, false
	}
	if last, ok := d.last[key]; ok && now.Sub(last) < d.cooldown {
		return nil, false
	}
	d.pending[key] = struct{}{}
	return &cooldownClaim{dedup: d, key: key}, true
}

// Touch rolls the cooldown window forward for a suppressed event
// without claiming it. The fleet group's suppressed-path handler uses
// this to keep the persisted alert's last_observed fresh while the
// condition persists.
func (d *CooldownDeduplicator) Touch(event AlertEvent, now time.Time) {
	d.mu.Lock()
	d.last[dedupKeyOf(event)] = now
	d.mu.Unlock()
}

// Forget removes a key from the dedup state. The fleet group uses it
// after auto-resolving an alert whose condition cleared, so a later
// re-fire of the same condition is treated as fresh.
func (d *CooldownDeduplicator) Forget(key DedupKey) {
	d.mu.Lock()
	delete(d.last, key)
	delete(d.pending, key)
	d.mu.Unlock()
}

// Keys returns the keys recently seen for a group. An empty subject
// matches every subject of that group. Used by the fleet group's
// auto-resolve pass to enumerate previously-firing alerts.
func (d *CooldownDeduplicator) Keys(group Group, subject string) []DedupKey {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DedupKey, 0, 4)
	for key := range d.last {
		if key.Group != group {
			continue
		}
		if subject != "" && key.Subject != subject {
			continue
		}
		out = append(out, key)
	}
	return out
}

// ── Sinks ────────────────────────────────────────────────────────────────

type SinkError struct {
	Stage string
	Err   error
}

func (e SinkError) Error() string { return e.Stage + ": " + e.Err.Error() }
func (e SinkError) Unwrap() error { return e.Err }

func StageOf(err error) string {
	var sinkErr SinkError
	if errors.As(err, &sinkErr) {
		return sinkErr.Stage
	}
	return ""
}

// Sink is the one sink contract of the runtime. Persistence adapters
// and the notification adapter implement it.
type Sink interface {
	Process(context.Context, AlertEvent) error
}

// FuncSink adapts a function to Sink.
type FuncSink func(context.Context, AlertEvent) error

func (f FuncSink) Process(ctx context.Context, event AlertEvent) error { return f(ctx, event) }

// ── Pipeline ─────────────────────────────────────────────────────────────

func NewPipeline(dedup Deduplicator, sinks ...Sink) *Pipeline {
	p := &Pipeline{
		Dedup:            dedup,
		afterDone:        make(map[string]map[int]bool),
		afterPending:     make(map[string]map[int]bool),
		primarySinks:     append([]Sink(nil), sinks...),
		afterCommitSinks: nil,
	}
	return p
}

type Pipeline struct {
	Dedup Deduplicator

	mu               sync.RWMutex
	primarySinks     []Sink
	afterCommitSinks []Sink
	afterDone        map[string]map[int]bool
	afterPending     map[string]map[int]bool

	OnSuppressed func(context.Context, AlertEvent) error
}

func (p *Pipeline) AddSink(sink Sink) {
	if p == nil || sink == nil {
		return
	}
	p.mu.Lock()
	p.primarySinks = append(p.primarySinks, sink)
	p.mu.Unlock()
}

func (p *Pipeline) AddAfterCommitSink(sink Sink) {
	if p == nil || sink == nil {
		return
	}
	p.mu.Lock()
	p.afterCommitSinks = append(p.afterCommitSinks, sink)
	p.mu.Unlock()
}

func (p *Pipeline) sinkSnapshot() (primary, after []Sink) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]Sink(nil), p.primarySinks...), append([]Sink(nil), p.afterCommitSinks...)
}

func (p *Pipeline) Dispatch(ctx context.Context, events []AlertEvent) error {
	_, err := p.dispatch(ctx, events)
	return err
}

func (p *Pipeline) resetAfterCommit(key string) {
	p.mu.Lock()
	delete(p.afterDone, key)
	delete(p.afterPending, key)
	p.mu.Unlock()
}

func (p *Pipeline) runAfterCommit(ctx context.Context, event AlertEvent, sinks []Sink) []error {
	var errs []error
	key := afterCommitKey(event)
	for index, sink := range sinks {
		if sink == nil {
			continue
		}
		p.mu.Lock()
		if p.afterDone[key] == nil {
			p.afterDone[key] = make(map[int]bool)
		}
		if p.afterPending[key] == nil {
			p.afterPending[key] = make(map[int]bool)
		}
		if p.afterDone[key][index] || p.afterPending[key][index] {
			p.mu.Unlock()
			continue
		}
		p.afterPending[key][index] = true
		p.mu.Unlock()

		if err := sink.Process(ctx, event); err != nil {
			p.mu.Lock()
			p.afterPending[key][index] = false
			p.mu.Unlock()
			errs = append(errs, SinkError{Stage: "notifier", Err: fmt.Errorf("group=%s event=%s rule=%s: %w", event.Group, event.EventID, event.RuleID, err)})
			continue
		}
		p.mu.Lock()
		p.afterPending[key][index] = false
		p.afterDone[key][index] = true
		p.mu.Unlock()
	}
	return errs
}

func (p *Pipeline) dispatch(ctx context.Context, events []AlertEvent) ([]bool, error) {
	if p == nil {
		return nil, errors.New("alerts runtime: nil pipeline")
	}
	primarySinks, afterCommitSinks := p.sinkSnapshot()
	results := make([]bool, 0, len(events))
	var errs []error
	for _, event := range events {
		now := event.FiredAt
		if now.IsZero() {
			now = time.Now().UTC()
			event.FiredAt = now
		}
		if provider, ok := p.Dedup.(EventIDProvider); ok {
			event = provider.Prepare(event, now)
			event.EventID = provider.EventID(event, now)
		}
		if event.EventID == "" {
			event.EventID = EventIDFor(event)
		}

		var claim Claim
		if p.Dedup != nil {
			var ok bool
			claim, ok = p.Dedup.Claim(event, now)
			if !ok {
				results = append(results, false)
				if p.OnSuppressed != nil {
					if err := p.OnSuppressed(ctx, event); err != nil {
						errs = append(errs, err)
					}
				}
				errs = append(errs, p.runAfterCommit(ctx, event, afterCommitSinks)...)
				continue
			}
		}

		var eventErrs []error
		for _, sink := range primarySinks {
			if sink == nil {
				continue
			}
			if err := sink.Process(ctx, event); err != nil {
				eventErrs = append(eventErrs, fmt.Errorf("group=%s event=%s rule=%s: %w", event.Group, event.EventID, event.RuleID, err))
				break
			}
		}
		if len(eventErrs) > 0 {
			results = append(results, false)
			if claim != nil {
				claim.Release()
			}
			errs = append(errs, eventErrs...)
			continue
		}
		if claim != nil {
			claim.Commit()
			// A fresh claim starts a new notification occurrence. Clear
			// the previous occurrence's completion state while retaining
			// retry state for suppressed events within this cooldown.
			p.resetAfterCommit(afterCommitKey(event))
		}
		results = append(results, true)
		errs = append(errs, p.runAfterCommit(ctx, event, afterCommitSinks)...)
	}
	return results, errors.Join(errs...)
}
