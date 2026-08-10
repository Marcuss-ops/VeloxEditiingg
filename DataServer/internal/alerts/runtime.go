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

// Group identifies the independent rule catalog that produced an event.
// Compute and fleet remain separate groups even though they share the
// downstream runtime contract.
type Group string

const (
	GroupCompute Group = "compute"
	GroupFleet   Group = "fleet"
)

// AlertEvent is the canonical runtime event exchanged between an evaluator,
// deduplicator, and side-effect sinks. EventID is stable for one firing
// occurrence and lets every sink make retries idempotent. Subject and Labels
// carry producer context; consumers do not need to know the rule catalog.
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

// EventIDFor returns a deterministic ID for one firing occurrence. Evaluators
// should call it when constructing an event; the pipeline also fills an empty
// ID defensively. FiredAt is part of the identity so a later warning-window
// refire creates a new durable alert event while retries of the same event do
// not.
func EventIDFor(event AlertEvent) string {
	firedAt := event.FiredAt.UTC().UnixNano()
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%s", event.Group, event.RuleID, event.Subject, firedAt, event.Severity)
	if len(event.Labels) > 0 {
		keys := make([]string, 0, len(event.Labels))
		for key := range event.Labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(h, "\x00%s=%s", key, event.Labels[key])
		}
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// Evaluator produces canonical alert events and returns infrastructure
// errors. Compute and fleet implement this independently; the shared runtime
// only sees AlertEvent.
type Evaluator interface {
	Evaluate(context.Context) ([]AlertEvent, error)
}

// Claim is held while an event is being delivered. Commit records successful
// delivery; Release makes the event eligible for a retry after any sink fails.
// Implementations must reserve a dedup key atomically, but must not hold their
// internal lock while sink I/O runs.
type Claim interface {
	Commit()
	Release()
}

// Deduplicator atomically claims an event for delivery. A false result means
// the event is suppressed by the domain's dedup policy.
type Deduplicator interface {
	Claim(AlertEvent, time.Time) (Claim, bool)
}

// EventIDProvider lets a domain deduplicator retain the same occurrence ID
// across a failed delivery and retry while still allocating a new ID for a
// later refire. It is optional for simple deduplicators.
type EventIDProvider interface {
	// Prepare may restore immutable occurrence fields before persistence.
	Prepare(AlertEvent, time.Time) AlertEvent
	EventID(AlertEvent, time.Time) string
}

// SinkError preserves the shared side-effect stage while retaining the
// original cause for errors.Is and supervisor classification.
type SinkError struct {
	Stage string
	Err   error
}

func (e SinkError) Error() string { return e.Stage + ": " + e.Err.Error() }
func (e SinkError) Unwrap() error { return e.Err }

// StageOf returns a sink stage (for example persistence or notifier) when
// the error chain contains a SinkError.
func StageOf(err error) string {
	var sinkErr SinkError
	if errors.As(err, &sinkErr) {
		return sinkErr.Stage
	}
	return ""
}

// Sink is the common persistence/notifier boundary. Sinks should use
// AlertEvent.EventID as their idempotency key.
type Sink interface {
	Process(context.Context, AlertEvent) error
}

// NewPipeline creates a runtime pipeline with the supplied primary sinks.
func NewPipeline(dedup Deduplicator, sinks ...Sink) *Pipeline {
	p := &Pipeline{Dedup: dedup, afterDone: make(map[string]map[int]bool), afterPending: make(map[string]map[int]bool)}
	p.primarySinks = append(p.primarySinks, sinks...)
	return p
}

// FuncSink adapts a function into the shared sink contract.
type FuncSink func(context.Context, AlertEvent) error

func (f FuncSink) Process(ctx context.Context, event AlertEvent) error { return f(ctx, event) }

// Pipeline is the shared evaluator → event → dedup → sink runtime. It is
// deliberately small so compute and fleet can migrate independently.
type Pipeline struct {
	Dedup Deduplicator	mu              sync.RWMutex
	primarySinks    []Sink
	afterDone       map[string]map[int]bool
	afterPending    map[string]map[int]bool


	// AfterCommit are notification/secondary sinks. They run only after the
	// primary sinks succeed and the dedup claim is committed, so a failure
	// here cannot cause primary persistence to be repeated.
	afterCommitSinks []Sink

	// OnSuppressed is an optional compatibility hook for domain persistence
	// updates such as fleet's last_observed_at touch. It runs after Claim
	// returns false and outside the deduplicator's lock.
	OnSuppressed func(context.Context, AlertEvent) error
}

// AddSink appends a primary sink before or during bootstrap. Dispatch takes a
// snapshot, so runtime delivery is safe if configuration is assembled while
// another goroutine is starting.
func (p *Pipeline) AddSink(sink Sink) {
	if p == nil || sink == nil {
		return
	}
	p.mu.Lock()
	p.primarySinks = append(p.primarySinks, sink)
	p.mu.Unlock()
}

// AddAfterCommitSink appends a post-commit sink (typically a notifier).
func (p *Pipeline) AddAfterCommitSink(sink Sink) {
	if p == nil || sink == nil {
		return
	}
	p.mu.Lock()
	p.afterCommitSinks = append(p.afterCommitSinks, sink)
	p.mu.Unlock()
}

// sinkSnapshot returns immutable slices for one dispatch.
func (p *Pipeline) sinkSnapshot() (primary, after []Sink) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]Sink(nil), p.primarySinks...), append([]Sink(nil), p.afterCommitSinks...)
}

// Dispatch sends events through deduplication and all configured sinks.
// Claims are released when any sink fails, so the same event can be retried.
// Sink fan-out is at-least-once: sinks must be idempotent by EventID if one
// child succeeds and a later child fails. No lock is held during sink I/O.
func (p *Pipeline) Dispatch(ctx context.Context, events []AlertEvent) error {
	_, err := p.dispatch(ctx, events)
	return err
}

// DispatchOne sends one event and reports whether it was actually claimed and
// delivered. A false result means the event was suppressed by deduplication;
// sink errors still return false with the error. This lets domain engines run
// post-persistence side effects only for newly committed events.
func (p *Pipeline) DispatchOne(ctx context.Context, event AlertEvent) (bool, error) {
	results, err := p.dispatch(ctx, []AlertEvent{event})
	if len(results) == 0 {
		return false, err
	}
	return results[0], err
}

func (p *Pipeline) runAfterCommit(ctx context.Context, event AlertEvent, sinks []Sink) []error {
	var errs []error
	for index, sink := range sinks {
		if sink == nil {
			continue
		}
		p.mu.Lock()
		if p.afterDone[event.EventID] == nil {
			p.afterDone[event.EventID] = make(map[int]bool)
		}
		if p.afterDone[event.EventID][index] {
			p.mu.Unlock()
			continue
		}
		p.mu.Unlock()
		if err := sink.Process(ctx, event); err != nil {
			errs = append(errs, SinkError{Stage: "notifier", Err: fmt.Errorf("group=%s event=%s rule=%s: %w", event.Group, event.EventID, event.RuleID, err)})
			continue
		}
		p.mu.Lock()
		p.afterDone[event.EventID][index] = true
		p.mu.Unlock()
	}
	return errs
}

func (p *Pipeline) dispatch(ctx context.Context, events []AlertEvent) ([]bool, error) {
	if p == nil {
		return nil, errors.New("alerts runtime: nil pipeline")
	}
	results := make([]bool, 0, len(events))
	primarySinks, afterCommitSinks := p.sinkSnapshot()
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
		}
		results = append(results, true)
		errs = append(errs, p.runAfterCommit(ctx, event, afterCommitSinks)...)
	}
	return results, errors.Join(errs...)
}

// CooldownDeduplicator provides a generic bounded cooldown for compute-like
// events. Claims reserve a key only for the duration of sink delivery.
type CooldownDeduplicator struct {
	mu       sync.Mutex
	cooldown time.Duration
	last     map[string]time.Time
	pending  map[string]struct{}
}

func NewCooldownDeduplicator(cooldown time.Duration) *CooldownDeduplicator {
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	return &CooldownDeduplicator{cooldown: cooldown, last: make(map[string]time.Time), pending: make(map[string]struct{})}
}

func runtimeEventKey(event AlertEvent) string {
	return string(event.Group) + ":" + event.RuleID + ":" + event.Subject
}

type cooldownClaim struct {
	dedup *CooldownDeduplicator
	key   string
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

func (d *CooldownDeduplicator) Claim(event AlertEvent, now time.Time) (Claim, bool) {
	key := runtimeEventKey(event)
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
