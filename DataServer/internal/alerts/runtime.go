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

// Evaluator is the common contract implemented independently by compute and fleet rule groups.
type Evaluator interface {
	Evaluate(context.Context) ([]AlertEvent, error)
}

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

type Sink interface {
	Process(context.Context, AlertEvent) error
}

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

type FuncSink func(context.Context, AlertEvent) error

func (f FuncSink) Process(ctx context.Context, event AlertEvent) error { return f(ctx, event) }

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
		if p.afterPending[event.EventID] == nil {
			p.afterPending[event.EventID] = make(map[int]bool)
		}
		if p.afterDone[event.EventID][index] || p.afterPending[event.EventID][index] {
			p.mu.Unlock()
			continue
		}
		p.afterPending[event.EventID][index] = true
		p.mu.Unlock()

		if err := sink.Process(ctx, event); err != nil {
			p.mu.Lock()
			p.afterPending[event.EventID][index] = false
			p.mu.Unlock()
			errs = append(errs, SinkError{Stage: "notifier", Err: fmt.Errorf("group=%s event=%s rule=%s: %w", event.Group, event.EventID, event.RuleID, err)})
			continue
		}
		p.mu.Lock()
		p.afterPending[event.EventID][index] = false
		p.afterDone[event.EventID][index] = true
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
		}
		results = append(results, true)
		errs = append(errs, p.runAfterCommit(ctx, event, afterCommitSinks)...)
	}
	return results, errors.Join(errs...)
}

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
