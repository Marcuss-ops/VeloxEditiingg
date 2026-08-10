package alerts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// RuleGroup describes one independently evaluated rule catalog. It is kept
// for source compatibility with early adopters; new code should construct a
// Runtime directly for one rule group.
type RuleGroup struct {
	Name     string
	Group    Group
	Tick     time.Duration
	Evaluate Evaluator
}

// Engine is a deprecated compatibility adapter over Runtime. It preserves
// the old multi-group construction API without introducing another execution
// loop or another event/dedup/sink contract.
//
// Deprecated: use Runtime with a group-specific Evaluator instead.
type Engine struct {
	pipeline *Pipeline
	mu       sync.RWMutex
	groups   []RuleGroup
	runtime  *Runtime
	lastRun  map[string]time.Time
}

// NewEngine preserves the former constructor while routing all execution
// through the single Runtime implementation.
//
// Deprecated: use NewRuntime.
func NewEngine(dedup Deduplicator, sinks ...Sink) *Engine {
	engine := &Engine{pipeline: NewPipeline(dedup, sinks...), lastRun: make(map[string]time.Time)}
	engine.runtime = NewRuntime(compositeEvaluator{engine: engine}, engine.pipeline, 30*time.Second)
	return engine
}

// Pipeline exposes the shared pipeline for compatibility wiring.
func (e *Engine) Pipeline() *Pipeline {
	if e == nil {
		return nil
	}
	return e.pipeline
}

// AddGroup registers a legacy group. Groups remain separate evaluators, but
// their events travel through the same Runtime pipeline.
func (e *Engine) AddGroup(group RuleGroup) {
	if e == nil {
		return
	}
	if group.Tick <= 0 {
		group.Tick = 30 * time.Second
	}
	if group.Name == "" {
		e.mu.RLock()
		group.Name = fmt.Sprintf("group-%d", len(e.groups))
		e.mu.RUnlock()
	}
	e.mu.Lock()
	e.groups = append(e.groups, group)
	if group.Tick < e.runtime.Tick {
		e.runtime.Tick = group.Tick
	}
	e.mu.Unlock()
}

// SetOnSuppressed preserves the old fleet lifecycle hook.
func (e *Engine) SetOnSuppressed(handler func(context.Context, AlertEvent) error) {
	if e != nil && e.pipeline != nil {
		e.pipeline.OnSuppressed = handler
	}
}

// GroupCount reports the number of registered compatibility groups.
func (e *Engine) GroupCount() int {
	if e == nil {
		return 0
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.groups)
}

// EvaluateAll executes one shared runtime pass.
func (e *Engine) EvaluateAll(ctx context.Context) error {
	if e == nil || e.runtime == nil {
		return errors.New("alerts runtime: nil compatibility engine")
	}
	return e.runtime.RunOnce(ctx)
}

// Run executes the shared runtime loop.
func (e *Engine) Run(ctx context.Context) error {
	if e == nil || e.runtime == nil {
		return errors.New("alerts runtime: nil compatibility engine")
	}
	return e.runtime.Run(ctx)
}

type compositeEvaluator struct{ engine *Engine }

func (e compositeEvaluator) Evaluate(ctx context.Context) ([]AlertEvent, error) {
	if e.engine == nil {
		return nil, errors.New("alerts runtime: nil compatibility engine")
	}
	e.engine.mu.RLock()
	groups := append([]RuleGroup(nil), e.engine.groups...)
	e.engine.mu.RUnlock()
	var events []AlertEvent
	var errs []error
	for _, group := range groups {
		if group.Evaluate == nil {
			errs = append(errs, fmt.Errorf("group %q: nil evaluator", group.Name))
			continue
		}
		now := time.Now().UTC()
		e.engine.mu.Lock()
		last := e.engine.lastRun[group.Name]
		if !last.IsZero() && now.Sub(last) < group.Tick {
			e.engine.mu.Unlock()
			continue
		}
		e.engine.lastRun[group.Name] = now
		e.engine.mu.Unlock()
		groupEvents, err := group.Evaluate.Evaluate(ctx)
		if group.Group != "" {
			filtered := groupEvents[:0]
			for index := range groupEvents {
				if groupEvents[index].Group != "" && groupEvents[index].Group != group.Group {
					errs = append(errs, fmt.Errorf("group %q emitted event for %q", group.Name, groupEvents[index].Group))
					continue
				}
				groupEvents[index].Group = group.Group
				filtered = append(filtered, groupEvents[index])
			}
			groupEvents = filtered
		}
		events = append(events, groupEvents...)
		errs = append(errs, err)
	}
	return events, errors.Join(errs...)
}
