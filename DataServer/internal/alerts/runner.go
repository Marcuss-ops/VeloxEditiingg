package alerts

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Runtime is the common execution contract for alert groups. Evaluators own
// only rule/data-source evaluation and return canonical AlertEvents. Runtime
// owns the pass lifecycle: optional persistence preparation, pipeline dedup,
// primary persistence, and post-commit notification.
type Runtime struct {
	Evaluator Evaluator
	Pipeline  *Pipeline
	Tick      time.Duration

	// BeforeDispatch is an optional persistence lifecycle hook. It is used by
	// fleet alerts to resolve no-longer-firing rows before current events are
	// claimed/persisted. It must not perform evaluator work.
	BeforeDispatch func(context.Context, []AlertEvent) error

	// NormalizeDispatchError lets a group classify sink failures. A nil
	// normalized error means the sink failure was isolated and has been
	// metricated/retryable without restarting the whole evaluator.
	NormalizeDispatchError func(error) error

	mu sync.Mutex
}

// NewRuntime creates the common runner with a safe default tick.
func NewRuntime(evaluator Evaluator, pipeline *Pipeline, tick time.Duration) *Runtime {
	if tick <= 0 {
		tick = 30 * time.Second
	}
	return &Runtime{Evaluator: evaluator, Pipeline: pipeline, Tick: tick}
}

// RunOnce evaluates one pass and sends all resulting events through the
// shared lifecycle. Evaluation errors do not prevent partial events from
// reaching persistence, but are returned to the caller.
func afterCommitKey(event AlertEvent) string {
	return string(event.Group) + "\x00" + event.RuleID + "\x00" + event.Subject + "\x00" + event.Severity
}

func (r *Runtime) RunOnce(ctx context.Context) error {
	if r == nil || r.Evaluator == nil {
		return errors.New("alerts runtime: nil evaluator")
	}
	if r.Pipeline == nil {
		return errors.New("alerts runtime: nil pipeline")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	events, evalErr := r.Evaluator.Evaluate(ctx)
	var beforeErr error
	if evalErr == nil && r.BeforeDispatch != nil {
		beforeErr = r.BeforeDispatch(ctx, events)
	}
	dispatchErr := r.Pipeline.Dispatch(ctx, events)
	if dispatchErr != nil && r.NormalizeDispatchError != nil {
		dispatchErr = r.NormalizeDispatchError(dispatchErr)
	}
	return errors.Join(evalErr, beforeErr, dispatchErr)
}

// Run executes an immediate pass followed by periodic passes until canceled.
// Immediate evaluation prevents a dependency outage from remaining hidden
// until the first interval elapses.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.RunOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(r.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.RunOnce(ctx); err != nil {
				return err
			}
		}
	}
}
