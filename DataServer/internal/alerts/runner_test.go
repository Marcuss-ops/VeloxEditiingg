package alerts_test

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/alerts"
)

type runnerEvaluator struct {
	group alerts.Group
}

func (e runnerEvaluator) Evaluate(context.Context) ([]alerts.AlertEvent, error) {
	return []alerts.AlertEvent{{
		Group:   e.group,
		RuleID:  "same-rule",
		Subject: "same-subject",
		FiredAt: time.Unix(100, 0).UTC(),
	}}, nil
}

type runnerSink struct {
	seen []alerts.Group
}

func (s *runnerSink) Process(_ context.Context, event alerts.AlertEvent) error {
	s.seen = append(s.seen, event.Group)
	return nil
}

func TestRuntimeRunsComputeAndFleetGroupsThroughSameLifecycle(t *testing.T) {
	dedup := alerts.NewCooldownDeduplicator(time.Hour)
	sink := &runnerSink{}
	pipeline := alerts.NewPipeline(dedup, sink)

	compute := alerts.NewRuntime(runnerEvaluator{group: alerts.GroupCompute}, pipeline, time.Hour)
	fleet := alerts.NewRuntime(runnerEvaluator{group: alerts.GroupFleet}, pipeline, time.Hour)
	if err := compute.RunOnce(context.Background()); err != nil {
		t.Fatalf("compute RunOnce: %v", err)
	}
	if err := fleet.RunOnce(context.Background()); err != nil {
		t.Fatalf("fleet RunOnce: %v", err)
	}
	if len(sink.seen) != 2 || sink.seen[0] != alerts.GroupCompute || sink.seen[1] != alerts.GroupFleet {
		t.Fatalf("sink groups = %v, want compute then fleet", sink.seen)
	}
}

func TestRuntimeAfterCommitUsesLogicalIdentityWhenFiredAtChanges(t *testing.T) {
	var calls int
	pipeline := alerts.NewPipeline(alerts.NewCooldownDeduplicator(time.Hour), alerts.FuncSink(func(context.Context, alerts.AlertEvent) error { return nil }))
	pipeline.AddAfterCommitSink(alerts.FuncSink(func(context.Context, alerts.AlertEvent) error {
		calls++
		return nil
	}))
	evaluator := alerts.EvaluatorFunc(func(context.Context) ([]alerts.AlertEvent, error) {
		return []alerts.AlertEvent{{Group: alerts.GroupCompute, RuleID: "same", Subject: "subject", Severity: "warning"}}, nil
	})
	runtime := alerts.NewRuntime(evaluator, pipeline, time.Hour)
	if err := runtime.RunOnce(context.Background()); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if err := runtime.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if calls != 1 {
		t.Fatalf("after-commit calls = %d, want 1 for one logical alert", calls)
	}
}

func TestRuntimeBeforeDispatchRunsBeforePrimarySink(t *testing.T) {
	order := make([]string, 0, 2)
	pipeline := alerts.NewPipeline(nil, alerts.FuncSink(func(context.Context, alerts.AlertEvent) error {
		order = append(order, "sink")
		return nil
	}))
	runtime := alerts.NewRuntime(runnerEvaluator{group: alerts.GroupFleet}, pipeline, time.Hour)
	runtime.BeforeDispatch = func(context.Context, []alerts.AlertEvent) error {
		order = append(order, "before")
		return nil
	}
	if err := runtime.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := order; len(got) != 2 || got[0] != "before" || got[1] != "sink" {
		t.Fatalf("lifecycle order = %v, want before then sink", got)
	}
}
