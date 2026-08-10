package alerts_test

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/alerts"
)

func TestLegacyEngineUsesSharedRuntimeAndPreservesGroupIsolation(t *testing.T) {
	var seen []alerts.Group
	engine := alerts.NewEngine(nil, alerts.FuncSink(func(_ context.Context, event alerts.AlertEvent) error {
		seen = append(seen, event.Group)
		return nil
	}))
	engine.AddGroup(alerts.RuleGroup{
		Name:  "compute",
		Group: alerts.GroupCompute,
		Tick:  time.Hour,
		Evaluate: alerts.EvaluatorFunc(func(context.Context) ([]alerts.AlertEvent, error) {
			return []alerts.AlertEvent{{RuleID: "compute-rule"}}, nil
		}),
	})
	engine.AddGroup(alerts.RuleGroup{
		Name:  "fleet",
		Group: alerts.GroupFleet,
		Tick:  time.Minute,
		Evaluate: alerts.EvaluatorFunc(func(context.Context) ([]alerts.AlertEvent, error) {
			return []alerts.AlertEvent{{RuleID: "fleet-rule"}}, nil
		}),
	})
	if engine.GroupCount() != 2 {
		t.Fatalf("GroupCount = %d, want 2", engine.GroupCount())
	}
	if err := engine.EvaluateAll(context.Background()); err != nil {
		t.Fatalf("EvaluateAll: %v", err)
	}
	if len(seen) != 2 || seen[0] != alerts.GroupCompute || seen[1] != alerts.GroupFleet {
		t.Fatalf("sink groups = %v, want compute then fleet", seen)
	}
}
