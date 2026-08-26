package alertengine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/supervisor"
)

type computeErrorMetrics struct {
	mu    sync.Mutex
	calls []string
}

func (m *computeErrorMetrics) RecordAlertEvaluationError(engine, category string, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, engine+":"+category)
}

func (m *computeErrorMetrics) countCategory(category string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, call := range m.calls {
		if call == "compute:"+category {
			count++
		}
	}
	return count
}

type computeFailingSink struct {
	mu    sync.Mutex
	calls int
}

func (s *computeFailingSink) Process(context.Context, runtimealerts.AlertEvent) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return errors.New("compute notifier unavailable")
}

func (s *computeFailingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestEvaluatePropagatesInfrastructureErrorAndPreservesHealthySibling(t *testing.T) {
	engine := New(0, nil)
	metrics := &computeErrorMetrics{}
	engine.SetErrorMetrics(metrics)
	engine.AddRule(func(context.Context) (*runtimealerts.AlertEvent, error) {
		return nil, sql.ErrConnDone
	})
	engine.AddRule(func(context.Context) (*runtimealerts.AlertEvent, error) {
		return &runtimealerts.AlertEvent{RuleID: "HealthyRule", Severity: "warning", Summary: "still visible"}, nil
	})

	events, err := engine.Evaluate(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("Evaluate error = %v, want supervisor.ErrInfrastructure", err)
	}
	if len(events) != 1 || events[0].RuleID != "HealthyRule" {
		t.Fatalf("events = %+v, want healthy sibling event preserved", events)
	}
	if metrics.countCategory("infrastructure") != 1 {
		t.Fatalf("infrastructure metrics = %d, want 1", metrics.countCategory("infrastructure"))
	}
}

func TestEvaluateAllKeepsIsolatedSinkErrorForPipelineRetryWithoutRestart(t *testing.T) {
	engine := New(0, nil)
	sink := &computeFailingSink{}
	engine.AddSink(sink)
	engine.AddRule(func(context.Context) (*runtimealerts.AlertEvent, error) {
		return &runtimealerts.AlertEvent{RuleID: "AlwaysOn", Severity: "warning", Summary: "test"}, nil
	})

	if err := engine.evaluateAll(context.Background()); err != nil {
		t.Fatalf("first evaluateAll error = %v, want nil for isolated sink failure", err)
	}
	if err := engine.evaluateAll(context.Background()); err != nil {
		t.Fatalf("second evaluateAll error = %v, want nil for isolated sink failure", err)
	}
	if sink.count() != 2 {
		t.Fatalf("sink calls = %d, want 2 retry attempts", sink.count())
	}
}
