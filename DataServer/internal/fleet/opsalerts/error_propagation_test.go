package opsalerts

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"

	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/supervisor"
)

type runtimeErrorMetrics struct {
	mu    sync.Mutex
	calls []string
}

func (m *runtimeErrorMetrics) RecordAlertEvaluationError(engine, category string, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, engine+":"+category)
}

func (m *runtimeErrorMetrics) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func TestEvaluatePropagatesInventoryInfrastructureErrorAndMetrics(t *testing.T) {
	cause := sql.ErrConnDone
	engine, err := NewEngine(engineTestStore{}, evaluationSource{listErr: cause})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	metrics := &runtimeErrorMetrics{}
	engine.SetRuntimeErrorMetrics(metrics)

	_, err = engine.Evaluate(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) || !errors.Is(err, cause) {
		t.Fatalf("Evaluate error = %v, want supervisor infrastructure and cause", err)
	}
	if metrics.count() != 1 {
		t.Fatalf("runtime error metrics = %d, want 1", metrics.count())
	}
}

func TestEvaluatePropagatesSnapshotInfrastructureErrorSeparatelyFromIsolatedErrors(t *testing.T) {
	engine, err := NewEngine(engineTestStore{}, evaluationSource{
		workerIDs:    []string{"infra-worker", "isolated-worker"},
		snapshotErrs: map[string]error{"infra-worker": sql.ErrConnDone, "isolated-worker": errors.New("bad worker payload")},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	metrics := &runtimeErrorMetrics{}
	engine.SetRuntimeErrorMetrics(metrics)

	_, err = engine.Evaluate(context.Background())
	if !errors.Is(err, supervisor.ErrInfrastructure) {
		t.Fatalf("Evaluate error = %v, want infrastructure", err)
	}
	if metrics.count() != 1 {
		t.Fatalf("runtime error metrics = %d, want one global infrastructure observation", metrics.count())
	}
}

var _ runtimealerts.ErrorMetrics = (*runtimeErrorMetrics)(nil)
