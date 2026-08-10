package opsalerts

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/supervisor"
)

type retrySource struct {
	calls  atomic.Int32
	cancel context.CancelFunc
}

func (s *retrySource) WorkerIDs(CallCtx) ([]string, error) {
	call := s.calls.Add(1)
	if call < 3 {
		return nil, sql.ErrConnDone
	}
	s.cancel()
	return nil, nil
}

func (*retrySource) Snapshot(CallCtx, string) (*WorkerSnapshot, error) { return nil, nil }

func TestRunPropagatesInfrastructureToSupervisorRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &retrySource{cancel: cancel}
	engine, err := NewEngineWithClock(engineTestStore{}, source, time.Millisecond, 1)
	if err != nil {
		t.Fatalf("NewEngineWithClock: %v", err)
	}

	sup := supervisor.New()
	if err := sup.Register(supervisor.Runner{
		Name:  "opsalerts",
		Class: supervisor.ClassRestartable,
		Policy: supervisor.RestartPolicy{
			MaxRetries:     2,
			RestartOnPanic: true,
		},
		Run: engine.Run,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := sup.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Supervisor.Run: %v", err)
	}
	if got := source.calls.Load(); got != 3 {
		t.Fatalf("WorkerIDs calls = %d, want 3", got)
	}
}
