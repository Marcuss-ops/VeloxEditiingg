package alertengine

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runtimealerts "velox-server/internal/alerts"

	"velox-server/internal/supervisor"
)

func TestRunPropagatesInfrastructureToSupervisorRetry(t *testing.T) {
	engine := New(time.Millisecond, nil)
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.AddRule(func(context.Context) (*runtimealerts.AlertEvent, error) {
		call := calls.Add(1)
		if call < 3 {
			return nil, sql.ErrConnDone
		}
		cancel()
		return nil, nil
	})

	sup := supervisor.New()
	if err := sup.Register(supervisor.Runner{
		Name:  "alert-engine",
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
	if got := calls.Load(); got != 3 {
		t.Fatalf("rule calls = %d, want 3", got)
	}
}
