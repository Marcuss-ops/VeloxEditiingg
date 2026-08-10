package supervisor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestSupervisorRetriesAlertInfrastructureFailure(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := New()
	if err := s.Register(Runner{
		Name:   "alert-engine",
		Class:  ClassRestartable,
		Policy: RestartPolicy{MaxRetries: 2, RestartOnPanic: true},
		Run: func(context.Context) error {
			calls.Add(1)
			if calls.Load() < 3 {
				return errors.Join(ErrInfrastructure, errors.New("alert datasource offline"))
			}
			cancel()
			return nil
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("runner calls = %d, want 3 (two retries then recovery)", got)
	}
}
