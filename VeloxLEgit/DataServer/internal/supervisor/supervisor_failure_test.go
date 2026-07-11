package supervisor

// supervisor_failure_test.go
//
// Failure-path tests for Supervisor.Run: ClassCritical exhaustion
// must cancel the supervisor-internal ctx and return a fatal error;
// ClassRestartable exhaustion must NOT; MaxRetries=0 must exit on
// first error for ClassRestartable and stay infinite for ClassCritical
// under parent-ctx cancellation.
import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestSupervisor_ClassCritical_ExhaustionCancelsSupCtxAndReturnsFatalErr
// verifies the critical-runner exhaustion contract: the supervisor-internal
// ctx is cancelled (cascading to siblings), but the parent ctx is NOT,
// and the supervisor returns a fatal error wrapping both the runner name
// and the underlying cause.
func TestSupervisor_ClassCritical_ExhaustionCancelsSupCtxAndReturnsFatalErr(t *testing.T) {
	silentLog(t)
	sup := New()

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	const maxRetries = 2
	errBoom := errors.New("critical boom")
	var runsCritical int32
	if err := sup.Register(Runner{
		Name: "always-fails", Class: ClassCritical,
		Policy: RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runsCritical, 1)
			return errBoom
		},
	}); err != nil {
		t.Fatalf("register critical: %v", err)
	}
	if err := sup.Register(Runner{
		Name: "cascade-sibling", Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     999,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
	}); err != nil {
		t.Fatalf("register cascade: %v", err)
	}

	runErr := runWithTimeout(t, sup, parentCtx, 2*time.Second)

	if runErr == nil {
		t.Fatal("expected fatal error, got nil")
	}
	if !errors.Is(runErr, errBoom) {
		t.Errorf("expected errBoom wrapped, got: %v", runErr)
	}
	if got := atomic.LoadInt32(&runsCritical); got != int32(maxRetries+1) {
		t.Errorf("critical runs: want %d, got %d", maxRetries+1, got)
	}
	if parentCtx.Err() != nil {
		t.Errorf("parent ctx must NOT be cancelled by critical exhaustion: %v", parentCtx.Err())
	}
}

// TestSupervisor_ClassRestartable_ExhaustionRemovesRunnerCleanly
// verifies that a ClassRestartable that exhausts its budget is removed
// without escalating to a fatal and surfaces in Missing() once the
// supervisor has fully drained.
func TestSupervisor_ClassRestartable_ExhaustionRemovesRunnerCleanly(t *testing.T) {
	silentLog(t)
	sup := New()

	const maxRetries = 3
	errTrans := errors.New("transient")
	var runs int32
	if err := sup.Register(Runner{
		Name: "transient-fails", Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errTrans
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
	if runErr != nil {
		t.Fatalf("ClassRestartable exhaustion must NOT yield fatal: %v", runErr)
	}
	if got := atomic.LoadInt32(&runs); got != int32(maxRetries+1) {
		t.Errorf("restartable runs: want %d, got %d", maxRetries+1, got)
	}
	missing := sup.Missing()
	found := false
	for _, m := range missing {
		if m == "transient-fails" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q in Missing() post-exhaustion, got: %v", "transient-fails", missing)
	}
}

// TestSupervisor_ClassRestartable_ZeroMaxRetriesExitsAtFirstError verifies
// that ClassRestartable with MaxRetries=0 exits on the first error
// (the bug fix from the old `if maxR > 0 && attempt > maxR` short-circuit).
func TestSupervisor_ClassRestartable_ZeroMaxRetriesExitsAtFirstError(t *testing.T) {
	silentLog(t)
	sup := New()
	errBoom := errors.New("restartable boom")
	var runs int32
	if err := sup.Register(Runner{
		Name: "zero-maxretries", Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     0,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errBoom
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
	if runErr != nil {
		t.Fatalf("ClassRestartable MaxRetries=0 must NOT yield fatal: %v", runErr)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Errorf("ran %d times, want 1", got)
	}
	missing := sup.Missing()
	found := false
	for _, m := range missing {
		if m == "zero-maxretries" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q in Missing(), got: %v", "zero-maxretries", missing)
	}
}

// TestSupervisor_ClassCritical_ZeroMaxRetriesContinuesUntilCtxCancel
// verifies that ClassCritical with MaxRetries=0 keeps retrying
// indefinitely until parent-ctx cancellation. Ctx cancellation is
// the only exit path; the supervisor returns nil (graceful shutdown).
func TestSupervisor_ClassCritical_ZeroMaxRetriesContinuesUntilCtxCancel(t *testing.T) {
	silentLog(t)
	sup := New()
	errBoom := errors.New("critical boom")
	var runs int32
	if err := sup.Register(Runner{
		Name: "infinite-critical", Class: ClassCritical,
		Policy: RestartPolicy{
			MaxRetries:     0,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errBoom
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	const minRuns = 5
	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		for {
			if atomic.LoadInt32(&runs) >= minRuns {
				parentCancel()
				return
			}
			select {
			case <-time.After(1 * time.Millisecond):
			case <-parentCtx.Done():
				return
			}
		}
	}()

	runErr := runWithTimeout(t, sup, parentCtx, 2*time.Second)
	if runErr != nil {
		t.Errorf("must NOT yield fatal (ctx cancel is not exhaustion): %v", runErr)
	}
	<-cancelDone
	if got := atomic.LoadInt32(&runs); got < minRuns {
		t.Errorf("expected >= %d runs, got %d", minRuns, got)
	}
}
