package supervisor

// supervisor_lifecycle_test.go
//
// Lifecycle tests for Supervisor.Run: ClassOneShot must run exactly
// once; Register must reject bad inputs; PanicRecovery must convert
// a panic into a restartable failure under RestartOnPanic=true.
//
// This file holds the shared test helpers (silentLog, runWithTimeout)
// used by the failure-test and readiness-test files in the same
// package.
import (
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

// silentLog redirects the default logger to io.Discard for the
// duration of each test.
func silentLog(t *testing.T) {
	t.Helper()
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
}

// runWithTimeout launches sup.Run(parentCtx) in a goroutine and blocks
// until either Run returns or the deadline expires. A timeout is a
// deadlock / restart-loop regression, surfaced as a t.Fatal so the
// test process doesn't hang.
func runWithTimeout(t *testing.T, sup *Supervisor, parentCtx context.Context, max time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- sup.Run(parentCtx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(max):
		t.Fatalf("sup.Run did not return within %s — likely deadlock or restart-loop regression", max)
		return nil
	}
}

// TestSupervisor_ClassOneShot_RunsOnceNeverRestarts verifies that
// ClassOneShot runners run exactly once, regardless of Run's return.
func TestSupervisor_ClassOneShot_RunsOnceNeverRestarts(t *testing.T) {
	t.Run("success path — runs once, Missing() empty", func(t *testing.T) {
		silentLog(t)
		sup := New()
		var runs int32
		if err := sup.Register(Runner{
			Name: "manifest-gen", Class: ClassOneShot,
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&runs, 1)
				return nil
			},
		}); err != nil {
			t.Fatalf("register: %v", err)
		}
		runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
		if runErr != nil {
			t.Fatalf("ClassOneShot clean exit must NOT yield error: %v", runErr)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Errorf("ran %d times, want 1", got)
		}
		if m := sup.Missing(); len(m) != 0 {
			t.Errorf("clean OneShot must NOT appear in Missing(): %v", m)
		}
	})

	t.Run("error path — runs once, never restarts, supervisor stays silent", func(t *testing.T) {
		silentLog(t)
		sup := New()
		var runs int32
		errBoom := errors.New("one-shot boom")
		if err := sup.Register(Runner{
			Name: "setup-task", Class: ClassOneShot,
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&runs, 1)
				return errBoom
			},
		}); err != nil {
			t.Fatalf("register: %v", err)
		}
		runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
		if runErr != nil {
			t.Errorf("ClassOneShot error must NOT escalate: %v", runErr)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Errorf("ran %d times on error, want 1", got)
		}
		if m := sup.Missing(); len(m) != 0 {
			t.Errorf("errored OneShot must NOT appear in Missing(): %v", m)
		}
	})
}

// TestSupervisor_PanicRecovery verifies that a panic inside Run is
// recovered when RestartOnPanic=true, counted as a restartable failure,
// and the runner is restarted.
func TestSupervisor_PanicRecovery(t *testing.T) {
	silentLog(t)
	sup := New()
	var runs int32
	if err := sup.Register(Runner{
		Name: "panic-recovery", Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     5,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
			RestartOnPanic: true,
		},
		Run: func(ctx context.Context) error {
			n := atomic.AddInt32(&runs, 1)
			if n == 1 {
				panic("first invocation boom")
			}
			// Success path: block until ctx is cancelled.
			// Returning nil here would be remapped to ErrUnexpectedExit
			// by the runLoop, so we must block instead.
			<-ctx.Done()
			return ctx.Err()
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	const minRuns = 2
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
		t.Fatalf("restart-after-panic must succeed on ctx cancel: %v", runErr)
	}
	<-cancelDone
	if got := atomic.LoadInt32(&runs); got < minRuns {
		t.Errorf("expected >= %d runs (panic + recovery), got %d", minRuns, got)
	}
}

// TestSupervisor_Register_Validation verifies Register rejects bad inputs.
func TestSupervisor_Register_Validation(t *testing.T) {
	silentLog(t)
	sup := New()

	// Empty name.
	if err := sup.Register(Runner{Name: "", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }}); err == nil {
		t.Error("register with empty name must fail")
	}
	// Nil Run.
	if err := sup.Register(Runner{Name: "x", Class: ClassOneShot}); err == nil {
		t.Error("register with nil Run must fail")
	}
	// Happy path.
	ok := Runner{Name: "ok", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }}
	if err := sup.Register(ok); err != nil {
		t.Errorf("register ok runner failed: %v", err)
	}
	// Duplicate name rejected.
	if err := sup.Register(ok); err == nil {
		t.Error("registering duplicate name must fail")
	}
}
