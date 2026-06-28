package main

// PR-SUPERVISOR-TAXONOMY unit tests.
//
// Covers the three restart-policy invariants the supervisor must
// enforce and a couple of safety-net tests for the helpers around
// them (effectiveMaxRetries, Register validation, panic recovery).
//
// Conventions:
//   * Time-driven backoff uses sub-millisecond values so tests run
//     well under the 200ms poll deadline; sleepCtx is ctx-aware so
//     parent cancel short-circuits any retry-loop wait.
//   * Run is invoked in a goroutine guarded by a 2-second hard
//     timeout. Anything > 1s without test progress is a real bug.
//   * log.Printf spam from the supervisor is silenced via a package-
//     scoped silentLog helper (we never want a noisy CI run).

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// silentLog redirects the default logger to io.Discard for the
// duration of each test. Cheap, safe to call concurrently because
// log.SetOutput is mutex-guarded internally.
func silentLog(t *testing.T) {
	t.Helper()
	prev := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prev) })
}

// runWithTimeout launches sup.Run(parentCtx) in a goroutine and blocks
// until either Run returns or the deadline expires. Returns the error
// Run produced (nil on graceful completion, context.Canceled when
// the parent ctx was the cause, or a fatalErr-style sentinel for
// ClassCritical exhaustion).
//
// Using a real goroutine (rather than invoking sup.Run synchronously
// inside the test goroutine) is intentional: the supervisor wg.Wait's
// on every runner goroutine, and any deadlock or leak surfaces as a
// fail-loud timeout rather than a hung test process.
func runWithTimeout(t *testing.T, sup *BackgroundSupervisor, parentCtx context.Context, max time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- sup.Run(parentCtx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(max):
		t.Fatalf("sup.Run did not return within %s — likely a deadlock or restart-loop regression", max)
		return nil
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Invariant 1: ClassCritical exhaustion cancels the SUPERVISOR-internal
// ctx (not the parent ctx) AND returns a fatal error wrapping both
// the runner name and the underlying reason.
//
// Two runners are registered:
//   * critical   — fails immediately, MaxRetries=2 (so 3 total invocations).
//   * cascade    — Restartable that blocks on ctx.Done(). When critical
//                  exhausts and calls supCancel(), the cascade Run must
//                  observe the cancellation. If it were the parent ctx
//                  being cancelled, the supervisor's `defer supCancel()`
//                  would still drain — but the test enforces the more
//                  specific invariant: parent ctx MUST remain alive.
// ─────────────────────────────────────────────────────────────────────────

func TestBackgroundSupervisor_ClassCritical_ExhaustionCancelsSupCtxAndReturnsFatalErr(t *testing.T) {
	silentLog(t)

	sup := NewBackgroundSupervisor()

	// Declare parent ctx BEFORE the closures so the cascade runner
	// captures it directly. Less indirection = less surface area
	// for closure-capture bugs.
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	const maxRetries = 2
	errBoom := errors.New("critical boom")
	var runsCritical int32
	critical := &SupervisedRunner{
		Name:  "always-fails",
		Class: ClassCritical,
		Policy: RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runsCritical, 1)
			return errBoom
		},
	}

	// Cascade sibling: blocks on its own ctx (which is the supervisor-
	// internal ctx, NOT the test's parent ctx). Because the cascade
	// is a Restartable with effectively-infinite retries, the only
	// way it can exit cleanly is for its ctx to be cancelled — and
	// the only ctx it observes is supCtx.
	cascade := &SupervisedRunner{
		Name:  "cascade-sibling",
		Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     999,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			// Returning nil here signals "clean exit" to runLoop;
			// since the supervisor's supCtx is what was cancelled,
			// the runLoop still treats this as graceful shutdown.
			return nil
		},
	}

	if err := sup.Register(critical); err != nil {
		t.Fatalf("register critical: %v", err)
	}
	if err := sup.Register(cascade); err != nil {
		t.Fatalf("register cascade: %v", err)
	}

	runErr := runWithTimeout(t, sup, parentCtx, 2*time.Second)

	// 1. Run returned a fatal error wrapping both the runner name
	//    AND the underlying errBoom (verifies %w wrapping).
	if runErr == nil {
		t.Fatal("expected fatal error from ClassCritical exhaustion, got nil")
	}
	if !strings.Contains(runErr.Error(), "always-fails") {
		t.Errorf("expected error to name the runner, got: %v", runErr)
	}
	if !errors.Is(runErr, errBoom) {
		t.Errorf("expected errBoom wrapped in fatal, got: %v", runErr)
	}

	// 2. Critical ran exactly MaxRetries+1 times (1 initial + 2 retries).
	if got := atomic.LoadInt32(&runsCritical); got != int32(maxRetries+1) {
		t.Errorf("critical invocation count: want %d, got %d", maxRetries+1, got)
	}

	// 3. Parent ctx MUST still be alive — critical fatally cancelled
	//    the SUPERVISOR ctx, not the parent's.
	if parentCtx.Err() != nil {
		t.Errorf("parent ctx must NOT be cancelled by ClassCritical exhaustion, got: %v", parentCtx.Err())
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Invariant 2: ClassRestartable exhaustion does NOT cancel any context
// and does NOT propagate a fatal error. The runner is "removed cleanly"
// — i.e. Missing() surfaces it as expected-but-dead after exhaustion.
// ─────────────────────────────────────────────────────────────────────────

func TestBackgroundSupervisor_ClassRestartable_ExhaustionRemovesRunnerCleanly(t *testing.T) {
	silentLog(t)

	sup := NewBackgroundSupervisor()

	const maxRetries = 3
	errTrans := errors.New("transient")
	var runsRestartable int32
	restartable := &SupervisedRunner{
		Name:  "transient-fails",
		Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runsRestartable, 1)
			return errTrans
		},
	}
	if err := sup.Register(restartable); err != nil {
		t.Fatalf("register: %v", err)
	}

	// The supervisor's Run must return nil once wg.Wait() drains —
	// a ClassRestartable that exhausts must NEVER escalate to fatal.
	runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
	if runErr != nil {
		t.Fatalf("ClassRestartable exhaustion must NOT yield a fatal error, got: %v", runErr)
	}

	if got := atomic.LoadInt32(&runsRestartable); got != int32(maxRetries+1) {
		t.Errorf("restartable invocation count: want %d, got %d", maxRetries+1, got)
	}

	// Once the supervisor goroutine has fully exited, the runner
	// must show up in Missing() — i.e. it is dead but the supervisor
	// still owns the registration, so /ready should fail loud on it.
	missing := sup.Missing()
	foundTransient := false
	for _, m := range missing {
		if m == "transient-fails" {
			foundTransient = true
			break
		}
	}
	if !foundTransient {
		t.Errorf("expected %q in Missing() post-exhaustion, got: %v", "transient-fails", missing)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Invariant 3: ClassOneShot runs exactly once — even when Run returns
// an error — and never restarts.
// ─────────────────────────────────────────────────────────────────────────

func TestBackgroundSupervisor_ClassOneShot_RunsOnceNeverRestarts(t *testing.T) {
	silentLog(t)

	t.Run("success path — runs once, Missing() empty", func(t *testing.T) {
		silentLog(t)
		sup := NewBackgroundSupervisor()
		var runs int32
		manifest := &SupervisedRunner{
			Name:  "manifest-gen",
			Class: ClassOneShot,
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&runs, 1)
				return nil
			},
		}
		if err := sup.Register(manifest); err != nil {
			t.Fatalf("register: %v", err)
		}

		runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
		if runErr != nil {
			t.Fatalf("ClassOneShot clean exit must NOT yield an error, got: %v", runErr)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Errorf("ClassOneShot ran more than once: got %d, want 1", got)
		}
		// One-shot that exits cleanly MUST NOT be flagged in Missing().
		if m := sup.Missing(); len(m) != 0 {
			t.Errorf("ClassOneShot clean exit must NOT appear in Missing(), got: %v", m)
		}
	})

	t.Run("error path — runs once, never restarts, supervisor stays silent", func(t *testing.T) {
		silentLog(t)
		sup := NewBackgroundSupervisor()
		var runs int32
		errBoom := errors.New("one-shot boom")
		setup := &SupervisedRunner{
			Name:  "setup-task",
			Class: ClassOneShot,
			Run: func(ctx context.Context) error {
				atomic.AddInt32(&runs, 1)
				return errBoom
			},
		}
		if err := sup.Register(setup); err != nil {
			t.Fatalf("register: %v", err)
		}

		runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
		// One-shot error must NOT propagate as the supervisor's
		// return value — supervisor only escalates ClassCritical.
		if runErr != nil {
			t.Errorf("ClassOneShot error must NOT escalate to supervisor return, got: %v", runErr)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Errorf("ClassOneShot ran more than once on failure: got %d, want 1", got)
		}
		// One-shot that errored out SHOULD NOT appear in Missing()
		// either — clean-exit semantics; the error is logged as WARN.
		if m := sup.Missing(); len(m) != 0 {
			t.Errorf("ClassOneShot error must NOT appear in Missing(), got: %v", m)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────
// Safety-net coverage for the helpers around the three invariants.
// ─────────────────────────────────────────────────────────────────────────

func TestEffectiveMaxRetries(t *testing.T) {
	cases := []struct {
		class RunnerClass
		n     int
		want  int
	}{
		{ClassOneShot, 5, 0},     // one-shot always zero
		{ClassOneShot, 0, 0},     // even with 0
		{ClassRestartable, 5, 5}, // bounded retry
		{ClassRestartable, 0, 0}, // 0 → no retries for restartable
		{ClassRestartable, -1, 0}, // negative → no retries
		{ClassCritical, 0, -1},   // 0 → infinite (-1 sentinel)
		{ClassCritical, 7, 7},    // bounded retry
		{ClassCritical, -1, -1},  // negative → infinite
		{RunnerClass(99), 5, 5},  // unknown class falls through
	}
	for _, tc := range cases {
		got := effectiveMaxRetries(tc.class, tc.n)
		if got != tc.want {
			t.Errorf("effectiveMaxRetries(%s, %d) = %d, want %d", tc.class, tc.n, got, tc.want)
		}
	}
}

func TestBackgroundSupervisor_Register_Validation(t *testing.T) {
	silentLog(t)
	sup := NewBackgroundSupervisor()

	// Nil runner.
	if err := sup.Register(nil); err == nil {
		t.Error("Register(nil) must return error")
	}

	// Empty name.
	if err := sup.Register(&SupervisedRunner{Name: "", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }}); err == nil {
		t.Error("Register(empty name) must return error")
	}

	// Nil Run func.
	if err := sup.Register(&SupervisedRunner{Name: "x", Class: ClassOneShot}); err == nil {
		t.Error("Register(nil Run) must return error")
	}

	// Happy path: Sane one-shot registers fine.
	if err := sup.Register(&SupervisedRunner{Name: "ok", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }}); err != nil {
		t.Errorf("Register(sane one-shot) returned error: %v", err)
	}

	// Duplicate name rejected.
	if err := sup.Register(&SupervisedRunner{Name: "ok", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }}); err == nil {
		t.Error("Register(duplicate name) must return error")
	}
}

func TestBackgroundSupervisor_PanicRecovery(t *testing.T) {
	silentLog(t)
	sup := NewBackgroundSupervisor()

	// Restartable runner that panics once, then succeeds on the
	// retry. With RestartOnPanic=true, the panic is converted to
	// an error and the restart loop retries. We expect exactly 2
	// runs to land: 1 panic (counted as err) + 1 success.
	var runs int32
	recovered := &SupervisedRunner{
		Name:  "panic-recovery",
		Class: ClassRestartable,
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
			return nil
		},
	}
	if err := sup.Register(recovered); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
	if runErr != nil {
		t.Fatalf("restart-after-panic must succeed cleanly, got: %v", runErr)
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Errorf("expected 2 runs (panic + recovery), got: %d", got)
	}
}
