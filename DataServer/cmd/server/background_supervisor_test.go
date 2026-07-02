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
		{ClassOneShot, 5, 0},      // one-shot always zero
		{ClassOneShot, 0, 0},      // even with 0
		{ClassRestartable, 5, 5},  // bounded retry
		{ClassRestartable, 0, 0},  // 0 → no retries for restartable
		{ClassRestartable, -1, 0}, // negative → no retries
		{ClassCritical, 0, -1},    // 0 → infinite (-1 sentinel)
		{ClassCritical, 7, 7},     // bounded retry
		{ClassCritical, -1, -1},   // negative → infinite
		{RunnerClass(99), 5, 5},   // unknown class falls through
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

	t.Run("panic-recovery-then-block-on-ctx", func(t *testing.T) {
		silentLog(t)
		sup := NewBackgroundSupervisor()

		// Verdetto P0 #3 (Blocco 2) contract: a non-OneShot runner
		// that returns nil with a live context is treated as an
		// unexpected exit and retried. So a ClassRestartable
		// runner that wants to "succeed and stop" must block on
		// ctx.Done() instead of returning nil — the canonical
		// pattern for permanent runners (delivery, outbox,
		// forwarding) is "loop until ctx is cancelled". This test
		// pins down panic-recovery under that contract: the
		// runner panics on first invocation, succeeds on the
		// retry, then blocks on ctx.Done() until the test
		// cancels the parent ctx. We expect >= 2 runs: 1 panic
		// (counted as err) + 1 success (blocks on ctx).
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
				// Success path: block until ctx is cancelled.
				// Returning nil here would be remapped to
				// supervisor.ErrUnexpectedExit by the runLoop
				// (Verdetto P0 #3) and cause a retry, so we
				// must block instead.
				<-ctx.Done()
				return ctx.Err()
			},
		}
		if err := sup.Register(recovered); err != nil {
			t.Fatalf("register: %v", err)
		}

		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer parentCancel()

		// Cancel parent ctx after the runner has been called at
		// least 2 times (1 panic + 1 success-blocking). The
		// runner is in the success-blocking state on call 2,
		// so ctx cancel unblocks it and the supervisor exits
		// gracefully.
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
			t.Fatalf("restart-after-panic must succeed cleanly on ctx cancel, got: %v", runErr)
		}
		<-cancelDone

		if got := atomic.LoadInt32(&runs); got < minRuns {
			t.Errorf("expected >= %d runs (panic + recovery-blocking), got: %d", minRuns, got)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────
// Verdetto P0 #4 (Blocco 1.1): shouldExitAfterFailure is the single
// source of truth for the exit-after-failure rule. This table-driven
// test pins the rule matrix down so a future refactor can't regress
// the zero-case (which was the original bug: `if maxR > 0 && attempt > maxR`
// short-circuited on maxR==0, causing ClassRestartable with MaxRetries=0
// to loop forever instead of exiting on the first error).
// ─────────────────────────────────────────────────────────────────────────

func TestShouldExitAfterFailure(t *testing.T) {
	cases := []struct {
		name       string
		class      RunnerClass
		maxRetries int
		attempt    int
		want       bool
	}{
		// (a) ClassOneShot: always exit, regardless of maxRetries or attempt.
		{"OneShot/maxRetries=5/attempt=1", ClassOneShot, 5, 1, true},
		{"OneShot/maxRetries=0/attempt=1", ClassOneShot, 0, 1, true},
		{"OneShot/maxRetries=5/attempt=10", ClassOneShot, 5, 10, true},

		// (b) ClassRestartable: MaxRetries=0 → exit at first error (the bug fix).
		{"Restartable/maxRetries=0/attempt=1", ClassRestartable, 0, 1, true},
		{"Restartable/maxRetries=0/attempt=5", ClassRestartable, 0, 5, true},
		{"Restartable/maxRetries=-1/attempt=1", ClassRestartable, -1, 1, true},
		// ClassRestartable with positive budget: exit only when attempt exceeds it.
		{"Restartable/maxRetries=3/attempt=1", ClassRestartable, 3, 1, false},
		{"Restartable/maxRetries=3/attempt=3", ClassRestartable, 3, 3, false},
		{"Restartable/maxRetries=3/attempt=4", ClassRestartable, 3, 4, true},
		{"Restartable/maxRetries=3/attempt=10", ClassRestartable, 3, 10, true},

		// (c) ClassCritical: MaxRetries<=0 → never exit (infinite, only ctx cancel).
		{"Critical/maxRetries=0/attempt=1", ClassCritical, 0, 1, false},
		{"Critical/maxRetries=0/attempt=1000", ClassCritical, 0, 1000, false},
		{"Critical/maxRetries=-1/attempt=1", ClassCritical, -1, 1, false},
		// ClassCritical with positive budget: exit when attempt exceeds it.
		{"Critical/maxRetries=2/attempt=1", ClassCritical, 2, 1, false},
		{"Critical/maxRetries=2/attempt=2", ClassCritical, 2, 2, false},
		{"Critical/maxRetries=2/attempt=3", ClassCritical, 2, 3, true},

		// Defensive: unknown class exits immediately.
		{"Unknown/maxRetries=5/attempt=1", RunnerClass(99), 5, 1, true},
		{"Unknown/maxRetries=0/attempt=1", RunnerClass(99), 0, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldExitAfterFailure(tc.class, tc.maxRetries, tc.attempt)
			if got != tc.want {
				t.Errorf("shouldExitAfterFailure(%s, maxRetries=%d, attempt=%d) = %v, want %v",
					tc.class, tc.maxRetries, tc.attempt, got, tc.want)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Verdetto P0 #4 (Blocco 1.1) integration tests — case (b) and case (c)
// from the spec. Case (a) is already covered by
// TestBackgroundSupervisor_ClassOneShot_RunsOnceNeverRestarts above.
// ─────────────────────────────────────────────────────────────────────────

// Case (b): ClassRestartable with MaxRetries=0 must exit on the first
// error. Pre-fix, the `if maxR > 0 && attempt > maxR` guard short-circuited
// on maxR==0 and the runner looped forever through the backoff path.
func TestBackgroundSupervisor_ClassRestartable_ZeroMaxRetriesExitsAtFirstError(t *testing.T) {
	silentLog(t)

	sup := NewBackgroundSupervisor()
	errBoom := errors.New("restartable boom")
	var runs int32
	restartable := &SupervisedRunner{
		Name:  "zero-maxretries",
		Class: ClassRestartable,
		Policy: RestartPolicy{
			MaxRetries:     0, // zero = exit at first error (the bug fix)
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errBoom
		},
	}
	if err := sup.Register(restartable); err != nil {
		t.Fatalf("register: %v", err)
	}

	runErr := runWithTimeout(t, sup, context.Background(), 2*time.Second)
	// ClassRestartable exhaustion must NOT escalate to the supervisor's
	// return value — same invariant as the positive-budget case.
	if runErr != nil {
		t.Fatalf("ClassRestartable MaxRetries=0 exhaustion must NOT yield a fatal error, got: %v", runErr)
	}
	// Exactly one invocation: the first error must trigger exit.
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Errorf("ClassRestartable MaxRetries=0: want 1 run, got %d", got)
	}
	// Post-exhaustion, the runner must surface in Missing() so /ready
	// flips red (the same invariant as the positive-budget case).
	missing := sup.Missing()
	found := false
	for _, m := range missing {
		if m == "zero-maxretries" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected %q in Missing() post-exhaustion, got: %v", "zero-maxretries", missing)
	}
}

// Case (c): ClassCritical with MaxRetries=0 must keep retrying
// indefinitely — only parent-ctx cancellation exits the loop.
// Pre-fix, the same `maxR > 0` guard would have caused the runner to
// exit (because effectiveMaxRetries returns -1 for ClassCritical/0
// and -1 > 0 is false, so the guard would NEVER trigger — but the
// pre-fix code path was correct for this case by accident). The new
// shouldExitAfterFailure makes the contract explicit: MaxRetries<=0
// for ClassCritical means "infinite retries, only ctx cancellation
// exits". This test pins that contract down.
func TestBackgroundSupervisor_ClassCritical_ZeroMaxRetriesContinuesUntilCtxCancel(t *testing.T) {
	silentLog(t)

	sup := NewBackgroundSupervisor()
	errBoom := errors.New("critical boom")
	var runs int32
	critical := &SupervisedRunner{
		Name:  "infinite-critical",
		Class: ClassCritical,
		Policy: RestartPolicy{
			MaxRetries:     0, // zero = infinite retries for ClassCritical
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		},
		Run: func(ctx context.Context) error {
			atomic.AddInt32(&runs, 1)
			return errBoom
		},
	}
	if err := sup.Register(critical); err != nil {
		t.Fatalf("register: %v", err)
	}

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	// Cancel parent ctx as soon as the runner has been called at least
	// 5 times. The runner is called, fails, enters backoff, gets called
	// again, fails, etc. — without ctx cancel, this would loop forever.
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

	// Supervisor must exit gracefully (nil error) when parent ctx is
	// cancelled — ctx cancellation is NOT a ClassCritical exhaustion,
	// so it must NOT propagate as a fatal error.
	runErr := runWithTimeout(t, sup, parentCtx, 2*time.Second)
	if runErr != nil {
		t.Errorf("ClassCritical MaxRetries=0 with ctx cancel must NOT yield a fatal error, got: %v", runErr)
	}

	// Wait for the cancel goroutine to actually observe ctx cancellation
	// (it's polling, so there may be a brief race after the supervisor
	// exits before the goroutine sees the cancelled parent ctx).
	<-cancelDone

	// The runner must have been called at least minRuns times before
	// the ctx cancel landed — proving it kept retrying past the first
	// failure rather than exiting on the zero-budget.
	if got := atomic.LoadInt32(&runs); got < minRuns {
		t.Errorf("ClassCritical MaxRetries=0: want >= %d runs before ctx cancel, got %d", minRuns, got)
	}
}
