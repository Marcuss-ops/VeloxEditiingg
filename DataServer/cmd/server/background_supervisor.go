package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"velox-server/internal/supervisor"
)

// ── Runner taxonomy ────────────────────────────────────────────────────────
//
// PR-SUPERVISOR-TAXONOMY: every background runner has a class that
// determines what its restart policy should be when the runner exits
// with a non-nil error.
//
//   * ClassOneShot — runs once, exits, never restarted. Use for
//     fire-and-forget setup tasks (manifest generation, schema seeding)
//     where a failure is not recoverable but also not fatal.
//   * ClassRestartable — runs forever; on failure, restart with
//     exponential backoff (bounded by Policy.MaxRetries). After the
//     retry budget is exhausted the runner is removed and the
//     supervisor emits a WARN; the master keeps running.
//   * ClassCritical — runs forever; on failure, restart forever with
//     exponential backoff (Policy.MaxRetries ignored or 0=infinite).
//     After a long streak of unfixable failures, the supervisor cancels
//     its internal context (cascading to every other runner) and
//     returns the original error to runServer, which propagates a
//     fail-loud so Kubernetes restarts the pod. This is the desired
//     behaviour for outbox-dispatcher and delivery-runner: if they
//     ever exit, the master is dead in the water.

type RunnerClass int

const (
	ClassOneShot     RunnerClass = iota // run once, never restart
	ClassRestartable                    // restart on failure, bounded retries + backoff
	ClassCritical                       // restart forever, eventually fail-loud the master
)

func (c RunnerClass) String() string {
	switch c {
	case ClassOneShot:
		return "one-shot"
	case ClassRestartable:
		return "restartable"
	case ClassCritical:
		return "critical"
	default:
		return fmt.Sprintf("unknown-class(%d)", int(c))
	}
}

// ── RestartPolicy ──────────────────────────────────────────────────────────

// RestartPolicy drives the restart loop's backoff schedule. MaxRetries
// is interpreted in the context of Class:
//
//   - ClassOneShot: ignored (always zero restarts)
//   - ClassRestartable: bounded retry count; after this many restarts
//     the runner is removed and the supervisor logs WARN.
//   - ClassCritical: if zero, restart infinitely; if positive, restart
//     at most this many times before the supervisor cancels the
//     internal context and returns error to the caller.
//
// InitialBackoff doubles after each attempt until MaxBackoff. Zero
// Init/Max means "no sleep between restarts" (use only for extremely
// healthy cases — typically InitialBackoff=500ms, MaxBackoff=30s).
type RestartPolicy struct {
	MaxRetries     int           // 0 = infinite for ClassCritical; ignored for ClassOneShot
	InitialBackoff time.Duration // first retry delay; doubled after each subsequent retry
	MaxBackoff     time.Duration // cap on exponential growth
	RestartOnPanic bool          // when true, recover from a panic and treat as a failure
}

func (p RestartPolicy) backoffFor(attempt int) time.Duration {
	if attempt <= 0 || p.InitialBackoff <= 0 {
		return 0
	}
	// Clamp attempt to a sane window so we don't overflow Duration arithmetic.
	const capAttempt = 30
	n := attempt
	if n > capAttempt {
		n = capAttempt
	}
	// Shift by n-1 so attempt=1 gives InitialBackoff (not ×2).
	d := p.InitialBackoff << (n - 1)
	// MaxBackoff == 0 means no cap — only clamp when positive.
	if d <= 0 {
		return 24 * time.Hour
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// ── RunnerState ───────────────────────────────────────────────────────────

// RunnerState tracks the current lifecycle phase of a supervised runner.
// The /ready probe uses this to fail when a critical runner is BACKING_OFF,
// FAILED, or STOPPED instead of relying on a simple alive/dead boolean.
type RunnerState string

const (
	RunnerStarting   RunnerState = "STARTING"
	RunnerRunning    RunnerState = "RUNNING"
	RunnerBackingOff RunnerState = "BACKING_OFF"
	RunnerStopped    RunnerState = "STOPPED"
	RunnerFailed     RunnerState = "FAILED"
)

func (s RunnerState) IsHealthy() bool {
	return s == RunnerStarting || s == RunnerRunning
}

// ── SupervisedRunner ───────────────────────────────────────────────────────

// SupervisedRunner is the canonical unit the supervisor manages. Each
// runner has:
//
//   - Name — unique within a supervisor; duplicates are rejected.
//   - Class — drives the restart policy semantics.
//   - Policy — zero-policies are interpreted as "no backoff, no retries".
//   - Run — the actual loop body. When it returns nil, the supervisor
//     treats it as a clean exit (no restart regardless of Class).
type SupervisedRunner struct {
	Name   string
	Class  RunnerClass
	Policy RestartPolicy
	// Run is the loop body. Must respect ctx cancellation. Returning
	// non-nil triggers the restart policy; returning nil is treated as
	// a clean exit.
	Run func(ctx context.Context) error
}

// ── BackgroundRunner interface (kept for back-compat) ────────────────────

// BackgroundRunner preserves the pre-PR-SUPERVISOR-TAXONOMY interface
// so legacy call sites (the metrics-supervisor type, internal/metrics)
// keep working without a type shim. New call sites should use
// SupervisedRunner via Register.
type BackgroundRunner interface {
	Name() string
	Run(ctx context.Context) error
}

// RunnerFunc is the convenience adapter for simple ad-hoc goroutines.
// In the new taxonomy its default Class is ClassOneShot (legacy
// semantics: run once, exit, no restart). Wrap explicitly via
// SupervisedRunner for restart-aware behaviour.
type RunnerFunc struct {
	name string
	fn   func(ctx context.Context) error
}

func (r RunnerFunc) Name() string                  { return r.name }
func (r RunnerFunc) Run(ctx context.Context) error { return r.fn(ctx) }

// ── BackgroundSupervisor ──────────────────────────────────────────────────

// BackgroundSupervisor owns a set of SupervisedRunner entries and
// orchestrates their lifecycle:
//
//   - Start every runner in its own goroutine.
//   - On unexpected exit (non-nil error), apply the runner's
//     Class-specific restart loop with exponential backoff.
//   - Cancel the supervisor-internal context when:
//     (a) the parent ctx is cancelled (graceful shutdown), OR
//     (b) a ClassCritical runner exhausts its retry budget.
//   - Return from Run when ALL runners have stopped.
type BackgroundSupervisor struct {
	runners []SupervisedRunner

	// states tracks the lifecycle state of each registered runner.
	// A runner transitions STARTING → RUNNING → (BACKING_OFF → RUNNING)* → STOPPED/FAILED.
	// The /ready probe uses IsHealthy() to determine readiness.
	mu     sync.RWMutex
	states map[string]RunnerState
}

// NewBackgroundSupervisor creates an empty supervisor.
func NewBackgroundSupervisor() *BackgroundSupervisor {
	return &BackgroundSupervisor{
		states: make(map[string]RunnerState),
	}
}

// Register adds a SupervisedRunner to the supervisor. Duplicate names
// are rejected (must-fail at composition time — a misconfigured
// supervisor is a startup bug, not a runtime recovery scenario).
//
// Back-compat: callers can pass a RunnerFunc as a *BackgroundRunner;
// the supervisor wraps it as a ClassOneShot SupervisedRunner with no
// restart budget.
func (s *BackgroundSupervisor) Register(r interface{}) error {
	if r == nil {
		return fmt.Errorf("supervisor: nil runner")
	}

	switch v := r.(type) {
	case *SupervisedRunner:
		if v == nil {
			return fmt.Errorf("supervisor: nil *SupervisedRunner")
		}
		return s.register(*v)
	case SupervisedRunner:
		return s.register(v)
	case BackgroundRunner:
		// Legacy: back-compat with BackgroundRunner.
		if v == nil {
			return fmt.Errorf("supervisor: nil BackgroundRunner")
		}
		if v.Name() == "" {
			return fmt.Errorf("supervisor: runner %T has empty Name()", v)
		}
		return s.register(SupervisedRunner{
			Name:   v.Name(),
			Class:  ClassOneShot,
			Run:    v.Run,
			Policy: RestartPolicy{},
		})
	default:
		return fmt.Errorf("supervisor: unsupported runner type %T (want *SupervisedRunner, SupervisedRunner, or BackgroundRunner)", r)
	}
}

func (s *BackgroundSupervisor) register(r SupervisedRunner) error {
	if r.Name == "" {
		return fmt.Errorf("supervisor: runner has empty Name()")
	}
	if r.Run == nil {
		return fmt.Errorf("supervisor: runner %q has nil Run", r.Name)
	}
	for _, existing := range s.runners {
		if existing.Name == r.Name {
			return fmt.Errorf("supervisor: duplicate runner name %q", r.Name)
		}
	}
	s.runners = append(s.runners, r)
	log.Printf("[SUPERVISOR] registered runner: name=%s class=%s max_retries=%d",
		r.Name, r.Class.String(), effectiveMaxRetries(r.Class, r.Policy.MaxRetries))
	return nil
}

// effectiveMaxRetries returns the policy's effective ceiling for the
// given class. ClassCritical with MaxRetries=0 returns -1 (infinite).
func effectiveMaxRetries(c RunnerClass, n int) int {
	switch c {
	case ClassOneShot:
		return 0
	case ClassRestartable:
		if n < 0 {
			return 0
		}
		return n
	case ClassCritical:
		if n <= 0 {
			return -1
		}
		return n
	default:
		return n
	}
}

// shouldExitAfterFailure is the single source of truth for the
// Verdetto P0 #4 exit-after-failure rule. Given a runner of class c
// configured with maxRetries (the RAW Policy.MaxRetries value, NOT
// the effective budget from effectiveMaxRetries) and the current
// 1-based attempt count, it reports whether the runLoop should exit
// (stop retrying) after the current failed attempt.
//
// Rule matrix:
//
//	ClassOneShot,   maxRetries=*    → true  (fire-and-forget, no retry)
//	ClassRestartable, maxRetries=0  → true  (bounded=0 → exit on first error)
//	ClassRestartable, maxRetries>0  → attempt > maxRetries
//	ClassCritical,   maxRetries<=0  → false (0 or negative = infinite; only
//	                                    ctx cancellation exits the loop)
//	ClassCritical,   maxRetries>0  → attempt > maxRetries
//	unknown class                    → true  (defensive: don't loop forever)
//
// Why this replaces the old `if maxR > 0 && attempt > maxR` check:
// the old guard short-circuited on maxR==0, so a ClassRestartable
// runner with MaxRetries=0 never exhausted — it fell through to the
// backoff/sleep path and looped forever, which is the opposite of
// the intended "zero retries = exit on first error" semantic. The
// single-function centralization makes the zero-case explicit and
// testable in isolation, and aligns ClassCritical with the
// conventional infinite-retry sentinel (maxRetries <= 0).
func shouldExitAfterFailure(c RunnerClass, maxRetries int, attempt int) bool {
	switch c {
	case ClassOneShot:
		return true
	case ClassRestartable:
		if maxRetries <= 0 {
			return true
		}
		return attempt > maxRetries
	case ClassCritical:
		if maxRetries <= 0 {
			return false // 0 or negative = infinite; ctx cancellation is the only exit
		}
		return attempt > maxRetries
	default:
		return true // unknown class: don't loop forever
	}
}

// Run starts every registered runner in its own goroutine and blocks
// until ALL runners have exited.
//
// Returns nil when the parent ctx was cancelled (graceful shutdown).
// Returns a non-nil error when a ClassCritical runner exhausts its
// retry budget; in that case the supervisor has already cancelled
// its internal context so all OTHER runners are torn down at the
// same time.
func (s *BackgroundSupervisor) Run(ctx context.Context) error {
	if len(s.runners) == 0 {
		log.Printf("[SUPERVISOR] no runners registered — supervisor idle")
		<-ctx.Done()
		return ctx.Err()
	}

	supCtx, supCancel := context.WithCancel(ctx)
	defer supCancel()

	var wg sync.WaitGroup
	wg.Add(len(s.runners))

	var fatalMu sync.Mutex
	var fatalErr error

	for _, r := range s.runners {
		r := r
		// Pre-mark the runner as STARTING so a /ready check before
		// runLoop starts doesn't gate-fail spuriously.
		s.mu.Lock()
		s.states[r.Name] = RunnerStarting
		s.mu.Unlock()
		go func() {
			defer wg.Done()
			defer func() {
				s.mu.Lock()
				// Preserve RunnerFailed set by runLoop on exhaustion —
				// only demote to Stopped if the runner exited cleanly.
				if s.states[r.Name] != RunnerFailed {
					s.states[r.Name] = RunnerStopped
				}
				s.mu.Unlock()
			}()
			s.runLoop(supCtx, &fatalMu, &fatalErr, r, supCancel)
		}()
	}

	log.Printf("[SUPERVISOR] %d runners started", len(s.runners))
	wg.Wait()
	log.Printf("[SUPERVISOR] all runners stopped")
	fatalMu.Lock()
	defer fatalMu.Unlock()
	return fatalErr
}

// runLoop is the per-runner goroutine body. Honours the runner's
// Class and Policy:
//
//   - ClassOneShot:    Run once; non-nil error is logged WARN; no restart.
//   - ClassRestartable: Run, exponential backoff, bounded retries. On
//     exhaustion log WARN and stop looping.
//   - ClassCritical:   Run, exponential backoff, infinite retries (or
//     up to Policy.MaxRetries if non-zero). On
//     exhaustion cancel supCtx and stash the error
//     so Run() propagates it as the supervisor's
//     return value.
func (s *BackgroundSupervisor) runLoop(
	ctx context.Context,
	fatalMu *sync.Mutex,
	fatalErr *error,
	r SupervisedRunner,
	supCancel context.CancelFunc,
) {
	attempt := 0
	// maxR is the EFFECTIVE retry budget (raw MaxRetries mapped through
	// effectiveMaxRetries: 0 for OneShot, n for Restartable, -1 for
	// ClassCritical-with-0). It's used in the log lines that communicate
	// the retry ceiling to operators — the "retry N/ceiling" format
	// needs the effective budget so ClassCritical/MaxRetries=0 reads
	// as "retry 1/inf" (infinite) rather than "retry 1/0" (which would
	// misleadingly suggest the runner has no retries left). The exit
	// decision itself goes through shouldExitAfterFailure which takes
	// the raw Policy.MaxRetries.
	maxR := effectiveMaxRetries(r.Class, r.Policy.MaxRetries)
	for {
		// Transition to RUNNING before each invocation.
		s.mu.Lock()
		s.states[r.Name] = RunnerRunning
		s.mu.Unlock()

		log.Printf("[SUPERVISOR] starting runner: name=%s class=%s attempt=%d/%s",
			r.Name, r.Class.String(), attempt+1, retryCeilingString(maxR))
		err := safeCall(ctx, r.Run, r.Policy.RestartOnPanic, r.Name)

		// Graceful shutdown path: parent ctx cancelled mid-run.
		if ctx.Err() != nil {
			log.Printf("[SUPERVISOR] runner %s exiting: %v", r.Name, ctx.Err())
			return
		}

		// Verdetto P0 #3 (Blocco 2): nil err with a LIVE ctx is a
		// false-success path for permanent runners. A ClassOneShot
		// runner may legitimately return nil (fire-and-forget); for
		// ClassRestartable + ClassCritical we MUST treat a nil
		// return as an unexpected exit and feed it into the
		// restart machinery. Without this guard, a permanent runner
		// that silently dies (e.g. the loop body's last iteration
		// returns nil while the context is still live) would be
		// marked STOPPED and never restarted, leaving the master
		// serving with a dead delivery / forwarding / outbox
		// pipeline. Verdetto: 'un ritorno nil mentre il contesto è
		// ancora attivo non è una conclusione corretta: è una
		// morte inaspettata'.
		if err == nil && r.Class != ClassOneShot {
			err = supervisor.ErrUnexpectedExit
			log.Printf("[SUPERVISOR] runner %s returned nil err with live ctx (class=%s); treating as supervisor.ErrUnexpectedExit",
				r.Name, r.Class.String())
		}

		// Clean exit — Run returned nil (only possible for
		// ClassOneShot at this point, since other classes were
		// re-mapped to ErrUnexpectedExit above).
		if err == nil {
			s.mu.Lock()
			s.states[r.Name] = RunnerStopped
			s.mu.Unlock()
			log.Printf("[SUPERVISOR] runner %s exited cleanly", r.Name)
			return
		}

		attempt++

		// Mark as FAILED before deciding whether to restart.
		s.mu.Lock()
		s.states[r.Name] = RunnerFailed
		s.mu.Unlock()

		switch r.Class {
		case ClassOneShot:
			log.Printf("[SUPERVISOR] runner %s one-shot failed (NOT restarted): class=%s err=%v",
				r.Name, r.Class.String(), err)
			return

		case ClassRestartable:
			// Verdetto P0 #4 (Blocco 1.1): shouldExitAfterFailure
			// replaces the old `if maxR > 0 && attempt > maxR`
			// guard, which short-circuited on maxR==0 and caused
			// ClassRestartable with MaxRetries=0 to loop forever
			// instead of exiting on the first error. The new
			// function centralizes the rule: MaxRetries=0 → exit
			// on first error; MaxRetries>0 → exit when attempt
			// exceeds the budget.
			if shouldExitAfterFailure(r.Class, r.Policy.MaxRetries, attempt) {
				log.Printf("[SUPERVISOR] runner %s restartable budget EXHAUSTED after %d attempts; removing from supervisor: class=%s last_err=%v",
					r.Name, r.Policy.MaxRetries, r.Class.String(), err)
				return
			}
			s.mu.Lock()
			s.states[r.Name] = RunnerBackingOff
			s.mu.Unlock()
			delay := r.Policy.backoffFor(attempt)
			log.Printf("[SUPERVISOR] runner %s FAILED (restartable); sleeping %s before retry %d/%s: err=%v",
				r.Name, delay, attempt+1, retryCeilingString(maxR), err)
			if !sleepCtx(ctx, delay) {
				log.Printf("[SUPERVISOR] runner %s restartable: ctx cancelled during backoff", r.Name)
				return
			}

		case ClassCritical:
			// Verdetto P0 #4 (Blocco 1.1): shouldExitAfterFailure
			// centralizes the ClassCritical exit rule. For
			// MaxRetries=0 the function returns false (infinite
			// retries — only ctx cancellation exits the loop),
			// so the old `maxR > 0` short-circuit is no longer
			// needed: for ClassCritical, shouldExit is only
			// true when MaxRetries>0 AND attempt exceeds it.
			if shouldExitAfterFailure(r.Class, r.Policy.MaxRetries, attempt) {
				log.Printf("[SUPERVISOR] runner %s CRITICAL budget EXHAUSTED after %d attempts; cancelling supervisor: class=%s last_err=%v",
					r.Name, r.Policy.MaxRetries, r.Class.String(), err)
				fatalMu.Lock()
				*fatalErr = fmt.Errorf("supervisor: critical runner %q exhausted %d retries: %w", r.Name, r.Policy.MaxRetries, err)
				fatalMu.Unlock()
				supCancel()
				return
			}
			s.mu.Lock()
			s.states[r.Name] = RunnerBackingOff
			s.mu.Unlock()
			delay := r.Policy.backoffFor(attempt)
			log.Printf("[SUPERVISOR] runner %s FAILED (critical); sleeping %s before retry %d/%s: err=%v",
				r.Name, delay, attempt+1, retryCeilingString(maxR), err)
			if !sleepCtx(ctx, delay) {
				log.Printf("[SUPERVISOR] runner %s critical: ctx cancelled during backoff", r.Name)
				return
			}

		default:
			log.Printf("[SUPERVISOR] runner %s unknown class=%d; treating as one-shot: err=%v",
				r.Name, int(r.Class), err)
			return
		}
	}
}

func retryCeilingString(n int) string {
	if n < 0 {
		return "inf"
	}
	return fmt.Sprintf("%d", n)
}

// safeCall invokes fn under an optional panic recovery. When
// restartOnPanic is true, a recovered panic is converted to an error
// so the restart loop can treat it identically to a normal failure.
// When false, the panic propagates upward (intended for tests).
func safeCall(ctx context.Context, fn func(context.Context) error, restartOnPanic bool, name string) (err error) {
	if !restartOnPanic {
		return fn(ctx)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("supervisor runner %q panicked: %v", name, r)
		}
	}()
	return fn(ctx)
}

// sleepCtx blocks for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ── Diagnostics ────────────────────────────────────────────────────────────

// Len returns the number of registered runners (diagnostic).
func (s *BackgroundSupervisor) Len() int {
	return len(s.runners)
}

// Names returns the name of every registered runner (for /ready checks).
func (s *BackgroundSupervisor) Names() []string {
	names := make([]string, len(s.runners))
	for i, r := range s.runners {
		names[i] = r.Name
	}
	return names
}

// Classes returns the class of every registered runner. Used by
// /ready checks to surface the supervisor's policy mix.
func (s *BackgroundSupervisor) Classes() []RunnerClass {
	out := make([]RunnerClass, len(s.runners))
	for i, r := range s.runners {
		out[i] = r.Class
	}
	return out
}

// Missing returns the names of every registered runner whose current state
// is not healthy. ClassOneShot runners in STOPPED state are NOT flagged
// (they are expected to exit cleanly). ClassRestartable and ClassCritical
// runners are flagged when their state is BACKING_OFF, FAILED, or STOPPED.
//
// This is the gate the /ready check uses to fail-loud on runner
// silent-death (e.g. a critical runner exhausted retries and the master
// is now serving with a dead delivery pipeline).
func (s *BackgroundSupervisor) Missing() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []string
	for _, r := range s.runners {
		state, ok := s.states[r.Name]
		if !ok {
			// Runner was registered but never started — structural bug.
			missing = append(missing, r.Name)
			continue
		}
		if r.Class == ClassOneShot {
			// One-shot runners are expected to exit cleanly.
			// The !ok branch above handles the never-started case;
			// a stopped OneShot is not flagged as missing.
			continue
		}
		if !state.IsHealthy() {
			missing = append(missing, r.Name)
		}
	}
	sort.Strings(missing)
	return missing
}

// States returns the current state of every registered runner.
// Used by the /ready probe to surface per-runner health details.
func (s *BackgroundSupervisor) States() map[string]RunnerState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]RunnerState, len(s.runners))
	for _, r := range s.runners {
		if state, ok := s.states[r.Name]; ok {
			out[r.Name] = state
		} else {
			out[r.Name] = ""
		}
	}
	return out
}
