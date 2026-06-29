package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
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
	d := p.InitialBackoff << n
	if d <= 0 || d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
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

	// alive tracks which runners are currently executing. A runner
	// is added when its goroutine starts and removed when it
	// returns (cleanly OR with error OR after a critical exhausting
	// retries). /ready uses Missing() to surface silent-deaths of
	// ClassRestartable + ClassCritical runners.
	mu    sync.RWMutex
	alive map[string]bool
}

// NewBackgroundSupervisor creates an empty supervisor.
func NewBackgroundSupervisor() *BackgroundSupervisor {
	return &BackgroundSupervisor{
		alive: make(map[string]bool),
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
		// Pre-mark the runner as alive so a /ready check before
		// runLoop starts doesn't gate-fail spuriously.
		s.mu.Lock()
		s.alive[r.Name] = true
		s.mu.Unlock()
		go func() {
			defer wg.Done()
			defer func() {
				s.mu.Lock()
				s.alive[r.Name] = false
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
	maxR := effectiveMaxRetries(r.Class, r.Policy.MaxRetries)
	attempt := 0
	for {
		log.Printf("[SUPERVISOR] starting runner: name=%s class=%s attempt=%d/%d",
			r.Name, r.Class.String(), attempt+1, maxR)
		err := safeCall(ctx, r.Run, r.Policy.RestartOnPanic, r.Name)

		// Graceful shutdown path: parent ctx cancelled mid-run.
		if ctx.Err() != nil {
			log.Printf("[SUPERVISOR] runner %s exiting: %v", r.Name, ctx.Err())
			return
		}

		// Clean exit — Run returned nil regardless of Class.
		if err == nil {
			log.Printf("[SUPERVISOR] runner %s exited cleanly", r.Name)
			return
		}

		attempt++

		switch r.Class {
		case ClassOneShot:
			log.Printf("[SUPERVISOR] runner %s one-shot failed (NOT restarted): class=%s err=%v",
				r.Name, r.Class.String(), err)
			return

		case ClassRestartable:
			if maxR > 0 && attempt > maxR {
				log.Printf("[SUPERVISOR] runner %s restartable budget EXHAUSTED after %d attempts; removing from supervisor: class=%s last_err=%v",
					r.Name, maxR, r.Class.String(), err)
				return
			}
			delay := r.Policy.backoffFor(attempt)
			log.Printf("[SUPERVISOR] runner %s FAILED (restartable); sleeping %s before retry %d/%d: err=%v",
				r.Name, delay, attempt+1, maxR, err)
			if !sleepCtx(ctx, delay) {
				log.Printf("[SUPERVISOR] runner %s restartable: ctx cancelled during backoff", r.Name)
				return
			}

		case ClassCritical:
			if maxR > 0 && attempt > maxR {
				log.Printf("[SUPERVISOR] runner %s CRITICAL budget EXHAUSTED after %d attempts; cancelling supervisor: class=%s last_err=%v",
					r.Name, maxR, r.Class.String(), err)
				fatalMu.Lock()
				*fatalErr = fmt.Errorf("supervisor: critical runner %q exhausted %d retries: %w", r.Name, maxR, err)
				fatalMu.Unlock()
				supCancel()
				return
			}
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

// Missing returns the names of every registered runner that has STOPPED
// but is still EXPECTED to be running. ClassOneShot runners are
// returned only while they are still alive (they are EXPECTED to exit
// cleanly post-startup, so a dead one is NOT flagged as missing here).
// ClassRestartable and ClassCritical runners are flagged the moment
// their goroutine returns — even if the supervisor as a whole is still
// running other runners.
//
// This is the gate the /ready check uses to fail-loud on runner
// silent-death (e.g. metrics-supervisor exhausts its 5 retries and the
// master is now serving stale metrics but looks "healthy").
func (s *BackgroundSupervisor) Missing() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var missing []string
	for _, r := range s.runners {
		if r.Class == ClassOneShot {
			// One-shot runners are expected to exit. Only flag them
			// as missing if they haven't even started yet (alive
			// was never set true) — which would indicate a
			// construction-time bug, not a runtime failure.
			if _, started := s.alive[r.Name]; !started {
				missing = append(missing, r.Name)
			}
			continue
		}
		if alive, ok := s.alive[r.Name]; !ok || !alive {
			missing = append(missing, r.Name)
		}
	}
	sort.Strings(missing)
	return missing
}
