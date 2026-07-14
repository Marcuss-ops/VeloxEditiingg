// Package supervisor / supervisor.go
//
// Background runner supervisor: owns a set of Runner entries and drives
// their lifecycle with class-specific restart semantics. The supervisor
// is the canonical entry point for long-lived background loops (delivery,
// outbox, forwarding, metrics, …) so a single component owns the
// goroutine topology, the supervised-state map, the /ready diagnostics,
// and the failure-escalation contract.
//
// Three RunnerClass values drive the restart semantics:
//
//   - ClassOneShot     — runs once, exits, never restarted. Setup tasks.
//   - ClassRestartable — runs forever; bounded retries + exponential
//     backoff. After exhaustion the runner is removed
//     and the supervisor emits a WARN.
//   - ClassCritical    — runs forever; infinite retries (or bounded when
//     Policy.MaxRetries > 0). On exhaustion cancels
//     the supervisor-internal ctx and returns a fatal
//     error so Kubernetes restarts the pod.
package supervisor

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// ── RunnerClass ──────────────────────────────────────────────────────────

// RunnerClass drives restart policy semantics. See the package comment
// for the per-class behavior matrix.
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

// ── RestartPolicy ────────────────────────────────────────────────────────

// RestartPolicy drives the restart loop's backoff schedule. MaxRetries
// is interpreted in the context of Class:
//
//   - ClassOneShot:     ignored (always zero restarts).
//   - ClassRestartable: bounded; after this many restarts the runner
//     is removed and the supervisor logs WARN.
//   - ClassCritical:    if zero, restart infinitely; if positive, restart
//     at most this many times before the supervisor
//     cancels its internal ctx and returns error.
//
// InitialBackoff doubles after each attempt until MaxBackoff. Zero
// values mean "no sleep between restarts" (typical default is 500ms → 30s).
type RestartPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	RestartOnPanic bool
}

func (p RestartPolicy) backoffFor(attempt int) time.Duration {
	if attempt <= 0 || p.InitialBackoff <= 0 {
		return 0
	}
	const capAttempt = 30
	n := attempt
	if n > capAttempt {
		n = capAttempt
	}
	d := p.InitialBackoff << (n - 1)
	if d <= 0 {
		return 24 * time.Hour
	}
	if p.MaxBackoff > 0 && d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// ── RunnerState ──────────────────────────────────────────────────────────

// RunnerState tracks the lifecycle phase of a supervised runner.
// The /ready probe uses this to fail when a runner is BACKING_OFF,
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

// ── Runner (the canonical supervised unit) ──────────────────────────────

// Runner is the canonical unit the supervisor manages. Run must respect
// ctx cancellation; returning non-nil triggers the restart policy;
// returning nil is treated as a clean exit (ClassOneShot only — see
// runLoop for the contract on ClassRestartable + ClassCritical).
type Runner struct {
	Name   string
	Class  RunnerClass
	Policy RestartPolicy
	// Run is the loop body. Must respect ctx cancellation. Returning
	// non-nil triggers the restart policy; returning nil is treated as
	// a clean exit.
	// ClassRestartable / ClassCritical: a nil return with a LIVE ctx
	// is remapped to ErrUnexpectedExit (defined in policy.go) so the
	// restart loop catches the false-success path before the runner is
	// marked STOPPED.
	Run func(ctx context.Context) error
}

// ── Supervisor (the orchestrator) ───────────────────────────────────────

// Supervisor owns a set of Runner entries and orchestrates their
// lifecycle:
//
//   - Start every runner in its own goroutine.
//   - On non-nil Run return, apply the Class-specific restart loop with
//     exponential backoff.
//   - Cancel the supervisor-internal ctx when:
//     (a) the parent ctx is cancelled (graceful shutdown), OR
//     (b) a ClassCritical runner exhausts its retry budget.
//   - Return from Run when ALL runners have stopped.
type Supervisor struct {
	runners []Runner

	// states tracks the lifecycle state of each registered runner.
	// A runner transitions STARTING → RUNNING → (BACKING_OFF → RUNNING)* → STOPPED/FAILED.
	mu     sync.RWMutex
	states map[string]RunnerState
}

// New creates an empty supervisor.
func New() *Supervisor {
	return &Supervisor{
		states: make(map[string]RunnerState),
	}
}

// Register adds a Runner to the supervisor. Duplicate names are
// rejected at composition time — a misconfigured supervisor is a
// startup bug, not a runtime recovery scenario.
func (s *Supervisor) Register(r Runner) error {
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

// ── Effective retries + exit rule ────────────────────────────────────────

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
// exit-after-failure rule. Given a class c with maxRetries and the
// current 1-based attempt count, it reports whether runLoop should
// stop retrying after the current failed attempt.
//
// Rule matrix:
//
//	ClassOneShot,        maxRetries=*    → true  (fire-and-forget)
//	ClassRestartable,    maxRetries<=0   → true  (zero budget = exit on first error)
//	ClassRestartable,    maxRetries>0    → attempt > maxRetries
//	ClassCritical,       maxRetries<=0   → false (infinite; ctx cancel exits only)
//	ClassCritical,       maxRetries>0    → attempt > maxRetries
//	unknown class         → true        (defensive: don't loop forever)
//
// Centralizing the rule here avoids the short-circuit bug from the
// previous `if maxR > 0 && attempt > maxR` guard (which short-circuited
// on maxR==0 and caused ClassRestartable with MaxRetries=0 to loop
// forever instead of exiting on the first error).
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
			return false
		}
		return attempt > maxRetries
	default:
		return true
	}
}

// ── Run / runLoop ────────────────────────────────────────────────────────

// Run starts every registered runner in its own goroutine and blocks
// until ALL runners have exited.
//
// Returns nil when the parent ctx was cancelled (graceful shutdown).
// Returns a non-nil error when a ClassCritical runner exhausts its
// retry budget; in that case the supervisor has already cancelled its
// internal ctx so every OTHER runner is torn down at the same time.
func (s *Supervisor) Run(ctx context.Context) error {
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

// runLoop is the per-runner goroutine body. Honours the runner's Class
// and Policy: OneShot runs once; Restartable retries bounded; Critical
// retries forever (or up to the positive MaxRetries budget) and on
// exhaustion cancels the supervisor-internal ctx so every OTHER runner
// is torn down at the same time.
func (s *Supervisor) runLoop(
	ctx context.Context,
	fatalMu *sync.Mutex,
	fatalErr *error,
	r Runner,
	supCancel context.CancelFunc,
) {
	attempt := 0
	// maxR is the EFFECTIVE retry budget (raw MaxRetries mapped through
	// effectiveMaxRetries). It's used in the log lines that communicate
	// the retry ceiling to operators — the "retry N/ceiling" format
	// needs the effective budget so ClassCritical/MaxRetries=0 reads
	// as "retry 1/inf" (infinite) rather than "retry 1/0" (which would
	// misleadingly suggest the runner has no retries left). The exit
	// decision itself goes through shouldExitAfterFailure which takes
	// the raw Policy.MaxRetries.
	maxR := effectiveMaxRetries(r.Class, r.Policy.MaxRetries)
	for {
		s.mu.Lock()
		s.states[r.Name] = RunnerRunning
		s.mu.Unlock()

		log.Printf("[SUPERVISOR] starting runner: name=%s class=%s attempt=%d/%s",
			r.Name, r.Class.String(), attempt+1, retryCeilingString(maxR))
		err := safeCall(ctx, r.Run, r.Policy.RestartOnPanic, r.Name)

		if ctx.Err() != nil {
			log.Printf("[SUPERVISOR] runner %s exiting: %v", r.Name, ctx.Err())
			return
		}

		// nil err with a LIVE ctx is a false-success path for permanent
		// runners (ClassRestartable / ClassCritical). Remap to
		// ErrUnexpectedExit so the restart machinery kicks in rather
		// than marking the runner STOPPED and silently leaving the
		// master with a dead delivery / forwarding / outbox pipeline.
		if err == nil && r.Class != ClassOneShot {
			err = ErrUnexpectedExit
			log.Printf("[SUPERVISOR] runner %s returned nil err with live ctx (class=%s); treating as ErrUnexpectedExit",
				r.Name, r.Class.String())
		}

		if err == nil {
			s.mu.Lock()
			s.states[r.Name] = RunnerStopped
			s.mu.Unlock()
			log.Printf("[SUPERVISOR] runner %s exited cleanly", r.Name)
			return
		}

		attempt++

		s.mu.Lock()
		s.states[r.Name] = RunnerFailed
		s.mu.Unlock()

		switch r.Class {
		case ClassOneShot:
			log.Printf("[SUPERVISOR] runner %s one-shot failed (NOT restarted): class=%s err=%v",
				r.Name, r.Class.String(), err)
			return

		case ClassRestartable:
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

// ── Diagnostics ──────────────────────────────────────────────────────────

// Len returns the number of registered runners.
func (s *Supervisor) Len() int {
	return len(s.runners)
}

// Names returns the name of every registered runner.
func (s *Supervisor) Names() []string {
	names := make([]string, len(s.runners))
	for i, r := range s.runners {
		names[i] = r.Name
	}
	return names
}

// Classes returns the class of every registered runner.
func (s *Supervisor) Classes() []RunnerClass {
	out := make([]RunnerClass, len(s.runners))
	for i, r := range s.runners {
		out[i] = r.Class
	}
	return out
}

// Missing returns the names of every registered runner whose current
// state is not healthy. ClassOneShot runners in STOPPED state are NOT
// flagged (they are expected to exit cleanly). ClassRestartable and
// ClassCritical runners are flagged when their state is BACKING_OFF,
// FAILED, or STOPPED.
//
// This is the gate the /ready check uses to fail-loud on runner
// silent-death (e.g. a critical runner exhausted retries and the master
// is now serving with a dead delivery pipeline).
func (s *Supervisor) Missing() []string {
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
func (s *Supervisor) States() map[string]RunnerState {
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
