package supervisor

// supervisor_types.go: runner-class vocabulary, restart policy, runner
// state, and the Runner / Supervisor core types + registration.
// Split out of supervisor.go; the orchestration loop lives in
// supervisor_run.go and the diagnostics surface in
// supervisor_diagnostics.go.

import (
	"context"
	"fmt"
	"log"
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
