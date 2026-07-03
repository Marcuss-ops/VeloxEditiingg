// Package supervisor: retry policy + error classification for the
// per-run background loops (delivery, outbox, forwarding, metrics).
//
// Verdetto P1 #10 (Blocco 4) — kill the log-and-continue anti-pattern.
// Every background runner used to swallow tick errors via:
//
//	if err := r.tick(ctx); err != nil {
//	    log.Printf("[RUNNER] tick error: %v", err)
//	}
//
// After enough consecutive infra failures the master was silently
// broken (DB down, network down, scheduler dead). This package
// gives the runners a single explicit policy: classify the error,
// persist per-element failures on the row, count infrastructure
// failures, and escalate to the BackgroundSupervisor when a
// threshold is crossed.
//
// Classification:
//
//   - ErrInfrastructure:    DB closed, ctx deadline from infra, sql.ErrConnDone.
//     Consecutive > threshold → return to supervisor.
//   - ErrElementScoped:     Per-row failure already persisted
//     (MarkFailed/MarkRetry/BlockedAuth on the row).
//     Continue to the next element.
//   - ErrLeaseLost:         CAS conflict from another runner.
//     Cancel in-flight via context; no row mutation.
//
// The runners call supervisor.ClassifyError on every tick error,
// record the verdict via supervisor.FailureTracker.Record, and
// propagate ErrInfrastructure only when the consecutive threshold
// is exceeded.
package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Sentinel errors. Use errors.Is(err, ErrXxx) to inspect; never
// compare .Error() strings. Callers MUST treat these as the public
// contract — package versions may evolve, signatures must not.
var (
	// ErrInfrastructure indicates a non-per-element failure: DB
	// closed, sql.ErrConnDone, deadline-from-infra. After N
	// consecutive occurrences (configurable via RetryPolicy) the
	// runner returns this to the BackgroundSupervisor so the
	// ClassRestartable / ClassCritical restart machinery kicks in.
	ErrInfrastructure = errors.New("supervisor: infrastructure error")

	// ErrElementScoped indicates a failure attached to a single
	// element (lease, delivery, outbox event, forwarding). The
	// runner persists the failure on the row (MarkFailed / Retry /
	// BlockedAuth) and proceeds to the next element. Element-scoped
	// errors do NOT count toward the consecutive-error threshold.
	ErrElementScoped = errors.New("supervisor: element-scoped error")

	// ErrLeaseLost indicates the lease was preempted by another
	// runner (ErrTransitionConflict on CAS). The in-flight work
	// must be cancelled via context; the row stays untouched so
	// the new lease holder owns it.
	ErrLeaseLost = errors.New("supervisor: lease lost")

	// ErrPanicked is set when a runner goroutine recovers from a
	// panic and converts it to an error. Counts as infrastructure
	// for the threshold tracker (a panicking handler is not a
	// per-element failure).
	ErrPanicked = errors.New("supervisor: runner panicked")

	// ErrUnexpectedExit is set by runLoop when a runner's Run
	// returns nil while the supervisor's context is still live AND
	// the runner is NOT ClassOneShot. For ClassOneShot runners a
	// nil return is the contract (fire-and-forget). For
	// ClassRestartable and ClassCritical runners a nil return
	// with a live context is a false-success path: a permanent
	// runner (e.g. an outbox dispatcher, a delivery runner, a
	// forwarding runner) exiting without an error means the
	// master is silently broken. Verdetto P0 #3 mandates the
	// supervisor treat this as a failure (and the existing
	// restart-loop machinery escalates it through the
	// ClassRestartable / ClassCritical contract).
	ErrUnexpectedExit = errors.New("supervisor: runner exited unexpectedly (nil err with live ctx on non-oneshot)")
)

// RetryPolicy governs when consecutive infrastructure errors
// escalate the runner to its BackgroundSupervisor. Element-scoped
// and lease-lost errors never escalate on their own — they are
// resolved either by row mutation (element-scoped) or by
// cancellation (lease-lost) inside the runner's loop body.
type RetryPolicy struct {
	// ConsecutiveErrorThreshold is the number of consecutive
	// infrastructure errors before the runner returns
	// ErrInfrastructure to its supervisor. Default 5.
	ConsecutiveErrorThreshold int

	// ResetWindow is the wall-clock duration after which a
	// non-error tick resets the consecutive-error counter even if
	// no fresh tick has fired yet. The intent: an old infrastructure
	// error from 15 minutes ago should not count against today's
	// burst. Zero means no reset window (counter resets only on a
	// successful tick).
	ResetWindow time.Duration
}

// DefaultRetryPolicy returns the canonical values matching the
// audit-recommended behaviour: 5 consecutive infrastructure
// errors → escalate, with a 30s reset window so a single transient
// blip doesn't poison the counter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		ConsecutiveErrorThreshold: 5,
		ResetWindow:               30 * time.Second,
	}
}

// ClassifyError wraps a raw error in one of the three sentinel
// types above (or returns nil if err is nil). Rules:
//
//   - err is nil → nil.
//   - errors.Is(err, sql.ErrConnDone) OR err message contains
//     "database is closed" OR "context deadline exceeded" without
//     a row-mutation context → ErrInfrastructure.
//   - errors.Is(err, ErrTransitionConflict) (the lease-CAS sentinel
//     from store types) → ErrLeaseLost.
//   - errors.Is(err, ErrPanicked) → ErrInfrastructure (panics are
//     not per-element by definition).
//   - everything else → ErrElementScoped.
//
// Wrapping uses errors.Join(sentinel, err) so the resulting error
// chain supports errors.Is for BOTH the sentinel AND the original
// cause. This preserves callers' ability to introspect both —
// e.g. errors.Is(classified, sql.ErrConnDone) still hits the
// underlying DB error even after ClassifyError wrapped it as
// ErrInfrastructure.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrElementScoped) || errors.Is(err, ErrInfrastructure) || errors.Is(err, ErrPanicked) {
		// Already classified — return as-is so the caller can
		// still rely on errors.Is at the outer boundary.
		return err
	}
	// shorthand: pick a sentinel by classification rule then
	// errors.Join it with the original error.
	sentinel := ErrElementScoped
	switch {
	case errors.Is(err, sql.ErrConnDone):
		sentinel = ErrInfrastructure
	case errors.Is(err, context.DeadlineExceeded):
		sentinel = ErrInfrastructure
	case errors.Is(err, context.Canceled):
		// Context cancellation in a tick function is usually the
		// supervisor shutting down — the run loop sees the
		// cancelled ctx on the next iteration and exits cleanly.
		// Map to element-scoped so the tracker does not count it.
		sentinel = ErrElementScoped
	case errors.Is(err, errLeaseLostSentinelValue),
		strings.Contains(err.Error(), "transition conflict"),
		strings.Contains(err.Error(), "lease") && strings.Contains(err.Error(), "conflict"):
		sentinel = ErrLeaseLost
	case strings.Contains(err.Error(), "database is closed"),
		strings.Contains(err.Error(), "sql: connection is busy"),
		strings.Contains(err.Error(), "no such table"):
		sentinel = ErrInfrastructure
	}
	return errors.Join(sentinel, err)
}

// errLeaseLostSentinel is the canonical lease-lost sentinel value used
// by the supervisor package's ClassifyError. Verdetto P0 #6 (Blocco 2):
// it MUST be a package-level var (not constructed on every call via
// errors.New) so `errors.Is(err, errLeaseLostSentinelValue)` in
// ClassifyError actually matches — previously the helper returned a
// fresh `errors.New` on every call which meant errors.Is NEVER matched
// and the only effective classification was the strings.Contains
// fallback. The string value mirrors the canonical completion-layer
// message; the strings.Contains fallback is kept as a belt+braces for
// the case where a third party wraps the error with a different format.
var errLeaseLostSentinelValue = errors.New("completion: transition conflict")

// IsInfrastructure reports whether err has been classified as
// ErrInfrastructure (or chains to it via errors.Is).
func IsInfrastructure(err error) bool {
	return errors.Is(err, ErrInfrastructure)
}

// IsElementScoped reports whether err has been classified as
// ErrElementScoped.
func IsElementScoped(err error) bool {
	return errors.Is(err, ErrElementScoped)
}

// IsLeaseLost reports whether err has been classified as
// ErrLeaseLost.
func IsLeaseLost(err error) bool {
	return errors.Is(err, ErrLeaseLost)
}

// FailureTracker counts consecutive infrastructure errors and
// decides when the runner should escalate. Per-element errors
// and lease-lost errors do not increment the counter.
//
// The tracker is concurrency-safe; the runners' Run loop and their
// inner goroutines (lease renewal, etc.) can both call Record
// without an external lock.
type FailureTracker struct {
	Policy RetryPolicy

	mu          sync.Mutex
	consecutive int
	firstErrAt  time.Time
	lastErrAt   time.Time
	nowFn       func() time.Time // injectable for tests
}

// NewFailureTracker constructs a tracker with the given policy and a
// RealClock default (time.Now). Convenience wrapper over
// NewFailureTrackerWithClock — kept for back-compat with callers that
// do not care about the clock seam. Tests that need a mock clock
// MUST use NewFailureTrackerWithClock directly.
//
// Verdetto P2 (Blocco 5): Clock is now an explicit constructor seam
// so the FailureTracker can be advanced deterministically in unit
// tests, integration tests, and any future code that wants to drive
// the streak-refresh wall-clock without sleeping through real time.
func NewFailureTracker(p RetryPolicy) *FailureTracker {
	return NewFailureTrackerWithClock(p, RealClock{})
}

// NewFailureTrackerWithClock is the canonical FailureTracker
// constructor. Callers that need to inject a mock clock (e.g. via a
// MockClock in policy_test.go that maintains a virtual Now) pass it
// here.
//
// Nil Clock contract: passing nil is explicitly supported and falls
// back to RealClock, matching the existing NewFailureTracker
// contract and the documentation above the Clock interface in
// clock.go. This avoids crashing long-running runners if clock
// injection is missed and gives every caller a working
// time-dependent state machine by default.
//
// Production callers SHOULD pass RealClock{} explicitly for
// code-review clarity even though nil silently degrades; both
// forms work. Defensive `if clk == nil` checks at call sites are
// unnecessary because the fallback is documented behavior, not
// an undocumented bug surface, but explicit injection at every
// production call site reads better in PR review.
func NewFailureTrackerWithClock(p RetryPolicy, clk Clock) *FailureTracker {
	if p.ConsecutiveErrorThreshold <= 0 {
		p.ConsecutiveErrorThreshold = 5
	}
	if p.ResetWindow <= 0 {
		p.ResetWindow = 30 * time.Second
	}
	if clk == nil {
		clk = RealClock{}
	}
	return &FailureTracker{
		Policy: p,
		nowFn:  clk.Now,
	}
}

// WithClock replaces the tracker's wall-clock source. Used by tests
// to drive ResetWindow deterministically.
func (t *FailureTracker) WithClock(nowFn func() time.Time) *FailureTracker {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nowFn = nowFn
	return t
}

// Record registers a classified tick error with the tracker. The
// behaviour:
//
//   - err is nil → Reset + return nil.
//   - IsElementScoped OR IsLeaseLost → no count change, return nil
//     (these are resolved in-line by the row mutation / ctx-cancel
//     contract; they do not count toward the threshold).
//   - IsInfrastructure → ++consecutive; if now-firstErrAt > ResetWindow
//     reset the consecutive to 1 and treat the current error as the
//     start of a fresh streak. If consecutive >= threshold, return
//     an ErrInfrastructure wrapped with the streak summary.
//   - everything else → treat as infrastructure (defensive belt + braces).
//
// The returned error is non-nil only when the runner should bail
// out and propagate ErrInfrastructure to the BackgroundSupervisor.
// Returning nil means: continue with the next tick.
func (t *FailureTracker) Record(err error) error {
	if err == nil {
		t.Reset()
		return nil
	}
	// Per-element / lease-lost: pass through (no escalation).
	if IsElementScoped(err) || IsLeaseLost(err) {
		return nil
	}
	// Everything else (Infrastructure, Panicked, unknown): count.
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.nowFn()
	if t.consecutive == 0 || (t.Policy.ResetWindow > 0 && now.Sub(t.firstErrAt) > t.Policy.ResetWindow) {
		t.consecutive = 1
		t.firstErrAt = now
		t.lastErrAt = now
	} else {
		t.consecutive++
		t.lastErrAt = now
	}
	if t.consecutive >= t.Policy.ConsecutiveErrorThreshold {
		return fmt.Errorf("%w: consecutive=%d (since=%s last=%s) original=%v",
			ErrInfrastructure, t.consecutive,
			t.firstErrAt.Format(time.RFC3339Nano),
			t.lastErrAt.Format(time.RFC3339Nano),
			err)
	}
	return nil
}

// Reset clears the consecutive-error counter. Called automatically
// on a successful tick (Record(nil)) and exposed so callers can
// reset manually — e.g. when the runner recovers from a transient
// infra failure out-of-band.
func (t *FailureTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consecutive = 0
	t.firstErrAt = time.Time{}
	t.lastErrAt = time.Time{}
}

// Consecutive returns the current consecutive-error counter value.
// Useful for tests and observability.
func (t *FailureTracker) Consecutive() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.consecutive
}
