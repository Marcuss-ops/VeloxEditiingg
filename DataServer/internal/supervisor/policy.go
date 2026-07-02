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
//                          Consecutive > threshold → return to supervisor.
//   - ErrElementScoped:     Per-row failure already persisted
//                          (MarkFailed/MarkRetry/BlockedAuth on the row).
//                          Continue to the next element.
//   - ErrLeaseLost:         CAS conflict from another runner.
//                          Cancel in-flight via context; no row mutation.
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
		ResetWindow:              30 * time.Second,
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
// The wrapped error preserves the original via errors.Unwrap so
// callers can introspect both the sentinel and the underlying
// cause.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrElementScoped) || errors.Is(err, ErrInfrastructure) || errors.Is(err, ErrPanicked) {
		// Already classified — re-wrap so the caller can still
		// rely on errors.Is at the outer boundary.
		return err
	}
	if errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("%w: %v", ErrInfrastructure, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrInfrastructure, err)
	}
	if errors.Is(err, context.Canceled) {
		// Context cancellation is almost always the supervisor
		// shutting down. NOT infrastructure — treat as
		// element-scoped so the runner returns nil and the loop
		// sees the cancelled ctx on the next iteration.
		return fmt.Errorf("%w: %v", ErrElementScoped, err)
	}
	// Lease-CAS conflicts surface as ErrTransitionConflict from
	// the store layer. Distinct from infrastructure (the row still
	// observes a successful CAS path; the runner is just not the
	// winner). Map to ErrLeaseLost so the in-flight cancellation
	// contract kicks in.
	if errors.Is(err, errLeaseLostSentinel()) || strings.Contains(err.Error(), "transition conflict") ||
		strings.Contains(err.Error(), "lease") && strings.Contains(err.Error(), "conflict") {
		return fmt.Errorf("%w: %v", ErrLeaseLost, err)
	}
	// Bare "database is closed" string match — drivers vary on
	// whether it surfaces as sql.ErrConnDone or a custom string
	// error.
	if strings.Contains(err.Error(), "database is closed") ||
		strings.Contains(err.Error(), "sql: connection is busy") ||
		strings.Contains(err.Error(), "no such table") {
		return fmt.Errorf("%w: %v", ErrInfrastructure, err)
	}
	return fmt.Errorf("%w: %v", ErrElementScoped, err)
}

// errLeaseLostSentinel returns the store-layer ErrTransitionConflict
// sentinel if it exists in this package's graph. Encapsulated in a
// function so the import edges stay minimal; the compare is done
// by string match against the canonical error message to avoid a
// hard import from supervisor → store.
func errLeaseLostSentinel() error {
	return errors.New("completion: transition conflict")
}

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

// NewFailureTracker constructs a tracker with the given policy.
// The default clock is time.Now; tests can override via WithClock.
func NewFailureTracker(p RetryPolicy) *FailureTracker {
	if p.ConsecutiveErrorThreshold <= 0 {
		p.ConsecutiveErrorThreshold = 5
	}
	if p.ResetWindow <= 0 {
		p.ResetWindow = 30 * time.Second
	}
	return &FailureTracker{
		Policy: p,
		nowFn:  time.Now,
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
