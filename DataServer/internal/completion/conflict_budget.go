// Package completion / conflict_budget.go
//
// Bounded retry on CAS conflicts inside the Coordinator.
//
// Verdetto P2 (Blocco 5): the Coordinator wraps CAS rows in
// LevelSerializable SQLite transactions. Three specific attempt_commits
// CAS paths can race with concurrent writers:
//
//   - UpdateReadyCountExhaustive: CompleteUpload bumps ready count.
//   - SetExpired: CompleteUpload deadline-breach EXPIRED transition.
//   - MarkCommitted: CommitAttempt promotes attempt_commits to COMMITTED.
//
// A single short-lived CAS failure surfaces as ErrTransitionConflict
// to the caller; the caller (worker over gRPC, reconcile supervisor,
// recover_output CLI) usually handles it by re-reading canonical
// state. Repeated conflicts on the same path indicate the master-
// side lock graph is wedged (a long-running tx holding the write
// lock, a stuck process, a casualty of the completion supervisor's
// concurrent scans). Counting them without bound lets the
// Coordinator spin on a deadlock — the master looks alive but no
// work makes forward progress.
//
// ConflictBudget is the per-Coordinator counter that
//   - increments on ErrTransitionConflict from the attempt_commits
//     CAS paths above (task_attempts / tasks / jobs CAS conflicts
//     propagate NOW without counting, by design);
//   - resets on a successful Coordinator method exit;
//   - on the 4th consecutive conflict (default), returns
//     ErrConflictBudgetExhausted so the caller can route to the
//     appropriate restart policy — e.g. mapped to
//     supervisor.ErrInfrastructure by the ReconciliationSupervisor.
//
// The threshold and reset window are configurable. The counter is
// concurrency-safe; the Coordinator's methods own their own
// [*sql.Tx] lifecycle, so budget writes occur only from a single
// method-call goroutine in practice, but the mutex guarantees
// correctness if other goroutines (tests, future fan-out paths)
// call Coordinator methods concurrently.
package completion

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrConflictBudgetExhausted signals that the Coordinator's
// ConflictBudget crossed its threshold. Callers MUST treat this as
// "do not retry on the same path; surface to the operator or
// restart the master". Mapped to supervisor.ErrInfrastructure by
// the supervision layer when the budget hits.
//
// Use errors.Is(err, ErrConflictBudgetExhausted) to inspect.
var ErrConflictBudgetExhausted = errors.New("completion: conflict budget exhausted")

// ConflictBudgetPolicy governs ConflictBudget escalation.
type ConflictBudgetPolicy struct {
	// ConsecutiveConflictThreshold is the number of consecutive
	// ErrTransitionConflict from the canonical attempt_commits
	// CAS paths before the budget returns ErrConflictBudgetExhausted.
	// With threshold=3 (default) the 4th consecutive conflict is the
	// escalation boundary.
	ConsecutiveConflictThreshold int

	// ResetWindow is the wall-clock duration after which a stale
	// conflict is forgotten. Zero means the counter resets only on
	// a successful Coordinator method exit (no time-based window).
	ResetWindow time.Duration
}

// DefaultConflictBudgetPolicy returns the canonical thresholds
// matching Blocco 5's user spec: 3 consecutive conflicts allowed
// (the 4th escalates), with a 5-minute reset window so a one-off
// stale conflict at startup doesn't poison the counter long-term.
func DefaultConflictBudgetPolicy() ConflictBudgetPolicy {
	return ConflictBudgetPolicy{
		ConsecutiveConflictThreshold: 3,
		ResetWindow:                 5 * time.Minute,
	}
}

// ConflictBudget counts consecutive ErrTransitionConflict on the
// attempt_commits CAS paths and returns a wrapped
// ErrConflictBudgetExhausted when the threshold is crossed.
type ConflictBudget struct {
	Policy ConflictBudgetPolicy

	mu          sync.Mutex
	consecutive int
	firstErrAt  time.Time
	lastErrAt   time.Time
	nowFn       func() time.Time

	// sink is the optional Prometheus instrumentation point. When
	// non-nil, Record/Reset notify it of state-machine transitions
	// so dashboards can show the streak shape next to the under-
	// threshold counts. Nil-safe; tests construct the budget
	// without a sink (the default). See WithMetricsSink.
	sink ConflictBudgetSink
}

// ConflictBudgetSink is the optional completion-side contract the
// budget emits to after each state-machine transition. Defined
// here (consumed-by-completion) to avoid a metrics import in the
// completion package and to keep the structural-method-shape
// shared with the metrics-side interface of the same name.
//
// Three semantically distinct calls so the test surface can mock
// each transition independently:
//
//   - ResetConflictBudget()                 — real reset (Record(nil) on
//                                              a non-zero prior streak).
//                                              No-op resets (streak=0) do
//                                              NOT increment.
//   - ObserveConflictStreakUnderThreshold(streak int)
//                                            — Record(ErrTransitionConflict)
//                                              that incremented the streak
//                                              but stayed under threshold.
//                                              streak is the POST-increment
//                                              value (>= 1, <= threshold-1).
//   - EscalateConflictBudget(streak int)     — Record(ErrTransitionConflict)
//                                              that crossed the threshold.
//                                              streak is the POST-increment
//                                              value (= threshold).
//
// The metrics.Collector method set matches this interface via Go's
// structural typing; bootstrap can wire it without an explicit cast.
type ConflictBudgetSink interface {
	ResetConflictBudget()
	ObserveConflictStreakUnderThreshold(streak int)
	EscalateConflictBudget(streak int)
}

// WithMetricsSink installs (or replaces) the Prometheus sink used
// to instrument Record/Reset transitions. Nil is allowed and is
// treated as "no instrumentation" (the budget keeps the same state-
// machine behaviour but emits no metrics). Mirrors WithClock so the
// constructor seam stays minimal.
func (b *ConflictBudget) WithMetricsSink(sink ConflictBudgetSink) *ConflictBudget {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sink = sink
	return b
}

// NewConflictBudget constructs a budget with the supplied policy.
// The clock defaults to time.Now; tests can override via WithClock.
func NewConflictBudget(p ConflictBudgetPolicy) *ConflictBudget {
	if p.ConsecutiveConflictThreshold <= 0 {
		p.ConsecutiveConflictThreshold = 3
	}
	if p.ResetWindow <= 0 {
		p.ResetWindow = 5 * time.Minute
	}
	return &ConflictBudget{
		Policy: p,
		nowFn:  time.Now,
	}
}

// WithClock replaces the budget's wall-clock source. Used by tests
// to drive ResetWindow deterministically.
func (b *ConflictBudget) WithClock(nowFn func() time.Time) *ConflictBudget {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nowFn = nowFn
	return b
}

// Record registers a Coordinator-method CAS outcome. err is one of:
//
//   - nil → Reset + return nil. Callers invoke this from the
//     successful exit path of each Coordinator method so a single
//     completed commit clears the streak.
//   - ErrTransitionConflict → +1 consecutive; if crossed threshold
//     (consecutive >= ConsecutiveConflictThreshold), return
//     ErrConflictBudgetExhausted wrapped with the streak summary;
//     otherwise return nil so the caller can decide what to do
//     (typically propagate ErrTransitionConflict unchanged to the
//     outer caller — the worker over gRPC handles its retry).
//   - anything else → no count change, return err unchanged so the
//     caller can decide.
//
// The returned error is non-nil only when the Coordinator's caller
// should escalate. Returning nil means: continue with whatever
// fallback the caller has.
func (b *ConflictBudget) Record(err error) error {
	if err == nil {
		b.Reset()
		return nil
	}
	if !errors.Is(err, ErrTransitionConflict) {
		return err
	}
	// ErrTransitionConflict path: capture the prior lock + sink
	// references under the mutex, drop the lock, and notify the
	// sink AFTER the lock is released so the Prometheus registry's
	// own mutexes do not chain on the budget's CAS hot path.
	b.mu.Lock()
	now := b.nowFn()
	if b.consecutive == 0 || (b.Policy.ResetWindow > 0 && now.Sub(b.firstErrAt) > b.Policy.ResetWindow) {
		b.consecutive = 1
		b.firstErrAt = now
		b.lastErrAt = now
	} else {
		b.consecutive++
		b.lastErrAt = now
	}
	escalated := b.consecutive >= b.Policy.ConsecutiveConflictThreshold
	streakSnapshot := b.consecutive
	sink := b.sink
	firstErrAt := b.firstErrAt
	lastErrAt := b.lastErrAt
	b.mu.Unlock()

	wrapErr := func() error {
		return fmt.Errorf("%w: consecutive=%d (since=%s last=%s) original=%v",
			ErrConflictBudgetExhausted, streakSnapshot,
			firstErrAt.Format(time.RFC3339Nano),
			lastErrAt.Format(time.RFC3339Nano),
			err)
	}
	if sink == nil {
		if escalated {
			return wrapErr()
		}
		return nil
	}
	if escalated {
		sink.EscalateConflictBudget(streakSnapshot)
		return wrapErr()
	}
	sink.ObserveConflictStreakUnderThreshold(streakSnapshot)
	return nil
}

// Reset clears the consecutive-conflict counter. Called automatically
// on a successful Coordinator method exit (Record(nil)) and exposed
// so callers can reset manually — e.g. when the master recovers
// from a transient contention-out-of-band. Notifies the sink only
// on a REAL reset (the prior streak was non-zero) so the reset
// counter does not double-count trivial no-op resets on every
// successful exit.
func (b *ConflictBudget) Reset() {
	b.mu.Lock()
	wasStreak := b.consecutive > 0
	b.consecutive = 0
	b.firstErrAt = time.Time{}
	b.lastErrAt = time.Time{}
	sink := b.sink
	b.mu.Unlock()

	if wasStreak && sink != nil {
		// Notify OUTSIDE the mutex for the same reason as Record:
		// the registry's own mutexes should not chain on the
		// budget's hot path.
		sink.ResetConflictBudget()
	}
}

// Consecutive returns the current consecutive-conflict counter value.
// Useful for tests and observability.
func (b *ConflictBudget) Consecutive() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutive
}
