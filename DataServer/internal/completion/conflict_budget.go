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
//   - on the 3rd consecutive conflict (default), returns
//     ErrConflictBudgetExhausted so the caller can route to the
//     appropriate restart policy — e.g. mapped to
//     supervisor.ErrInfrastructure by the ReconciliationSupervisor.
//     (The 3rd conflict is the boundary because Record uses
//     `>=` against the threshold: threshold=3 means the 3rd
//     conflict escalates. Earlier docs said "4th (3+1)" but that
//     was an off-by-one typo; the test
//     TestConflictBudget_EscalatesAtThresholdBoundary pins the
//     actual invariant: 3rd consecutive = boundary.)
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
	// With threshold=3 (default) the 3rd consecutive conflict is the
	// escalation boundary.
	ConsecutiveConflictThreshold int

	// ResetWindow is the wall-clock duration after which a stale
	// conflict is forgotten. Zero means the counter resets only on
	// a successful Coordinator method exit (no time-based window).
	ResetWindow time.Duration
}

// DefaultConflictBudgetPolicy returns the canonical thresholds
// matching Blocco 5's user spec: 3 consecutive conflicts allowed
// (the 3rd escalates), with a 5-minute reset window so a one-off
// stale conflict at startup doesn't poison the counter long-term.
func DefaultConflictBudgetPolicy() ConflictBudgetPolicy {
	return ConflictBudgetPolicy{
		ConsecutiveConflictThreshold: 3,
		ResetWindow:                  5 * time.Minute,
	}
}

// ConflictBudget counts consecutive ErrTransitionConflict on the
// attempt_commits CAS paths and returns a wrapped
// ErrConflictBudgetExhausted when the threshold is crossed.
//
// Verdetto P0 #4 (Blocco 3): the budget is PER-KEY, where the key
// identifies the independent row operation (typically
// "commit:<commit_id>"). Two different commit_ids hitting CAS
// conflicts concurrently form TWO independent streaks; they do
// NOT share a global counter. This prevents the false-positive
// escalation where one stalled commit's conflicts cause an
// unrelated commit's first few conflicts to be re-counted as part
// of the same streak.
//
// Backward compat: the per-key API is additive. Callers that pass
// a fixed dummy key (e.g. "test") behave identically to the old
// single-counter design. The Coordinator passes the actual
// commit_id as the key at every canonical attempt_commits CAS path.
type ConflictBudget struct {
	Policy ConflictBudgetPolicy

	mu sync.Mutex
	// streaks is keyed by an arbitrary string (typically
	// "commit:<commit_id>"). Each entry tracks its own consecutive
	// counter + first/last error timestamps. Entries are
	// eagerly removed on Record(key, nil) (success) and on
	// escalation; the map size is bounded by the number of
	// in-flight CAS operations at any moment.
	streaks map[string]*streakState
	nowFn   func() time.Time

	// sink is the optional Prometheus instrumentation point. When
	// non-nil, Record/Reset notify it of state-machine transitions
	// so dashboards can show the streak shape next to the under-
	// threshold counts. Nil-safe; tests construct the budget
	// without a sink (the default). See WithMetricsSink.
	sink ConflictBudgetSink
}

// streakState is the per-key state held by ConflictBudget. Each
// key (typically "commit:<commit_id>") has its own streakState so
// independent operations don't share a counter.
type streakState struct {
	consecutive int
	firstErrAt  time.Time
	lastErrAt   time.Time
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
//     a non-zero prior streak).
//     No-op resets (streak=0) do
//     NOT increment.
//   - ObserveConflictStreakUnderThreshold(streak int)
//     — Record(ErrTransitionConflict)
//     that incremented the streak
//     but stayed under threshold.
//     streak is the POST-increment
//     value (>= 1, <= threshold-1).
//   - EscalateConflictBudget(streak int)     — Record(ErrTransitionConflict)
//     that crossed the threshold.
//     streak is the POST-increment
//     value (= threshold).
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
		Policy:  p,
		nowFn:   time.Now,
		streaks: make(map[string]*streakState),
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

// Record registers a Coordinator-method CAS outcome for a specific
// key. err is one of:
//
//   - nil → reset the streak for this key + return nil. Callers
//     invoke this from the successful exit path of each Coordinator
//     method so a single completed commit clears its own streak.
//   - ErrTransitionConflict → +1 consecutive for this key; if
//     crossed threshold (consecutive >= ConsecutiveConflictThreshold),
//     return ErrConflictBudgetExhausted wrapped with the streak
//     summary AND eagerly remove the key from the map; otherwise
//     return nil so the caller can decide what to do (typically
//     propagate ErrTransitionConflict unchanged to the outer
//     caller — the worker over gRPC handles its retry).
//   - anything else → no count change, return err unchanged so the
//     caller can decide.
//
// The returned error is non-nil only when the Coordinator's caller
// should escalate. Returning nil means: continue with whatever
// fallback the caller has.
//
// Per-key isolation: two different keys form two independent
// streaks. Verdetto P0 #4 (Blocco 3) mandates this so concurrent
// independent commit_ids don't aggregate into one false-positive
// escalation.
//
// Key granularity rationale: the key is typically
// "commit:<commit_id>". All four canonical attempt_commits CAS
// paths on the same commit_id (UpdateReadyCountExhaustive,
// SetExpired, MarkCommitted, SetExpiredByID) therefore share one
// streak. This is the CORRECT design — a commit_id that hits CAS
// conflicts across multiple operations IS a wedged-lock-graph
// symptom, and the budget SHOULD escalate on it. Keying per-
// operation (e.g. "MarkCommitted:<commit_id>") would let the
// budget never escalate because each operation gets a fresh
// streak. Do NOT "fix" this by adding the operation prefix.
//
// Escalation semantics: when a key crosses the threshold, the key
// is eagerly removed from the map and the next Record(key, err)
// starts a fresh streak at 1. This is NOT a permanent circuit-
// breaker — it is a SIGNAL to the caller (CompleteUpload /
// CommitAttempt / ReconcileAttempt) to stop retrying on this path
// and route ErrConflictBudgetExhausted to the supervisor's
// restart policy. The caller is expected to NOT retry on the same
// path; a "resurrected" streak from a different caller is fine
// (the budget is reusable across Coordinator method exits).
func (b *ConflictBudget) Record(key string, err error) error {
	if err == nil {
		b.resetKey(key)
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
	state := b.streaks[key]
	if state == nil || (b.Policy.ResetWindow > 0 && now.Sub(state.firstErrAt) > b.Policy.ResetWindow) {
		state = &streakState{
			consecutive: 1,
			firstErrAt:  now,
			lastErrAt:   now,
		}
		b.streaks[key] = state
	} else {
		state.consecutive++
		state.lastErrAt = now
	}
	escalated := state.consecutive >= b.Policy.ConsecutiveConflictThreshold
	streakSnapshot := state.consecutive
	firstErrAt := state.firstErrAt
	lastErrAt := state.lastErrAt
	sink := b.sink
	// Verdetto P0 #4 (Blocco 3): on escalation, eagerly remove
	// the key from the map so the next Record(key, ...) on the
	// same key starts a fresh streak. Without this, a stuck key
	// would permanently occupy a map slot AND permanently stay
	// at threshold (the next Record would see consecutive==3 and
	// escalate again immediately, which is correct but blocks
	// any "recover from the failure" path).
	if escalated {
		delete(b.streaks, key)
	}
	b.mu.Unlock()

	wrapErr := func() error {
		return fmt.Errorf("%w: consecutive=%d (since=%s last=%s) key=%s original=%v",
			ErrConflictBudgetExhausted, streakSnapshot,
			firstErrAt.Format(time.RFC3339Nano),
			lastErrAt.Format(time.RFC3339Nano),
			key,
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

// resetKey clears the streak for a specific key. Called automatically
// on Record(key, nil) and exposed via Reset (which iterates all
// keys). Notifies the sink only on a REAL reset (the prior streak
// was non-zero) so the reset counter does not double-count trivial
// no-op resets on every successful exit.
func (b *ConflictBudget) resetKey(key string) {
	b.mu.Lock()
	state, ok := b.streaks[key]
	if !ok {
		b.mu.Unlock()
		return
	}
	wasStreak := state.consecutive > 0
	delete(b.streaks, key)
	sink := b.sink
	b.mu.Unlock()

	if wasStreak && sink != nil {
		// Notify OUTSIDE the mutex for the same reason as Record:
		// the registry's own mutexes should not chain on the
		// budget's hot path.
		sink.ResetConflictBudget()
	}
}

// Reset clears every key's streak. Called manually — e.g. when
// the master recovers from a transient contention-out-of-band.
// Notifies the sink at most once per call, only if at least one
// key had a non-zero streak. For per-key resets, callers should
// use Record(key, nil) instead.
func (b *ConflictBudget) Reset() {
	b.mu.Lock()
	wasStreak := false
	for _, s := range b.streaks {
		if s.consecutive > 0 {
			wasStreak = true
			break
		}
	}
	b.streaks = make(map[string]*streakState)
	sink := b.sink
	b.mu.Unlock()

	if wasStreak && sink != nil {
		sink.ResetConflictBudget()
	}
}

// Consecutive returns the MAX consecutive-conflict counter across
// all keys. Useful for tests and observability where the caller
// doesn't need per-key granularity. For per-key queries, use
// ConsecutiveForKey.
//
// Returning the max (rather than the sum) preserves the semantics
// of the old single-counter design: "how bad is the worst streak
// right now?" The sum would conflate multiple healthy keys with
// one stuck key.
func (b *ConflictBudget) Consecutive() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	max := 0
	for _, s := range b.streaks {
		if s.consecutive > max {
			max = s.consecutive
		}
	}
	return max
}

// ConsecutiveForKey returns the consecutive-conflict counter for a
// specific key. Returns 0 if the key has no active streak — this
// covers both "never seen this key" and "the key escalated and
// was eagerly removed". The zero-default is intentional so
// callers (tests, diagnostics) can use ConsecutiveForKey as a
// presence-and-streak probe without a separate `hasKey` check.
func (b *ConflictBudget) ConsecutiveForKey(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if state, ok := b.streaks[key]; ok {
		return state.consecutive
	}
	return 0
}
