// Package completion / conflict_budget_test.go
//
// Tests for ConflictBudget. Uses a closure-driven mock clock so the
// ResetWindow streak-refresh can be driven deterministically
// without sleeping through real wall-clock advances.
//
// Coverage matrix:
//   - nil err → resets counter, returns nil.
//   - non-transition-conflict err → passes through unchanged,
//     counter unchanged.
//   - transition-conflict err under threshold → counter +1, returns
//     nil so the caller can surface ErrTransitionConflict itself.
//   - transition-conflict err at/above threshold → counter +1,
//     returns wrapped ErrConflictBudgetExhausted.
//   - mixed pattern (infra/element/conflict): ConflictBudget only
//     counts ErrTransitionConflict; others do not increment.
package completion

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// ── mock helper ────────────────────────────────────────────────────────

type conflictBudgetMockClock struct {
	now time.Time
}

// newConflictBudgetMockClock returns a closure-producing helper
// for the budget. Callers pass `clk.nowFn()` to WithClock; then dial
// time forward via `clk.Advance(...)`.
func newConflictBudgetMockClock(start time.Time) *conflictBudgetMockClock {
	return &conflictBudgetMockClock{now: start}
}

func (m *conflictBudgetMockClock) Advance(d time.Duration) {
	m.now = m.now.Add(d)
}

func (m *conflictBudgetMockClock) nowFn() func() time.Time {
	clk := m
	return func() time.Time { return clk.now }
}

// conflictErr returns a fresh error that errors.Is-matches
// ErrTransitionConflict so ConflictBudget.Record classifies it
// correctly.
func conflictErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrTransitionConflict, msg)
}

// ── nil / passthrough ──────────────────────────────────────────────────

func TestConflictBudget_NilResetsCounter(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	for i := 0; i < 2; i++ {
		_ = b.Record(testKey, conflictErr("test"))
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("after 2 conflicts, consecutive = %d, want 2", got)
	}
	if err := b.Record(testKey, nil); err != nil {
		t.Errorf("nil Record should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("after nil reset, consecutive = %d, want 0", got)
	}
}

func TestConflictBudget_NonConflictPassesThrough(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	otherErr := errors.New("some non-conflict error")
	for i := 0; i < 10; i++ {
		returned := b.Record(testKey, otherErr)
		if returned == nil {
			t.Errorf("Record iteration %d: returned nil for non-conflict err; should pass through unchanged", i+1)
			continue
		}
		if returned.Error() != otherErr.Error() {
			t.Errorf("Record iteration %d: returned %v, want unchanged %v", i+1, returned, otherErr)
		}
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("non-conflict errors must NOT count: got %d", got)
	}
}

// TestConflictBudget_ConsecutiveForKey_ZeroDefault locks down the
// "absent key returns 0" contract for the per-key accessor. This
// covers three cases the docstring promises:
//   - key was never recorded (truly absent)
//   - key was recorded then nil-reset (eagerly removed)
//   - key was recorded then escalated (eagerly removed)
// In all three cases ConsecutiveForKey must return 0, NOT panic
// and NOT return a stale value from a sibling key.
func TestConflictBudget_ConsecutiveForKey_ZeroDefault(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")

	// Case 1: key was never recorded → 0.
	if got := b.ConsecutiveForKey("never-seen"); got != 0 {
		t.Errorf("never-seen key: ConsecutiveForKey = %d, want 0", got)
	}

	// Case 2: key recorded then nil-reset → 0.
	_ = b.Record(testKey, cErr)
	_ = b.Record(testKey, cErr)
	if got := b.ConsecutiveForKey(testKey); got != 2 {
		t.Fatalf("setup: ConsecutiveForKey = %d, want 2 before nil reset", got)
	}
	_ = b.Record(testKey, nil) // nil reset removes the key
	if got := b.ConsecutiveForKey(testKey); got != 0 {
		t.Errorf("post-nil-reset: ConsecutiveForKey = %d, want 0 (key eagerly removed)", got)
	}

	// Case 3: key recorded then escalated → 0 (eager-delete on
	// escalation is the Blocco 3 invariant).
	_ = b.Record("escalate-me", cErr)
	_ = b.Record("escalate-me", cErr)
	boundary := b.Record("escalate-me", cErr)
	if !errors.Is(boundary, ErrConflictBudgetExhausted) {
		t.Fatalf("setup: boundary should escalate, got %v", boundary)
	}
	if got := b.ConsecutiveForKey("escalate-me"); got != 0 {
		t.Errorf("post-escalation: ConsecutiveForKey = %d, want 0 (key eagerly removed)", got)
	}

	// Case 4: a sibling key with a non-zero streak must NOT bleed
	// into the absent-key probe.
	_ = b.Record("sibling", cErr)
	_ = b.Record("sibling", cErr)
	if got := b.ConsecutiveForKey("never-seen"); got != 0 {
		t.Errorf("absent key with active sibling streak: ConsecutiveForKey = %d, want 0 (no bleed)", got)
	}
}

// ── escalation at threshold boundary ─────────────────────────────────

func TestConflictBudget_EscalatesAtThresholdBoundary(t *testing.T) {
	// threshold=3: docs say consecutive >= threshold escalates.
	// The 3rd consecutive conflict IS the boundary; the 4th would
	// also escalate.
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")

	// First conflict: under threshold → returns nil (caller surfaces
	// the ErrTransitionConflict on its own).
	if err := b.Record(testKey, cErr); err != nil {
		t.Errorf("1st conflict should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 1 {
		t.Errorf("consecutive after 1 conflict = %d, want 1", got)
	}
	// 2nd conflict: still under.
	if err := b.Record(testKey, cErr); err != nil {
		t.Errorf("2nd conflict should not escalate: %v", err)
	}
	// 3rd conflict: at the boundary → escalate now.
	err := b.Record(testKey, cErr)
	if err == nil {
		t.Fatal("3rd conflict should escalate at the threshold boundary")
	}
	if !errors.Is(err, ErrConflictBudgetExhausted) {
		t.Errorf("expected ErrConflictBudgetExhausted, got %v", err)
	}
	// Blocco 3: the key is eagerly removed from the per-key map
	// on escalation, so BOTH Consecutive() and ConsecutiveForKey
	// return 0 after the boundary. The escalation error is the
	// real signal, not the post-escalation counter.
	if got := b.Consecutive(); got != 0 {
		t.Errorf("Consecutive() at boundary = %d, want 0 (eager-delete on escalation, no active streaks)", got)
	}
	if got := b.ConsecutiveForKey(testKey); got != 0 {
		t.Errorf("ConsecutiveForKey at boundary = %d, want 0 (key eagerly removed)", got)
	}
}

// ── reset on nil clears a streak ─────────────────────────────────────

func TestConflictBudget_SuccessResetDoesNotAccumulateAcrossStreaks(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")
	// Streak 1: 2 conflicts then success.
	_ = b.Record(testKey, cErr)
	_ = b.Record(testKey, cErr)
	if err := b.Record(testKey, nil); err != nil {
		t.Errorf("success reset should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("streak 1 reset failed: %d", got)
	}
	// Streak 2: another 2 conflicts then nil. Should NOT escalate
	// since streak 2's counter starts at 0.
	for i := 0; i < 2; i++ {
		if err := b.Record(testKey, cErr); err != nil {
			t.Errorf("streak 2 conflict #%d should not escalate: %v", i+1, err)
		}
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("streak 2 counter = %d, want 2", got)
	}
}

// ── ResetWindow streak refresh ──────────────────────────────────────

func TestConflictBudget_ResetWindowRefreshesStreak(t *testing.T) {
	clk := newConflictBudgetMockClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	b := NewConflictBudget(ConflictBudgetPolicy{
		ConsecutiveConflictThreshold: 5,
		ResetWindow:                  1 * time.Second,
	}).WithClock(clk.nowFn())
	cErr := conflictErr("stale fence")

	// Wave 1: 2 errors within the window.
	clk.Advance(0)
	if err := b.Record(testKey, cErr); err != nil {
		t.Errorf("wave 1 err 1 should not escalate: %v", err)
	}
	clk.Advance(500 * time.Millisecond)
	if err := b.Record(testKey, cErr); err != nil {
		t.Errorf("wave 1 err 2 should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("after wave 1, consecutive = %d, want 2", got)
	}
	// Jump past the window.
	clk.Advance(5 * time.Second)
	if err := b.Record(testKey, cErr); err != nil {
		t.Errorf("post-window err should restart streak: %v", err)
	}
	if got := b.Consecutive(); got != 1 {
		t.Errorf("after window refresh, consecutive = %d, want 1", got)
	}
}

// ── mixed: only transition-conflict counts ──────────────────────────

func TestConflictBudget_MixedPattern_OnlyConflictsCount(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")
	otherErr := errors.New("provider returned 503")

	// Pattern: conflict, conflict, other, conflict (4th in
	// sequence but only 3rd counted).
	_ = b.Record(testKey, cErr)        // counter=1
	_ = b.Record(testKey, cErr)        // counter=2
	returned := b.Record(testKey, otherErr)
	if returned == nil || returned.Error() != otherErr.Error() {
		t.Errorf("Record(otherErr) should pass through unchanged; got %v want %v", returned, otherErr)
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("non-conflict must not bump counter: got %d, want 2", got)
	}
	// 3rd conflict: at boundary → escalate.
	err := b.Record(testKey, cErr)
	if err == nil {
		t.Fatal("3rd conflict at boundary should escalate")
	}
	if !errors.Is(err, ErrConflictBudgetExhausted) {
		t.Errorf("expected ErrConflictBudgetExhausted, got %v", err)
	}
}

// ── wrap-pattern: mirror coordinator.recordAttemptCommitsCAS ──────

// TestConflictBudget_WrapPattern_MirrorsCoordinator exercises the
// exact wiring the Coordinator uses at every canonical attempt_commits
// CAS path. The "wrap" closure reproduces coordinator.go::
// recordAttemptCommitsCAS — keep the input err separate, escalate
// only when Record returns non-nil, otherwise surface the original
// err so the worker over gRPC handles its retry.
//
// Threshold=3 → the 3rd consecutive conflict is the boundary; the
// wrap returns ErrConflictBudgetExhausted on that call. Under
// threshold the wrap returns the original input err unchanged so
// the chain still hits ErrTransitionConflict at the caller side.
func TestConflictBudget_WrapPattern_MirrorsCoordinator(t *testing.T) {
	budget := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	wrap := func(err error) error {
		// Mirror coordinator.recordAttemptCommitsCAS verbatim:
		budgetErr := budget.Record(testKey, err)
		if budgetErr == nil {
			return err // under-threshold or reset — surface original err
		}
		return budgetErr // over-threshold — escalation sentinel
	}

	cErr := conflictErr("stale fence")

	// First 2 conflicts: under threshold. Wrap returns the
	// original cErr so callsite errors.Is(err, ErrTransitionConflict)
	// still hits the chain.
	for i := 0; i < 2; i++ {
		got := wrap(cErr)
		if got == nil {
			t.Fatalf("iteration %d: wrap must return original cErr, got nil", i+1)
		}
		if !errors.Is(got, ErrTransitionConflict) {
			t.Errorf("iteration %d: wrap returned %v; chain lost ErrTransitionConflict", i+1, got)
		}
		if errors.Is(got, ErrConflictBudgetExhausted) {
			t.Errorf("iteration %d: wrap must NOT return ErrConflictBudgetExhausted under threshold", i+1)
		}
	}

	// 3rd conflict: at boundary. Wrap returns ErrConflictBudgetExhausted.
	got := wrap(cErr)
	if got == nil {
		t.Fatal("3rd conflict: wrap must return non-nil (ErrConflictBudgetExhausted)")
	}
	if !errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("3rd conflict: wrap returned %v; expected ErrConflictBudgetExhausted", got)
	}

	// Reset, repeat threshold escalation with a fresh streak
	// to confirm the budget is reusable across Coordinators
	// (e.g. after a manual reset on master restart).
	budget.Reset()
	if got := budget.Consecutive(); got != 0 {
		t.Errorf("Reset: consecutive = %d, want 0", got)
	}
	// 3 more conflicts → escalate.
	for i := 0; i < 2; i++ {
		if got := wrap(cErr); !errors.Is(got, ErrTransitionConflict) {
			t.Errorf("post-reset iteration %d: wrap must return ErrTransitionConflict-chain err, got %v", i+1, got)
		}
	}
	if got := wrap(cErr); !errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("post-reset 3rd conflict: wrap must return ErrConflictBudgetExhausted, got %v", got)
	}

	// Finally: nil input through the wrap → records nil reset, nil
	// return — exercises the docstring's nil-return-ambiguity note.
	if got := wrap(nil); got != nil {
		t.Errorf("wrap(nil) must return nil (reset path); got %v", got)
	}
}

// TestConflictBudget_ConcurrentKeys_IndependentStreaks locks down
// the per-key isolation guarantee Verdetto P0 #4 (Blocco 3) ships.
// Three independent keys (e.g. three concurrent commit_ids) form
// three independent streaks. A streak on key A that escalates to
// ErrConflictBudgetExhausted MUST NOT cause key B or key C's
// under-threshold conflict to be re-counted as part of the same
// streak. This is the regression-guard for the pre-Blocco-3 single-
// counter design where three concurrent commit_ids would have
// aggregated into one false-positive escalation.
//
// The test is constructed as follows:
//
//   - 3 keys: "commit:alpha", "commit:beta", "commit:gamma".
//   - threshold=3 (3rd conflict escalates).
//   - Phase 1: alpha escalates (3 consecutive → exhausted, key
//     eagerly removed).
//   - Phase 2: beta gets 1 conflict (under threshold, no escalation).
//   - Phase 3: gamma gets 2 conflicts (under threshold, no escalation).
//   - Phase 4: a fresh conflict on alpha starts a new streak at 1
//     (proves eager-delete actually re-arms the key).
//   - Phase 5: beta escalates independently (proves per-key isolation
//     across escalations on different keys).
//
// The pre-Blocco-3 single-counter design would have failed Phase 2
// and Phase 3: beta/gamma would have inherited alpha's streak
// (counter would jump to 4, 5 on beta/gamma) and the budget would
// have already been at the boundary, so even the FIRST beta/gamma
// conflict would have escalated. Per-key isolation prevents both.
func TestConflictBudget_ConcurrentKeys_IndependentStreaks(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")
	const (
		keyA = "commit:alpha"
		keyB = "commit:beta"
		keyC = "commit:gamma"
	)

	// Phase 1: keyA escalates to the boundary.
	// 1st and 2nd conflicts: under threshold, no escalation,
	// counter advances to 1 then 2.
	if err := b.Record(keyA, cErr); err != nil {
		t.Fatalf("alpha conflict #1 should not escalate: %v", err)
	}
	if got := b.ConsecutiveForKey(keyA); got != 1 {
		t.Errorf("alpha counter after 1 conflict = %d, want 1", got)
	}
	if err := b.Record(keyA, cErr); err != nil {
		t.Fatalf("alpha conflict #2 should not escalate: %v", err)
	}
	if got := b.ConsecutiveForKey(keyA); got != 2 {
		t.Errorf("alpha counter after 2 conflicts = %d, want 2", got)
	}
	// 3rd conflict: boundary → escalate, key eagerly removed.
	aBoundary := b.Record(keyA, cErr)
	if aBoundary == nil {
		t.Fatal("alpha boundary (3rd consecutive): want ErrConflictBudgetExhausted, got nil")
	}
	if !errors.Is(aBoundary, ErrConflictBudgetExhausted) {
		t.Errorf("alpha boundary: want errors.Is(_, ErrConflictBudgetExhausted), got %v", aBoundary)
	}
	// Post-escalation: the key is eagerly removed from the map
	// (ConsecutiveForKey returns 0 for absent keys). This is the
	// intended Blocco 3 behaviour: the budget is "armed" again
	// for a fresh streak.
	if got := b.ConsecutiveForKey(keyA); got != 0 {
		t.Errorf("alpha counter post-escalation = %d, want 0 (key eagerly removed)", got)
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("Consecutive() post-alpha-escalation = %d, want 0 (no active streaks)", got)
	}

	// Phase 2: keyB gets 1 conflict. Under threshold, must NOT
	// escalate (the budget was just emptied by alpha's escalation;
	// beta's first conflict starts a fresh streak at 1, NOT a
	// continuation of alpha's streak).
	//
	// Pre-Blocco-3 regression check: with the single-counter
	// design, beta's first conflict would have seen counter=3
	// (from alpha's pre-escalation streak) and escalated. The
	// per-key design prevents this.
	if err := b.Record(keyB, cErr); err != nil {
		t.Errorf("beta conflict #1 must NOT escalate (per-key isolation): %v", err)
	}
	if got := b.ConsecutiveForKey(keyB); got != 1 {
		t.Errorf("beta counter = %d, want 1 (independent from alpha's escalation)", got)
	}
	// alpha is still 0 (eager-deleted) — keys MUST NOT bleed.
	if got := b.ConsecutiveForKey(keyA); got != 0 {
		t.Errorf("alpha counter after beta conflict = %d, want 0 (eager-deleted, no bleed)", got)
	}

	// Phase 3: keyC gets 2 conflicts. Under threshold, must NOT
	// escalate, and must NOT aggregate with alpha or beta.
	for i := 0; i < 2; i++ {
		if err := b.Record(keyC, cErr); err != nil {
			t.Errorf("gamma conflict #%d must not escalate: %v", i+1, err)
		}
	}
	if got := b.ConsecutiveForKey(keyC); got != 2 {
		t.Errorf("gamma counter = %d, want 2 (independent from alpha/beta)", got)
	}
	// Per-key isolation: alpha=0 (eager-deleted), beta=1, gamma=2.
	// No bleed between keys.
	if got := b.ConsecutiveForKey(keyA); got != 0 {
		t.Errorf("alpha counter after gamma conflicts = %d, want 0 (eager-deleted, no bleed)", got)
	}
	if got := b.ConsecutiveForKey(keyB); got != 1 {
		t.Errorf("beta counter after gamma conflicts = %d, want 1 (no bleed)", got)
	}
	// Consecutive() returns the MAX across all keys = max(0, 1, 2) = 2.
	if got := b.Consecutive(); got != 2 {
		t.Errorf("Consecutive() = %d, want 2 (max across keys = gamma's 2)", got)
	}

	// Phase 4: a fresh conflict on keyA starts a new streak at 1
	// (proves eager-delete actually re-arms the key). This is the
	// critical post-escalation invariant: a key that escalated
	// MUST be reusable for a future streak.
	if err := b.Record(keyA, cErr); err != nil {
		t.Errorf("post-escalation alpha conflict must NOT escalate (eager delete re-armed the key): %v", err)
	}
	if got := b.ConsecutiveForKey(keyA); got != 1 {
		t.Errorf("alpha counter after post-escalation conflict = %d, want 1 (eager delete re-armed the key)", got)
	}

	// Phase 5: beta escalates independently of alpha's post-
	// escalation fresh streak. 2 more conflicts → boundary.
	if err := b.Record(keyB, cErr); err != nil {
		t.Errorf("beta conflict #2 must not escalate: %v", err)
	}
	bBoundary := b.Record(keyB, cErr)
	if bBoundary == nil {
		t.Fatal("beta boundary (3rd consecutive): want ErrConflictBudgetExhausted, got nil")
	}
	if !errors.Is(bBoundary, ErrConflictBudgetExhausted) {
		t.Errorf("beta boundary: want errors.Is(_, ErrConflictBudgetExhausted), got %v", bBoundary)
	}
	// Post-beta-escalation: keyB eagerly removed. Final state:
	//   alpha=1 (fresh streak), beta=0 (eager-deleted), gamma=2.
	// Consecutive() = max(1, 0, 2) = 2.
	if got := b.Consecutive(); got != 2 {
		t.Errorf("post-beta-escalation Consecutive() = %d, want 2 (max = gamma's 2)", got)
	}
	if got := b.ConsecutiveForKey(keyA); got != 1 {
		t.Errorf("alpha counter after beta escalation = %d, want 1 (still independent)", got)
	}
	if got := b.ConsecutiveForKey(keyC); got != 2 {
		t.Errorf("gamma counter after beta escalation = %d, want 2 (still independent)", got)
	}
}
