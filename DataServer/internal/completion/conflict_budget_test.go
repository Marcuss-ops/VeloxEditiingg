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
		_ = b.Record(conflictErr("test"))
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("after 2 conflicts, consecutive = %d, want 2", got)
	}
	if err := b.Record(nil); err != nil {
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
		returned := b.Record(otherErr)
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

// ── escalation at threshold boundary ─────────────────────────────────

func TestConflictBudget_EscalatesAtThresholdBoundary(t *testing.T) {
	// threshold=3: docs say consecutive >= threshold escalates.
	// The 3rd consecutive conflict IS the boundary; the 4th would
	// also escalate.
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")

	// First conflict: under threshold → returns nil (caller surfaces
	// the ErrTransitionConflict on its own).
	if err := b.Record(cErr); err != nil {
		t.Errorf("1st conflict should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 1 {
		t.Errorf("consecutive after 1 conflict = %d, want 1", got)
	}
	// 2nd conflict: still under.
	if err := b.Record(cErr); err != nil {
		t.Errorf("2nd conflict should not escalate: %v", err)
	}
	// 3rd conflict: at the boundary → escalate now.
	err := b.Record(cErr)
	if err == nil {
		t.Fatal("3rd conflict should escalate at the threshold boundary")
	}
	if !errors.Is(err, ErrConflictBudgetExhausted) {
		t.Errorf("expected ErrConflictBudgetExhausted, got %v", err)
	}
	if got := b.Consecutive(); got != 3 {
		t.Errorf("consecutive at boundary = %d, want 3", got)
	}
}

// ── reset on nil clears a streak ─────────────────────────────────────

func TestConflictBudget_SuccessResetDoesNotAccumulateAcrossStreaks(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")
	// Streak 1: 2 conflicts then success.
	_ = b.Record(cErr)
	_ = b.Record(cErr)
	if err := b.Record(nil); err != nil {
		t.Errorf("success reset should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("streak 1 reset failed: %d", got)
	}
	// Streak 2: another 2 conflicts then nil. Should NOT escalate
	// since streak 2's counter starts at 0.
	for i := 0; i < 2; i++ {
		if err := b.Record(cErr); err != nil {
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
	if err := b.Record(cErr); err != nil {
		t.Errorf("wave 1 err 1 should not escalate: %v", err)
	}
	clk.Advance(500 * time.Millisecond)
	if err := b.Record(cErr); err != nil {
		t.Errorf("wave 1 err 2 should not escalate: %v", err)
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("after wave 1, consecutive = %d, want 2", got)
	}
	// Jump past the window.
	clk.Advance(5 * time.Second)
	if err := b.Record(cErr); err != nil {
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
	_ = b.Record(cErr)        // counter=1
	_ = b.Record(cErr)        // counter=2
	returned := b.Record(otherErr)
	if returned == nil || returned.Error() != otherErr.Error() {
		t.Errorf("Record(otherErr) should pass through unchanged; got %v want %v", returned, otherErr)
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("non-conflict must not bump counter: got %d, want 2", got)
	}
	// 3rd conflict: at boundary → escalate.
	err := b.Record(cErr)
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
		budgetErr := budget.Record(err)
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
