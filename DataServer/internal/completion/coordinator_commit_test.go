// Package completion / coordinator_commit_test.go
//
// Per-phase split (declare / progress / complete-upload / commit /
// reconcile) extracted from coordinator_test.go. This file owns the
// CAS-budget wrapper used by the CommitAttempt path (MarkCommitted),
// and (by extension) the same wrapper used by CompleteUpload's
// UpdateReadyCountExhaustive + SetExpired and ReconcileAttempt's
// SetExpiredByID.
//
// Coverage is the Verdetto P2 (Blocco 5) wiring of ConflictBudget on
// the Coordinator: the nil-return ambiguity, the threshold-boundary
// contract (1st & 2nd consecutive ErrTransitionConflict pass through
// unchanged; 3rd flips to ErrConflictBudgetExhausted), the documented
// quirk that the boundary error's chain has ErrConflictBudgetExhausted
// but NOT ErrTransitionConflict, and the c.budget == nil bypass for
// future Coordinator configurations built without a budget wrapper.
package completion

import (
	"errors"
	"strings"
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// recordAttemptCommitsCAS tests
//
// Verdetto P2 (Blocco 5): the Coordinator wraps CAS errors from the
// three canonical attempt_commits CAS paths through ConflictBudget.
// These tests exercise the wrapper directly (no DB CAS collision
// required) so the nil-return ambiguity and threshold-boundary
// contract are pinned down independently of the higher-level
// CompleteUpload / CommitAttempt / ReconcileAttempt callers.
//
// The package-level conflict_budget_test.go already covers the
// bare ConflictBudget surface; this file covers the *Coordinator*
// wiring — the c.budget == nil bypass, the budget reset on
// successful Coordinator-method exit, and the documented quirk that
// the boundary error's chain has ErrConflictBudgetExhausted but NOT
// ErrTransitionConflict (the original is %v-formatted text, not %w).
// ────────────────────────────────────────────────────────────────────────

// TestCoordinator_RecordAttemptCommitsCAS_HappyPath covers the three
// non-escalating call shapes:
//   - nil input          → counter reset, returns nil (success path)
//   - non-CAS err input  → counter unchanged, returns pointer-equal err
//   - CAS err below thr → counter +1, returns pointer-equal err (worker
//     retries on the same path)
func TestCoordinator_RecordAttemptCommitsCAS_HappyPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Test-suite pollution guard: reset the budget before the test
	// body so any bleed from a sibling test does not skew the
	// counter assertions below.
	_ = c.recordAttemptCommitsCAS("test", nil)

	// 1. nil input — counter resets, returns nil.
	if err := c.recordAttemptCommitsCAS("test", nil); err != nil {
		t.Errorf("nil input: want nil err, got %v", err)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("after nil reset: budget.Consecutive() = %d, want 0", got)
	}

	// 2. non-CAS err — counter unchanged, pointer-equal passthrough.
	otherErr := errors.New("some non-CAS infrastructure error")
	if err := c.recordAttemptCommitsCAS("test", otherErr); err != otherErr {
		t.Errorf("non-CAS err: want pointer-equal passthrough (err=%v), got %v", otherErr, err)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("non-CAS err: budget.Consecutive() = %d, want 0", got)
	}

	// 3. CAS err under threshold — counter advances by 1, pointer-
	//    equal passthrough (caller wraps with ErrTransitionConflict
	//    on its own path).
	confErr := conflictErr("stale fence (recordAttemptCommitsCAS happy path)")
	if err := c.recordAttemptCommitsCAS("test", confErr); err != confErr {
		t.Errorf("under-threshold CAS: want pointer-equal passthrough, got %v", err)
	}
	if got := c.budget.Consecutive(); got != 1 {
		t.Errorf("after 1 under-threshold CAS: budget.Consecutive() = %d, want 1", got)
	}
}

// TestCoordinator_RecordAttemptCommitsCAS_CASExhaustionFallsBackToBudgetError
// pins down the boundary behaviour for the wrapper:
//   - The 1st and 2nd consecutive ErrTransitionConflict pass through
//     unchanged (preserving the ErrTransitionConflict chain so the
//     worker over gRPC retries on the same path).
//   - The 3rd consecutive conflict flips the wrapping entirely to
//     ErrConflictBudgetExhausted. The original ErrTransitionConflict
//     is logged as %v descriptive text, NOT in the errors.Is chain —
//     callers inspecting the error MUST check the ErrConflictBudgetExhausted
//     sentinel directly, and use the budget's Consecutive() counter
//     for diagnostics.
func TestCoordinator_RecordAttemptCommitsCAS_CASExhaustionFallsBackToBudgetError(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Test-suite pollution guard.
	_ = c.recordAttemptCommitsCAS("test", nil)

	confErr := conflictErr("locked attempt_commits row (exhaustion test)")

	// Default ConflictBudgetPolicy: ConsecutiveConflictThreshold=3,
	// so the 3rd consecutive conflict is the boundary; the 1st and
	// 2nd must propagate the original ConfErr unchanged.
	for i := 0; i < 2; i++ {
		err := c.recordAttemptCommitsCAS("test", confErr)
		if err == nil {
			t.Fatalf("iteration %d: want non-nil err (the original confErr), got nil", i+1)
		}
		if !errors.Is(err, ErrTransitionConflict) {
			t.Errorf("iteration %d: errors.Is chain lost ErrTransitionConflict (got %v)", i+1, err)
		}
		if errors.Is(err, ErrConflictBudgetExhausted) {
			t.Errorf("iteration %d: under-threshold must NOT escalate ErrConflictBudgetExhausted (got %v)", i+1, err)
		}
	}
	if got := c.budget.Consecutive(); got != 2 {
		t.Errorf("budget.Consecutive() = %d, want 2 before boundary call", got)
	}

	// 3rd consecutive — boundary. Returned error wraps
	// ErrConflictBudgetExhausted; original ErrTransitionConflict is
	// NO LONGER in the errors.Is chain (documented quirk: original
	// is %v-text, not %w-chain). The key is eagerly removed from
	// the per-key map (Blocco 3 per-key design), so BOTH
	// Consecutive() and consecutiveForKey("test") return 0 after
	// the boundary — the escalation error is the real signal, not
	// the post-escalation counter. The pre-boundary counter check
	// above (the loop's final iteration) confirms the streak reached 2.
	boundaryErr := c.recordAttemptCommitsCAS("test", confErr)
	if boundaryErr == nil {
		t.Fatal("3rd consecutive: want non-nil ErrConflictBudgetExhausted, got nil")
	}
	if !errors.Is(boundaryErr, ErrConflictBudgetExhausted) {
		t.Errorf("3rd consecutive: errors.Is did not match ErrConflictBudgetExhausted (got %v)", boundaryErr)
	}
	if errors.Is(boundaryErr, ErrTransitionConflict) {
		t.Errorf("3rd consecutive: ErrTransitionConflict must NOT be in errors.Is chain (only %%v-formatted; got %v)", boundaryErr)
	}
	// Post-escalation: the key is eagerly removed (consecutiveForKey
	// returns 0 for a non-existent key). This is the intended
	// Blocco 3 behaviour — the budget is "armed" again, ready for a
	// fresh streak if the caller retries.
	if got := c.budget.consecutiveForKey("test"); got != 0 {
		t.Errorf("budget.consecutiveForKey(\"test\") = %d, want 0 (eager-delete on escalation)", got)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("budget.Consecutive() = %d, want 0 (no active streaks after eager-delete)", got)
	}

	// The boundary error message SHOULD still reference the
	// underlying transition conflict textually so operators reading
	// logs see the original cause; this guards the 2nd-arg of
	// fmt.Errorf staying as %v.
	if !strings.Contains(boundaryErr.Error(), "lock") && !strings.Contains(boundaryErr.Error(), "transition") {
		t.Errorf("3rd consecutive: boundary error text should still describe the underlying transition conflict; got %q", boundaryErr.Error())
	}

	// Reset and confirm the budget is reusable across Coordinator
	// method exits (e.g., after Manual Restart in supervisor).
	c.recordAttemptCommitsCAS("test", nil)
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("after nil reset on boundary streak: budget.Consecutive() = %d, want 0", got)
	}
}

// TestCoordinator_RecordAttemptCommitsCAS_NilBudgetBypass locks in
// the c.budget == nil first-guard in coordinator.go. The guard is
// intended for future Coordinator configurations built without a
// ConflictBudget wrapper (e.g., legacy callers during migration);
// today, NewCoordinator always initializes the budget, but the test
// covers the aid path.
//
// Behaviour under nil budget: every input (nil, non-CAS, CAS) passes
// through unchanged with no panic and no counter advancement.
func TestCoordinator_RecordAttemptCommitsCAS_NilBudgetBypass(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Set up the nil-budget scenario.
	c.budget = nil

	confErr := conflictErr("pure CAS under nil-budget (bypass test)")
	otherErr := errors.New("non-CAS err under nil-budget")

	// nil input — must not panic, must return nil.
	if err := c.recordAttemptCommitsCAS("test", nil); err != nil {
		t.Errorf("nil-budget + nil input: want nil, got %v", err)
	}

	// 5 consecutive CAS errs — must all pass through pointer-equal,
	// no panic, no counter (because there is no budget to count).
	for i := 0; i < 5; i++ {
		err := c.recordAttemptCommitsCAS("test", confErr)
		if err != confErr {
			t.Errorf("iteration %d: nil-budget + CAS err want pointer-equal passthrough, got %v", i+1, err)
		}
	}

	// Non-CAS err — must pass through pointer-equal.
	if err := c.recordAttemptCommitsCAS("test", otherErr); err != otherErr {
		t.Errorf("nil-budget + non-CAS err want pointer-equal passthrough, got %v", err)
	}
}
