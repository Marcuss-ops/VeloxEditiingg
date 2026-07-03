// Package completion / conflict_budget_chaos_test.go
//
// Chaos tests for ConflictBudget. Models the real-world
// interleaving the Coordinator sees when SQLite returns transient
// SQLITE_BUSY alongside genuine ErrTransitionConflict. The
// Contract governs:
//
//   - ErrBusy / SQLITE_BUSY-equivalent: must NOT count toward the
//     conflict streak. Record returns the busy err unchanged so the
//     caller (typically recover_output or the worker over gRPC)
//     surfaces it to its retry layer.
//   - ErrTransitionConflict: counts toward the streak.
//   - nil: clears the streak.
//
// Three targeted chaos runs + one wrap-pattern run:
//
//  1. TestConflictBudget_Chaos_NoEscalationUnderThreshold — 4
//     records: B C B C with threshold=3; counter stays ≤ 2, no
//     escalation.
//  2. TestConflictBudget_Chaos_MidSequenceNilResetClearsStreak —
//     mid-sequence nil Record drops the counter back to 0 even
//     after a partial streak.
//  3. TestConflictBudget_Chaos_FinalSpecSequenceTriggersEscalation —
//     the literal user-described chaos sequence in one golden run:
//     [B, C, B, nil, C, C, C] with threshold=3. Each step's
//     expectation (err / counter) asserted individually.
//  4. TestConflictBudget_Chaos_WrapPatternMirrorsCoordinator — the
//     same chaos sequence fed through the coordinator.go wrap
//     closure, verifying the "keep input err separate" contract.
package completion

import (
	"errors"
	"testing"
)

// busyErr returns an error that simulates the shape of an SQLite
// SQLITE_BUSY error returned via database/sql (the completion
// package does not import mattn/sqlite3, and the test must stay
// driver-independent). The signature mirrors a transient lock
// contention so the "any non-ErrTransitionConflict error is
// passed through unchanged" branch of ConflictBudget.Record is
// exercised.
//
// The exact string is not matched anywhere — Record classifies
// errors via errors.Is(err, ErrTransitionConflict), and any
// other value is treated as "passthrough, do not increment".
// The wording is intentionally human-readable so a failure
// message ("got 'SQLite: database is locked'") is a faithful
// signal of what was injected.
func busyErr(reason string) error {
	return errors.New("SQLite: database is locked: " + reason)
}

// ── sub-test 1 ─────────────────────────────────────────────────────────

// TestConflictBudget_Chaos_NoEscalationUnderThreshold verifies the
// 4-record "chaos slice" with 2 busy errors interleaved among 2
// genuine ErrTransitionConflict. With threshold=3 the boundary is
// at consecutive >= 3; this sequence must keep consecutive ≤ 2.
func TestConflictBudget_Chaos_NoEscalationUnderThreshold(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")

	type step struct {
		name       string
		input      error
		wantConsec int
	}
	seq := []step{
		{"busy-1 (absorbed)", busyErr("worker-A holds writer lock"), 0}, // busy: passthrough, no count
		{"conflict-1", cErr, 1},
		{"busy-2 (absorbed)", busyErr("checkpoint wedge"), 1}, // busy: passthrough, no count
		{"conflict-2", cErr, 2},                               // threshold-1, OK
	}

	for i, s := range seq {
		got := b.Record(testKey, s.input)
		// Behaviour classification per input:
		//   - ErrTransitionConflict under threshold: Record returns
		//     nil (caller surfaces original err via wrapping). At
		//     threshold the wrap is what surfaces ErrConflictBudgetExhausted;
		//     Record itself also wraps ErrConflictBudgetExhausted at
		//     boundary — see cover by the wrap-pattern sub-test #4.
		//   - busy / passthrough: Record returns input unchanged.
		//   - nil: Record returns nil after resetting counter.
		switch {
		case errors.Is(s.input, ErrTransitionConflict):
			if got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
				t.Errorf("step %d (%s): budget unexpectedly escalated: %v", i+1, s.name, got)
			}
		case s.input == nil:
			if got != nil {
				t.Errorf("step %d (%s): Record(nil) returned %v; expected nil", i+1, s.name, got)
			}
		default:
			// busy / passthrough — expected to come back unchanged.
			if got == nil {
				t.Errorf("step %d (%s): Record(busy) returned nil; expected passthrough", i+1, s.name)
			} else if got.Error() != s.input.Error() {
				t.Errorf("step %d (%s): Record(busy) returned %q; expected unchanged %q", i+1, s.name, got.Error(), s.input.Error())
			}
		}
		if c := b.Consecutive(); c != s.wantConsec {
			t.Errorf("step %d (%s): consecutive = %d, want %d", i+1, s.name, c, s.wantConsec)
		}
	}

	// Final invariant: counter at end = number of GENUINE conflicts
	// in the streak (i.e. busy errors must not have leaked into the
	// counter). At threshold-1 with budget still alive.
	if c := b.Consecutive(); c != 2 {
		t.Errorf("after 4-record chaos, consecutive = %d, want 2 (only 2 genuine conflicts should count)", c)
	}
}

// ── sub-test 2 ─────────────────────────────────────────────────────────

// TestConflictBudget_Chaos_MidSequenceNilResetClearsStreak verifies
// a nil Record mid-sequence drops the counter back to 0 even after
// ErrTransitionConflict entries already advanced the streak.
func TestConflictBudget_Chaos_MidSequenceNilResetClearsStreak(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")

	// Build a partial streak.
	for i := 0; i < 2; i++ {
		if got := b.Record(testKey, cErr); got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
			t.Fatalf("pre-reset conflict #%d unexpectedly escalated: %v", i+1, got)
		}
	}
	if got := b.Consecutive(); got != 2 {
		t.Fatalf("pre-reset counter = %d, want 2", got)
	}

	// Inject a busy between partial streak and reset — busy must
	// NOT touch the counter.
	busyBetween := busyErr("checkpoint wedge")
	if got := b.Record(testKey, busyBetween); got == nil || got.Error() != busyBetween.Error() {
		t.Errorf("busy passthrough: got %v, expected passthrough %v", got, busyBetween)
	}
	if got := b.Consecutive(); got != 2 {
		t.Errorf("post-busy counter = %d, busy must not count; want 2", got)
	}

	// Mid-sequence nil reset.
	if got := b.Record(testKey, nil); got != nil {
		t.Errorf("mid-sequence nil reset should not return error: %v", got)
	}
	if got := b.Consecutive(); got != 0 {
		t.Errorf("after mid-sequence nil reset, counter = %d, want 0", got)
	}

	// Post-reset: a fresh conflict starts a new streak at 1.
	if got := b.Record(testKey, cErr); got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("post-reset conflict unexpectedly escalated: %v", got)
	}
	if got := b.Consecutive(); got != 1 {
		t.Errorf("post-reset counter = %d, want 1 (fresh streak)", got)
	}
}

// ── sub-test 3 ─────────────────────────────────────────────────────────

// TestConflictBudget_Chaos_FinalSpecSequenceTriggersEscalation runs
// the literal user-described chaos sequence in one golden run, with
// the post-reset phase pushing the counter over the threshold.
//
// Sequence: [B, C, B, nil, C, C, C]  with threshold=3
//
//	step  input  expected consecutive  expected return
//	────  ─────  ────────────────────  ───────────────────────
//	1     B      0                      busy err unchanged (passthrough)
//	2     C      1                      nil (under threshold)
//	3     B      1                      busy err unchanged (passthrough)
//	4     nil    0                      nil (real reset; cleared counter)
//	5     C      1                      nil (under threshold)
//	6     C      2                      nil (under threshold)
//	7     C      3 (boundary)           wrapped ErrConflictBudgetExhausted
func TestConflictBudget_Chaos_FinalSpecSequenceTriggersEscalation(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	cErr := conflictErr("stale fence")
	busy1 := busyErr("writer lock held by worker-A")
	busy2 := busyErr("checkpoint wedge")

	type expect struct {
		consec int
		// errMode:
		//   "passthrough" → return == input unchanged
		//   "nil"         → Record returned nil
		//   "escalate"    → Record returned non-nil, errors.Is(_, ErrConflictBudgetExhausted)
		errMode string
	}
	seq := []struct {
		name  string
		input error
		want  expect
	}{
		{"busy-1", busy1, expect{consec: 0, errMode: "passthrough"}},
		{"conflict-1", cErr, expect{consec: 1, errMode: "nil"}},
		{"busy-2", busy2, expect{consec: 1, errMode: "passthrough"}},
		{"nil-reset-mid", nil, expect{consec: 0, errMode: "nil"}},
		{"conflict-2", cErr, expect{consec: 1, errMode: "nil"}},
		{"conflict-3", cErr, expect{consec: 2, errMode: "nil"}},
		// boundary step: counter is OBSERVED at 2 immediately
		// before this Record call (from step 6). After the
		// Record call returns the boundary error, eager-delete
		// drops the key — so Consecutive() is 0 immediately
		// after, not 3. The escalation err is the real
		// signal here.
		{"conflict-4-final (boundary)", cErr, expect{consec: 0, errMode: "escalate"}},
	}

	for i, s := range seq {
		got := b.Record(testKey, s.input)

		switch s.want.errMode {
		case "passthrough":
			if got == nil {
				t.Errorf("step %d (%s): Record(busy) returned nil, expected unchanged passthrough", i+1, s.name)
				continue
			}
			if got.Error() != s.input.Error() {
				t.Errorf("step %d (%s): Record(busy) returned %q; expected unchanged %q", i+1, s.name, got.Error(), s.input.Error())
			}
		case "nil":
			if got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
				t.Errorf("step %d (%s): Record unexpectedly escalated: %v", i+1, s.name, got)
			}
		case "escalate":
			if got == nil {
				t.Errorf("step %d (%s): Record returned nil at boundary; expected ErrConflictBudgetExhausted", i+1, s.name)
				continue
			}
			if !errors.Is(got, ErrConflictBudgetExhausted) {
				t.Errorf("step %d (%s): Record returned %v; expected errors.Is(_, ErrConflictBudgetExhausted)", i+1, s.name, got)
			}
		}

		if c := b.Consecutive(); c != s.want.consec {
			t.Errorf("step %d (%s): consecutive = %d, want %d", i+1, s.name, c, s.want.consec)
		}
	}

	// Final invariants:
	//   - Blocco 3: the key is eagerly removed on escalation, so
	//     Consecutive() returns 0 (no active streaks) after the
	//     boundary. The escalation error is the real signal.
	//   - the very last errMode was "escalate".
	last := seq[len(seq)-1]
	if last.want.errMode != "escalate" {
		t.Fatalf("internal: last step should escalate; got %q", last.want.errMode)
	}
	if c := b.Consecutive(); c != 0 {
		t.Errorf("final counter = %d, want 0 (eager-delete on escalation, no active streaks)", c)
	}
	if c := b.consecutiveForKey(testKey); c != 0 {
		t.Errorf("final consecutiveForKey(testKey) = %d, want 0 (key eagerly removed)", c)
	}
	// Post-escalation: a fresh conflict on the same key starts a
	// NEW streak at 1 (eager-delete re-arms the key). The next
	// Record returns nil (under threshold, fresh streak), NOT
	// ErrConflictBudgetExhausted. The old "counter stays at 3
	// and every subsequent conflict escalates" behaviour was
	// replaced by the eager-delete design in Blocco 3 — the
	// escalation is a SIGNAL to the caller, not a permanent
	// circuit-breaker.
	// Record directly returns nil for under-threshold conflict
	// (the wrap closure in coordinator.go::recordAttemptCommitsCAS
	// surfaces the original err to callers — but Record's own
	// contract is "passthrough only for non-CAS errors, nil for
	// CAS-under-threshold, wrapped ErrConflictBudgetExhausted at
	// boundary"). Post-escalation + under-threshold means nil.
	if got := b.Record(testKey, cErr); got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("post-escalation conflict must start a fresh streak (under threshold, no escalation); got %v", got)
	}
	if got := b.consecutiveForKey(testKey); got != 1 {
		t.Errorf("post-escalation consecutiveForKey(testKey) = %d, want 1 (fresh streak)", got)
	}
}

// ── sub-test 4 ─────────────────────────────────────────────────────────

// TestConflictBudget_Chaos_WrapPatternMirrorsCoordinator replays the
// user-described chaos sequence through the wrap closure that
// coordinator.go::recordAttemptCommitsCAS uses at every canonical
// attempt_commits CAS path. This is the production caller; if the
// wrap mis-handles ErrBusy / nil-mid-sequence / final escalation,
// the chain loses observable surface (the worker's gRPC retry
// layer would miss ErrTransitionConflict).
func TestConflictBudget_Chaos_WrapPatternMirrorsCoordinator(t *testing.T) {
	budget := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})

	// Mirror coordinator.recordAttemptCommitsCAS verbatim.
	wrap := func(err error) error {
		budgetErr := budget.Record(testKey, err)
		if budgetErr == nil {
			return err // under-threshold or reset — surface original err
		}
		return budgetErr // over-threshold — escalation sentinel
	}

	cErr := conflictErr("stale fence")
	busy1 := busyErr("writer lock held by worker-A")
	busy2 := busyErr("checkpoint wedge")

	// Step 1: busy passthrough. Wrap returns busy unchanged.
	if got := wrap(busy1); got == nil || got.Error() != busy1.Error() {
		t.Errorf("step 1 wrap(busy) = %v; expected passthrough %q", got, busy1.Error())
	}
	if c := budget.Consecutive(); c != 0 {
		t.Errorf("step 1: counter = %d, want 0 (busy must not count)", c)
	}

	// Step 2: conflict #1 under threshold. Wrap returns cErr.
	if got := wrap(cErr); got == nil {
		t.Errorf("step 2 wrap(conflict) = nil; expected original cErr")
	} else if !errors.Is(got, ErrTransitionConflict) {
		t.Errorf("step 2 wrap(conflict) = %v; chain lost ErrTransitionConflict", got)
	} else if errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("step 2 wrap(conflict) = %v; under-threshold must NOT surface exhausted sentinel", got)
	}
	if c := budget.Consecutive(); c != 1 {
		t.Errorf("step 2: counter = %d, want 1", c)
	}

	// Step 3: busy at streak=1 → passthrough. Counter must NOT bump.
	if got := wrap(busy2); got == nil || got.Error() != busy2.Error() {
		t.Errorf("step 3 wrap(busy) = %v; expected passthrough %q", got, busy2.Error())
	}
	if c := budget.Consecutive(); c != 1 {
		t.Errorf("step 3: counter = %d, want 1 (busy must not count)", c)
	}

	// Step 4: nil reset. Wrap returns nil; counter is back to 0.
	if got := wrap(nil); got != nil {
		t.Errorf("step 4 wrap(nil) = %v; expected nil", got)
	}
	if c := budget.Consecutive(); c != 0 {
		t.Errorf("step 4: counter = %d, want 0 (nil reset)", c)
	}

	// Step 5–6: post-reset conflicts under threshold.
	for i := 5; i <= 6; i++ {
		if got := wrap(cErr); got == nil {
			t.Errorf("step %d wrap(conflict) = nil; expected cErr", i)
		} else if !errors.Is(got, ErrTransitionConflict) || errors.Is(got, ErrConflictBudgetExhausted) {
			t.Errorf("step %d wrap(conflict) = %v; expected ErrTransitionConflict-chain err (no exhausted)", i, got)
		}
	}
	if c := budget.Consecutive(); c != 2 {
		t.Errorf("post-steps 5–6: counter = %d, want 2", c)
	}

	// Step 7: post-reset boundary. Wrap returns ErrConflictBudgetExhausted.
	got := wrap(cErr)
	if got == nil {
		t.Fatalf("step 7 wrap(conflict) = nil; expected ErrConflictBudgetExhausted")
	}
	if !errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("step 7 wrap(conflict) = %v; expected ErrConflictBudgetExhausted", got)
	}
	// Blocco 3: the key is eagerly removed on escalation, so
	// Consecutive() returns 0 (no active streaks) after the
	// boundary. The escalation error is the real signal.
	if c := budget.Consecutive(); c != 0 {
		t.Errorf("step 7: counter = %d, want 0 (eager-delete on escalation, no active streaks)", c)
	}
	if c := budget.consecutiveForKey(testKey); c != 0 {
		t.Errorf("step 7: consecutiveForKey(testKey) = %d, want 0 (key eagerly removed)", c)
	}
}
