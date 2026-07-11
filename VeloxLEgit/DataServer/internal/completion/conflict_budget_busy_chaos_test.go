// Package completion / conflict_budget_busy_chaos_test.go
//
// Targeted chaos tests for the SQLITE_BUSY class of transient
// errors. Companion to conflict_budget_chaos_test.go: that file
// covers INTERLEAVED busy/conflict/nil sequences; THIS file
// focuses exclusively on the BUSY-only path so the busy-absorption
// contract is locked down independently of the chaos-sequence
// golden run.
//
// Contract (cross-referenced from the Record() docstring on
// conflict_budget.go):
//
//   - sqlite3.ErrBusy (driver-independent: any non-ErrTransitionConflict
//     error falls into the passthrough branch) MUST NOT count
//     toward the consecutive-conflict streak.
//   - Record returns the busy error unchanged so the caller
//     (typically recover_output or the worker over gRPC) can route
//     it to its own short-retry layer.
//   - A pure busy burst (e.g. 5 back-to-back busy errors at the
//     same call site) NEVER escalates, even past the threshold —
//     because the counter stays at 0 forever.
//   - A nil Record AFTER a busy burst MUST cleanly reset state
//     (the streak is already 0 but the bookkeeping must still be
//     idempotent — no sink double-notify, no leftover timing fields).
//
// Sub-tests (6 total):
//  1. TestConflictBudget_Chaos_BusyBackToBack_Absorbed
//  2. TestConflictBudget_Chaos_BusyPastThreshold_NeverEscalates
//  3. TestConflictBudget_Chaos_BusyWrapPattern_Unchanged
//  4. TestConflictBudget_Chaos_BusyThenConflict_FreshStreak
//  5. TestConflictBudget_Chaos_BusyThenNilReset_Idempotent
//  6. TestConflictBudget_Chaos_BusyThenEscalationBoundary_Reachable
package completion

import (
	"errors"
	"testing"
)

// driverBusyErr returns an error that simulates the shape of an
// SQLite SQLITE_BUSY. The package remains driver-independent
// (no mattn/go-sqlite3 import) so the test runs against any
// database/sql driver. The wording matches the canonical
// mattn/go-sqlite3 ErrBusy string so a future swap-in of the
// real driver would not break the contract.
//
// Reason is appended so a failure message ("got 'SQLite:
// database is locked: worker-A holds writer lock'") is a
// faithful trace of what was injected at each step.
func driverBusyErr(reason string) error {
	return errors.New("SQLite: database is locked: " + reason)
}

// ── 1. 3 consecutive busy errors absorbed unchanged ────────────────────

// TestConflictBudget_Chaos_BusyBackToBack_Absorbed exercises the
// minimum-fidelity scenario: 3 SQLite ErrBusy in a row, no other
// inputs interleaved. Record must:
//   - return the busy error verbatim on each call (no wrapping),
//   - leave consecutive counter at 0,
//   - never escalate regardless of threshold.
func TestConflictBudget_Chaos_BusyBackToBack_Absorbed(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	inputs := []struct {
		name string
		err  error
	}{
		{"busy-1 (writer lock held by worker-A)", driverBusyErr("writer lock held by worker-A")},
		{"busy-2 (writer lock contested)", driverBusyErr("writer lock contested")},
		{"busy-3 (checkpoint wedge)", driverBusyErr("checkpoint wedge")},
	}
	for i, s := range inputs {
		got := b.Record(testKey, s.err)
		if got == nil {
			t.Errorf("step %d (%s): Record(busy) returned nil; expected passthrough", i+1, s.name)
			continue
		}
		if got.Error() != s.err.Error() {
			t.Errorf("step %d (%s): Record returned %q; want unchanged %q",
				i+1, s.name, got.Error(), s.err.Error())
		}
		if c := b.Consecutive(); c != 0 {
			t.Errorf("step %d (%s): counter = %d; want 0 (busy must NOT count)", i+1, s.name, c)
		}
	}
	if c := b.Consecutive(); c != 0 {
		t.Errorf("after 3 busy errors, counter = %d; want 0", c)
	}
}

// ── 2. busy burst past threshold — never escalates ────────────────────

// TestConflictBudget_Chaos_BusyPastThreshold_NeverEscalates
// confirms the "busy never counts" property holds even when the
// burst length exceeds the threshold. Record returns every busy
// err unchanged and the streak stays at 0 — there is no path from
// busy to ErrConflictBudgetExhausted. Threshold = 3, burst = 7.
func TestConflictBudget_Chaos_BusyPastThreshold_NeverEscalates(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	const N = 7 // > threshold to confirm even past threshold never escalates
	for i := 0; i < N; i++ {
		err := driverBusyErr("over-threshold busy burst")
		got := b.Record(testKey, err)
		if got == nil {
			t.Errorf("iteration %d/%d: Record(busy) returned nil; expected passthrough", i+1, N)
			continue
		}
		if got.Error() != err.Error() {
			t.Errorf("iteration %d/%d: Record returned %q; want unchanged %q", i+1, N, got.Error(), err.Error())
		}
		if errors.Is(got, ErrConflictBudgetExhausted) {
			t.Errorf("iteration %d/%d: busy MUST NOT escalate; got %v", i+1, N, got)
		}
	}
	if c := b.Consecutive(); c != 0 {
		t.Errorf("after %d busy errors, counter = %d; want 0", N, c)
	}
}

// ── 3. busy via wrap-pattern (matches coordinator.recordAttemptCommitsCAS) ─

// TestConflictBudget_Chaos_BusyWrapPattern_Unchanged replays the
// 3x busy burst through the exact closure coordinator.go uses at
// every canonical attempt_commits CAS path
// (coordinator.recordAttemptCommitsCAS). The wrap must surface
// each busy err unchanged (chain returns busy unchanged, so the
// caller's retry layer sees the busy err and decides what to do
// — typically a short backoff, never an escalation).
func TestConflictBudget_Chaos_BusyWrapPattern_Unchanged(t *testing.T) {
	budget := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	wrap := func(err error) error {
		// Mirror coordinator.recordAttemptCommitsCAS verbatim:
		budgetErr := budget.Record(testKey, err)
		if budgetErr == nil {
			return err // under-threshold or reset — surface original err
		}
		return budgetErr // over-threshold — escalation sentinel
	}
	busyInputs := []error{
		driverBusyErr("writer lock held by worker-A"),
		driverBusyErr("checkpoint wedge"),
		driverBusyErr("writer lock contested"),
	}
	for i, busy := range busyInputs {
		got := wrap(busy)
		if got == nil {
			t.Errorf("iteration %d: wrap(busy) returned nil; expected passthrough", i+1)
			continue
		}
		if got.Error() != busy.Error() {
			t.Errorf("iteration %d: wrap(busy) returned %q; want unchanged %q",
				i+1, got.Error(), busy.Error())
		}
		if errors.Is(got, ErrConflictBudgetExhausted) {
			t.Errorf("iteration %d: wrap(busy) must NOT escalate; got %v", i+1, got)
		}
	}
	if c := budget.Consecutive(); c != 0 {
		t.Errorf("after 3 busy via wrap, counter = %d; want 0", c)
	}
}

// ── 4. busy burst + a real conflict starts a fresh streak at 1 ───────

// TestConflictBudget_Chaos_BusyThenConflict_FreshStreak confirms
// that busy does NOT insulate subsequent conflicts. A 3x busy
// burst followed by one ErrTransitionConflict: counter = 1
// (fresh streak), and Record returns nil (under threshold).
func TestConflictBudget_Chaos_BusyThenConflict_FreshStreak(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	// Burst of busy.
	for i := 0; i < 3; i++ {
		if got := b.Record(testKey, driverBusyErr("burst")); got == nil {
			t.Fatalf("burst step %d: Record(busy) returned nil; expected passthrough", i+1)
		}
	}
	if c := b.Consecutive(); c != 0 {
		t.Fatalf("post-busy: counter = %d; want 0", c)
	}
	// Now a real conflict: fresh streak at 1.
	cErr := conflictErr("stale fence")
	if got := b.Record(testKey, cErr); got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("post-busy conflict escalated unexpectedly: %v", got)
	}
	if c := b.Consecutive(); c != 1 {
		t.Errorf("post-busy conflict: counter = %d; want 1 (fresh streak)", c)
	}
}

// ── 5. busy then nil reset is idempotent ───────────────────────────────

// TestConflictBudget_Chaos_BusyThenNilReset_Idempotent confirms
// the reset path is safe after a busy burst. Counter is already
// 0 post-burst; the nil reset must NOT double-count and must keep
// the counter at 0 (no sink double-notification because the
// underlying Reset() short-circuits on `consecutive == 0`).
func TestConflictBudget_Chaos_BusyThenNilReset_Idempotent(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	for i := 0; i < 3; i++ {
		_ = b.Record(testKey, driverBusyErr("burst"))
	}
	if c := b.Consecutive(); c != 0 {
		t.Fatalf("pre-reset: counter = %d; want 0", c)
	}
	// nil reset (counter already 0 — should be no-op for the
	// sink's ResetConflictBudget call but keep bookkeeping tidy).
	if got := b.Record(testKey, nil); got != nil {
		t.Errorf("Record(nil) after busy burst returned %v; want nil", got)
	}
	if c := b.Consecutive(); c != 0 {
		t.Errorf("post-reset: counter = %d; want 0", c)
	}
	// Idempotency: a second Record(nil) must produce the same
	// counter=0 / nil-return / no-side-effects contract.
	if got := b.Record(testKey, nil); got != nil {
		t.Errorf("Record(nil) idempotent step returned %v; want nil", got)
	}
	if c := b.Consecutive(); c != 0 {
		t.Errorf("post-second-nil: counter = %d; want 0", c)
	}
}

// ── 6. busy then escalation boundary is reachable ───────────────────────

// TestConflictBudget_Chaos_BusyThenEscalationBoundary_Reachable
// confirms busy is not a "shield" that prevents escalation. With
// threshold=3: a 3x busy burst, then 3 ErrTransitionConflict, the
// 3rd conflict IS the boundary → ErrConflictBudgetExhausted is
// emitted. busy did not block the boundary.
func TestConflictBudget_Chaos_BusyThenEscalationBoundary_Reachable(t *testing.T) {
	b := NewConflictBudget(ConflictBudgetPolicy{ConsecutiveConflictThreshold: 3})
	// Burst of busy.
	for i := 0; i < 3; i++ {
		if got := b.Record(testKey, driverBusyErr("pre-boundary burst")); got == nil {
			t.Fatalf("busy step %d: Record(busy) returned nil; expected passthrough", i+1)
		}
	}
	if c := b.Consecutive(); c != 0 {
		t.Fatalf("post-busy: counter = %d; want 0", c)
	}
	cErr := conflictErr("stale fence")
	// 2 conflicts: under threshold.
	for step := 1; step <= 2; step++ {
		if got := b.Record(testKey, cErr); got != nil && errors.Is(got, ErrConflictBudgetExhausted) {
			t.Errorf("conflict #%d: unexpectedly escalated: %v", step, got)
		}
		if c := b.Consecutive(); c != step {
			t.Errorf("post-conflict #%d: counter = %d; want %d", step, c, step)
		}
	}
	// Boundary: 3rd conflict escalates.
	got := b.Record(testKey, cErr)
	if got == nil {
		t.Fatal("boundary 3rd conflict: Record returned nil; expected ErrConflictBudgetExhausted")
	}
	if !errors.Is(got, ErrConflictBudgetExhausted) {
		t.Errorf("boundary: Record returned %v; expected errors.Is(_, ErrConflictBudgetExhausted)", got)
	}
	// Blocco 3: the key is eagerly removed on escalation, so
	// Consecutive() returns 0 (no active streaks) after the
	// boundary. The escalation error is the real signal.
	if c := b.Consecutive(); c != 0 {
		t.Errorf("final counter = %d; want 0 (eager-delete on escalation, no active streaks)", c)
	}
	if c := b.consecutiveForKey(testKey); c != 0 {
		t.Errorf("final consecutiveForKey(testKey) = %d; want 0 (key eagerly removed)", c)
	}
}
