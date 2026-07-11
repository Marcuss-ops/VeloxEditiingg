// Package supervisor / clock_test.go
//
// Sanity tests for the Clock abstraction + a MockClock helper
// shared between the supervisor package and the completion
// package's conflict_budget_test.go.
//
// RealClock.Now just wraps time.Now; RealClock.NewTicker just wraps
// time.NewTicker. The tests below exercise both surfaces via the
// canonical "fire / wait" idiom and a deterministic
// before <= Now() <= after window check.
//
// The MockClock helpers (NewMockClock + Advance + Now) are the
// "clock injection" payload downstream consumers (FailureTracker
// tests, ConflictBudget tests) use to drive ResetWindow timing
// without sleeping through real time.
package supervisor

import (
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

// MockClock is a deterministic wall-clock for tests. The Now()
// closure captures the pointer, so callers can advance time via
// Advance(d) and every consumer reading clock.Now() sees the
// latest value.
type MockClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewMockClock constructs a MockClock at start. Useful for
// anchoring the chain at a specific wall-clock instant.
func NewMockClock(start time.Time) *MockClock {
	return &MockClock{now: start}
}

// Now returns the current virtual time.
func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Advance advances the virtual time by d.
func (m *MockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

// NewTicker returns a real time.NewTicker. Tests of streak-refresh
// / ResetWindow logic drive Now() directly via the MockClock;
// ticker-driven Run-loop tests still use the real ticker for
// goroutine scheduling.
func (m *MockClock) NewTicker(d time.Duration) Ticker {
	t := time.NewTicker(d)
	return realTicker{t: t}
}

func TestRealClock_NowReturnsRecentTime(t *testing.T) {
	c := RealClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Errorf("RealClock.Now() = %v, want between %v and %v", got, before, after)
	}
}

func TestRealClock_NewTickerFires(t *testing.T) {
	c := RealClock{}
	tk := c.NewTicker(5 * time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.C():
		// fired as expected
	case <-time.After(2 * time.Second):
		t.Fatal("RealClock.NewTicker(5ms).C did not fire within 2s")
	}
}

func TestMockClock_AdvancesOnDemand(t *testing.T) {
	clk := NewMockClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	if !clk.Now().Equal(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("MockClock.Now() at start = %v", clk.Now())
	}
	clk.Advance(1 * time.Hour)
	if !clk.Now().Equal(time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("MockClock.Now() after 1h = %v", clk.Now())
	}
	clk.Advance(30 * time.Minute)
	if !clk.Now().Equal(time.Date(2026, 1, 1, 13, 30, 0, 0, time.UTC)) {
		t.Fatalf("MockClock.Now() after +30m = %v", clk.Now())
	}
}

// TestNewFailureTrackerWithClock_NilClockLifecycle pins down the
// nil-clock fallback contract documented in clock.go and at the
// NewFailureTrackerWithClock constructor: passing nil MUST
// silently degrade to RealClock and the resulting tracker MUST
// complete the full Record/Reset/ResetWindow/escalation
// lifecycle exactly like a tracker that was given a wall-clock
// RealClock explicitly. The shallow tests the previous version of
// this file did (only `nowFn != nil`, only `nowFn() != zero`)
// left room for a regression where the fallback resolved but
// the state machine was broken under nil. This test closes that
// gap by driving the cross-threshold boundary with the same
// DefaultRetryPolicy the production runners use.
func TestNewFailureTrackerWithClock_NilClockLifecycle(t *testing.T) {
	// Phase 1 — Build: nil clk resolves to a working nowFn
	// closure backed by RealClock. We intentionally do NOT
	// assert tk.nowFn == RealClock{}.Now by closure identity
	// (avoid brittle pointer comparisons); instead we assert
	// non-nil AND a time bounded between two adjacent
	// time.Now() snapshots, mirroring the
	// TestRealClock_NowReturnsRecentTime pattern. The
	// bounded-window check catches the "stub clock returning
	// some sentinel future time" regression that an
	// `IsZero()`-only assertion would silently miss.
	tk := NewFailureTrackerWithClock(DefaultRetryPolicy(), nil)
	if tk.nowFn == nil {
		t.Fatal("NewFailureTrackerWithClock(nil clk) must fall back to a working nowFn; got nil")
	}
	before := time.Now()
	got := tk.nowFn()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Errorf("fallback nowFn returned %v, want between %v and %v", got, before, after)
	}
	if got.IsZero() {
		t.Error("fallback nowFn returned zero time; RealClock.Now should return wall-clock")
	}

	// Phase 2 — Pre-threshold: 4 consecutive ErrInfrastructure
	// records must increment the counter and return nil
	// (no escalation yet — threshold is 5). This proves the
	// fallback clock drives Record's increment-and-compare
	// branch, not a broken stub.
	for i := 0; i < 4; i++ {
		if err := tk.Record(ErrInfrastructure); err != nil {
			t.Errorf("Record #%d (sub-threshold) must not escalate on nil clock fallback; got %v", i+1, err)
		}
	}
	if got := tk.Consecutive(); got != 4 {
		t.Errorf("Consecutive() after 4 infra errors = %d, want 4", got)
	}

	// Phase 3 — Threshold boundary: the 5th consecutive
	// ErrInfrastructure must escalate with the wrapped sentinel.
	// We assert errors.Is to reach into the wrapping contract,
	// matching how coordinator.go and the runners introspect the
	// returned error.
	err := tk.Record(ErrInfrastructure)
	if err == nil {
		t.Fatal("5th consecutive Record must escalate with ErrInfrastructure; got nil")
	}
	if !errors.Is(err, ErrInfrastructure) {
		t.Errorf("escalation chain missing ErrInfrastructure sentinel on nil-clock fallback; errors.Is(err, ErrInfrastructure) = false; err = %v", err)
	}

	// Phase 4 — Recovery: a successful tick via Record(nil) must
	// cleanly clear the counter and return nil. RealClock-backed
	// fallback must support the reset path identically to the
	// happy path.
	if err := tk.Record(nil); err != nil {
		t.Errorf("Record(nil) after escalation on nil-clock fallback must reset (no error); got %v", err)
	}
	if got := tk.Consecutive(); got != 0 {
		t.Errorf("Consecutive() after Record(nil) = %d, want 0", got)
	}

	// Phase 5 — Element-scoped: ErrElementScoped must NOT
	// increment the counter under the nil-clock fallback.
	// Defends against a regression where the fallback stripped
	// the sentinel-classification logic. Two iterations is
	// enough — every Record is asserted, and the check is
	// monotonic, so iteration N silently passing implies 1..N
	// also passed.
	for i := 0; i < 2; i++ {
		if err := tk.Record(ErrElementScoped); err != nil {
			t.Errorf("Record(ErrElementScoped) #%d must not escalate; got %v", i+1, err)
		}
	}
	if got := tk.Consecutive(); got != 0 {
		t.Errorf("Consecutive() after 2 element-scoped errors = %d, want 0", got)
	}
}

func TestNewFailureTrackerWithClock_AppliesInjectedNowFn(t *testing.T) {
	clk := NewMockClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	tk := NewFailureTrackerWithClock(RetryPolicy{
		ConsecutiveErrorThreshold: 5,
		ResetWindow:               1 * time.Second,
	}, clk)
	// Drive 4 errors within the window — none should escalate
	// (threshold=5 means the 5th is the boundary). Tests do not
	// sleep; the MockClock drives ResetWindow deterministically.
	for i := 0; i < 4; i++ {
		if err := tk.Record(sql.ErrConnDone); err != nil {
			t.Errorf("Record #%d should not escalate: %v", i+1, err)
		}
	}
	if got := tk.Consecutive(); got != 4 {
		t.Errorf("Consecutive() = %d, want 4", got)
	}
	// Jump past the reset window via mock advance — streak should
	// refresh without escalating.
	clk.Advance(2 * time.Second)
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("post-window Record should restart streak (no escalate): %v", err)
	}
	if got := tk.Consecutive(); got != 1 {
		t.Errorf("after window refresh, Consecutive() = %d, want 1", got)
	}
}
