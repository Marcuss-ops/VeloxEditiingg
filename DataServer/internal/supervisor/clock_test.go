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

func TestNewFailureTrackerWithClock_NilFallsBackToRealClock(t *testing.T) {
	tk := NewFailureTrackerWithClock(DefaultRetryPolicy(), nil)
	if tk.nowFn == nil {
		t.Fatal("NewFailureTrackerWithClock(nil clk) should fall back to RealClock; nowFn is nil")
	}
	got := tk.nowFn()
	if got.IsZero() {
		t.Error("fallback nowFn returned zero time")
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
