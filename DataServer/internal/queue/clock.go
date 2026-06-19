// Package queue / clock.go — PR 3 clock injection.
//
// LifecycleService consumes a Clock instead of calling time.Now() directly so
// that lease-renewal tests, reaper deadlines, and SUCCEEDED gating can be
// driven with a deterministic time source. The interface is deliberately tiny
// so the production wiring (RealClock) is a zero-cost shim.
package queue

import "time"

// Clock is the time source for lifecycle operations.
//
// Implementations must return UTC times so event/history timestamps stay
// comparable across the DB (which is mapped to RFC3339 UTC strings). Tests
// use MockClock to advance time without sleeping.
type Clock interface {
	Now() time.Time
}

// RealClock returns the wall-clock UTC time. Used in production.
type RealClock struct{}

// Now returns the current wall clock time in UTC.
func (RealClock) Now() time.Time { return time.Now().UTC() }

// MockClock is a manual time source for tests. Advance(d) lets tests simulate
// lease expiry without sleeping.
type MockClock struct {
	T time.Time
}

// NewMockClock returns a MockClock seeded at `start`.
func NewMockClock(start time.Time) *MockClock {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	return &MockClock{T: start.UTC()}
}

// Now returns the mock clock's current time, never zero.
func (m *MockClock) Now() time.Time {
	if m.T.IsZero() {
		m.T = time.Now().UTC()
	}
	return m.T
}

// Advance moves the mock clock forward by d.
func (m *MockClock) Advance(d time.Duration) {
	m.T = m.T.Add(d)
}

// Set replaces the mock clock's time.
func (m *MockClock) Set(t time.Time) {
	m.T = t.UTC()
}

// Compile-time guards.
var (
	_ Clock = RealClock{}
	_ Clock = (*MockClock)(nil)
)
