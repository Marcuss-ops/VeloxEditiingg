// Package queue / clock.go — PR 3 clock injection.
//
// LifecycleService consumes a Clock instead of calling time.Now() directly so
// that lease-renewal tests, reaper deadlines, and SUCCEEDED gating can be
// driven with a deterministic time source.
//
// Types are aliased from platform/clock (canonical server clock package).
package queue

import (
	"time"

	"velox-server/internal/platform/clock"
)

// Clock is an alias for the canonical server clock interface.
type Clock = clock.Clock

// RealClock is an alias for the production system clock.
type RealClock = clock.System

// MockClock is an alias for the canonical fake clock from platform/clock.
// Kept for backward-compatible naming in queue tests.
type MockClock = clock.Fake

// NewMockClock returns a MockClock seeded at `start`.
func NewMockClock(start time.Time) *MockClock {
	return clock.NewFake(start)
}

