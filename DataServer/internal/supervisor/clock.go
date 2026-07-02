// Package supervisor / clock.go
//
// Wall-clock abstraction for the FailureTracker (and any future
// time-dependent helpers in this package).
//
// Verdetto P2 (Blocco 5): the FailureTracker's ResetWindow logic and
// the Run loops' ticker scheduling depend on time.Now / time.NewTicker.
// Tests of the consecutive-error / reset-window / reconciliation
// timing need a mock clock to advance the wall-clock deterministically
// without sleeping through real ticker waits.
//
// Clock is the canonical abstraction. RealClock wraps time.Now and
// time.NewTicker with the standard-library defaults. Tests can pass
// any Clock impl (e.g. a `mockClock` in policy_test.go that maintains
// a virtual Now and a manual advance knob) into
// NewFailureTrackerWithClock. The existing NewFailureTracker is a
// thin wrapper around NewFailureTrackerWithClock(RealClock{}) so all
// existing call sites continue to behave the same.
package supervisor

import "time"

// Clock is the minimal wall-clock + ticker surface the supervisor
// package depends on. Both methods mirror time.Now + time.NewTicker
// semantics — RealClock is a one-line wrapper; tests can pass any
// Clock that maintains a virtual Now and dispatches channel ticks
// on demand.
type Clock interface {
	// Now returns the current instant in time.
	Now() time.Time
	// NewTicker returns a Ticker that fires every d. Mirrors
	// time.NewTicker semantically.
	NewTicker(d time.Duration) Ticker
}

// Ticker is the supervisor-scoped ticker surface. Mirrors the
// subset of *time.Ticker methods the supervisor uses.
type Ticker interface {
	// C returns the channel on which ticks are delivered.
	C() <-chan time.Time
	// Stop releases the underlying resources.
	Stop()
}

// RealClock is the production Clock. Backed by time.Now + time.NewTicker.
type RealClock struct{}

// Now returns time.Now.
func (RealClock) Now() time.Time { return time.Now() }

// NewTicker returns a *time.Ticker wrapped in realTicker.
func (RealClock) NewTicker(d time.Duration) Ticker {
	t := time.NewTicker(d)
	return realTicker{t: t}
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
