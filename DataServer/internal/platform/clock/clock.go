// Package clock provides a neutral time abstraction for all server packages.
// Consumers import this package instead of defining their own Clock interface.
package clock

import "time"

// Clock is the canonical time source for all server-side components.
// Every package that needs testable time must use this interface.
type Clock interface {
	Now() time.Time
}

// System returns the wall-clock UTC time. Used in production wiring.
type System struct{}

// Now returns the current wall clock time in UTC.
func (System) Now() time.Time { return time.Now().UTC() }

// Compile-time guard.
var _ Clock = System{}
