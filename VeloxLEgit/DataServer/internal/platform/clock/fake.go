package clock

import "time"

// Fake is a manual time source for tests. Advance(d) lets tests simulate
// lease expiry without sleeping. Set(t) replaces the clock's time.
type Fake struct {
	Time time.Time
}

// NewFake returns a Fake clock seeded at start. If start is zero, defaults to now.
func NewFake(start time.Time) *Fake {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	return &Fake{Time: start.UTC()}
}

// Now returns the fake clock's current time, never zero.
func (f *Fake) Now() time.Time {
	if f.Time.IsZero() {
		f.Time = time.Now().UTC()
	}
	return f.Time
}

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.Time = f.Time.Add(d)
}

// Set replaces the fake clock's time.
func (f *Fake) Set(t time.Time) {
	f.Time = t.UTC()
}

// Compile-time guard.
var _ Clock = (*Fake)(nil)
