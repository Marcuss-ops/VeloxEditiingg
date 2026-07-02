package retry

import "time"

// BackoffPolicy computes a backoff delay for a given attempt number.
// Attempt is 1-based (first attempt = 1). Implementations must be
// safe for concurrent use.
type BackoffPolicy interface {
	// Delay returns the duration to wait before the next attempt.
	// Attempt is 1-based. Returns 0 when no delay is needed.
	Delay(attempt int) time.Duration
}

// ScheduleBackoff implements BackoffPolicy using a fixed schedule of
// durations. Each entry maps an attempt number (1-based) to the delay
// before the next attempt. The last entry is used for all subsequent
// attempts. When the schedule is empty, Fallback is used (default 30s).
//
// This is the canonical backoff strategy for the forwarding and delivery
// runners, which use discrete retry windows rather than exponential growth.
type ScheduleBackoff struct {
	// Schedule is the ordered list of backoff durations. Index 0 is the
	// delay before attempt 2, index 1 before attempt 3, etc.
	Schedule []time.Duration
	// Fallback is used when Schedule is empty. Defaults to 30s.
	Fallback time.Duration
}

// Delay returns the backoff delay for the given 1-based attempt.
func (s ScheduleBackoff) Delay(attempt int) time.Duration {
	if len(s.Schedule) == 0 {
		if s.Fallback > 0 {
			return s.Fallback
		}
		return 30 * time.Second
	}
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s.Schedule) {
		idx = len(s.Schedule) - 1
	}
	return s.Schedule[idx]
}
