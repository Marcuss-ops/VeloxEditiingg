// Package logger provides structured event logging for the Velox Worker Agent.
package logger

import (
	"sync"
)

// RateLimiter implements rate limiting for log events.
type RateLimiter struct {
	mu       sync.Mutex
	counters map[string]int
}

var globalRateLimiter = &RateLimiter{
	counters: make(map[string]int),
}

// GlobalRateLimiter returns the global rate limiter.
func GlobalRateLimiter() *RateLimiter {
	return globalRateLimiter
}

// Reset resets all counters.
func (r *RateLimiter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters = make(map[string]int)
}

// ShouldLog returns true if the event should be logged based on rate limiting rules.
// Milestones: 1, 5, 10, 50, 100, 500, 1000, then every 1000.
func (r *RateLimiter) ShouldLog(eventKey string) (bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counters[eventKey]++
	count := r.counters[eventKey]

	switch {
	case count == 1:
		return true, count
	case count == 5:
		return true, count
	case count == 10:
		return true, count
	case count == 50:
		return true, count
	case count == 100:
		return true, count
	case count == 500:
		return true, count
	case count >= 1000 && count%1000 == 0:
		return true, count
	default:
		return false, count
	}
}
