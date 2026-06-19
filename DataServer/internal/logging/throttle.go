package logging

import (
	"sync"
	"time"
)

// Throttler provides time-based deduplication for log messages
// Per 11_LOGGING_OPERATIVO_SENZA_RUMORE.md: deduplica warning ripetitivi
type Throttler struct {
	mu       sync.Mutex
	seen     map[string]time.Time
	interval time.Duration
	maxSize  int
}

// NewThrottler creates a new throttler with the given dedup interval
func NewThrottler(interval time.Duration) *Throttler {
	return &Throttler{
		seen:     make(map[string]time.Time),
		interval: interval,
		maxSize:  10000, // Prevent memory leaks
	}
}

// Allow returns true if the key hasn't been seen recently
// Returns false if the key was logged within the throttle interval
func (t *Throttler) Allow(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()

	// Check if we've seen this key recently
	if lastSeen, exists := t.seen[key]; exists {
		if now.Sub(lastSeen) < t.interval {
			return false // Throttled
		}
	}

	// Clean up old entries if map is too large
	if len(t.seen) >= t.maxSize {
		t.cleanupLocked(now)
	}

	// Mark as seen
	t.seen[key] = now
	return true
}

// cleanupLocked removes stale entries (must hold lock)
func (t *Throttler) cleanupLocked(now time.Time) {
	cutoff := now.Add(-t.interval * 2)
	for k, v := range t.seen {
		if v.Before(cutoff) {
			delete(t.seen, k)
		}
	}
}

// Reset clears all throttling state
func (t *Throttler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seen = make(map[string]time.Time)
}

// Stats returns throttling statistics
func (t *Throttler) Stats() map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	return map[string]interface{}{
		"tracked_keys": len(t.seen),
		"interval":     t.interval.String(),
		"max_size":     t.maxSize,
	}
}

// SetInterval changes the throttle interval
func (t *Throttler) SetInterval(interval time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.interval = interval
}

// CountThrottled counts how many keys are currently in the throttle window
func (t *Throttler) CountThrottled() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	count := 0
	for _, v := range t.seen {
		if now.Sub(v) < t.interval {
			count++
		}
	}
	return count
}
