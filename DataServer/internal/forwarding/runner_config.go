// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import "time"

// ── Config ───────────────────────────────────────────────────────────────

// RunnerConfig tunes the CreatorForwardingRunner.
type RunnerConfig struct {
	// PollInterval is how often the runner scans for claimable forwardings.
	PollInterval time.Duration
	// LeaseDuration is how long a claim is held before another runner can
	// re-claim it. Should be > the worst-case remote poll latency.
	LeaseDuration time.Duration
	// MaxAttempts per forwarding before declaring FAILED. 0 means default (12).
	MaxAttempts int
	// ClaimBatch limits how many forwardings the runner claims per tick.
	ClaimBatch int
	// Concurrency limits how many forwardings are processed concurrently.
	Concurrency int
	// BackoffSchedule maps attempt number (1-based) to the delay before the
	// next attempt. The last entry is used for all subsequent attempts.
	// Only used for transient errors (poll failures); non-terminal "still
	// running" statuses release the claim immediately (no backoff).
	BackoffSchedule []time.Duration
}

// DefaultRunnerConfig returns sensible defaults matching the audit
// recommended values.
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PollInterval:  5 * time.Second,
		LeaseDuration: 5 * time.Minute,
		ClaimBatch:    20,
		MaxAttempts:   12,
		Concurrency:   4,
		BackoffSchedule: []time.Duration{
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
			30 * time.Minute,
		},
	}
}

// backoffForAttempt returns the backoff delay for the given 1-based
// attempt number using the configured schedule.
func (cfg *RunnerConfig) backoffForAttempt(attempt int) time.Duration {
	if len(cfg.BackoffSchedule) == 0 {
		return 30 * time.Second
	}
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cfg.BackoffSchedule) {
		idx = len(cfg.BackoffSchedule) - 1
	}
	return cfg.BackoffSchedule[idx]
}
