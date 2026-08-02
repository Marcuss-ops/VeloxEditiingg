package deliveries

// runner_config.go: RunnerConfig + backoff schedule helper for the
// DeliveryRunner. Split out of runner.go; the runner lifecycle lives
// in runner.go and per-lease processing in runner_process.go.

import "time"

// RunnerConfig tunes the runner.
type RunnerConfig struct {
	// PollInterval is how often the runner scans for pending deliveries.
	PollInterval time.Duration
	// LeaseDuration is how long a claim is held before another runner
	// can re-claim it. Should be > the worst-case provider latency.
	LeaseDuration time.Duration
	// MaxAttempts per delivery before declaring FAILED.
	MaxAttempts int
	// ClaimBatch limits how many deliveries the runner can claim in a
	// single tick. Should be ≥ Concurrency to keep the pool saturated.
	ClaimBatch int
	// Concurrency limits how many deliveries are processed concurrently.
	// Each delivery gets its own lease renewal goroutine; a bounded pool
	// prevents resource exhaustion. Default 2.
	Concurrency int

	// BackoffSchedule maps attempt number (1-based) to the delay before
	// the next attempt. The last entry is used for all subsequent attempts.
	// Defaults to the canonical schedule: 30s, 2m, 10m, 30m.
	BackoffSchedule []time.Duration
}

// DefaultRunnerConfig returns sensible defaults.
func DefaultRunnerConfig() *RunnerConfig {
	return &RunnerConfig{
		PollInterval:  5 * time.Second,
		LeaseDuration: 5 * time.Minute,
		MaxAttempts:   5,
		ClaimBatch:    4,
		Concurrency:   2,
		BackoffSchedule: []time.Duration{
			30 * time.Second,
			2 * time.Minute,
			10 * time.Minute,
			30 * time.Minute,
		},
	}
}

// backoffForAttempt returns the backoff delay for the given 1-based attempt
// number using the configured schedule. If the attempt exceeds the schedule
// length, the last entry is used.
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
