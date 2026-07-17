// Package retry provides a generic retry policy for both server-side and
// worker-side operations. The Policy struct is shared; Classifier functions
// are domain-specific (see social upload and worker HTTP client).
//
// Architecture guide §10: one Policy definition, per-domain Classifier.
// Do not couple server and worker retry logic into a single package.
package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Compile-time check: Policy satisfies BackoffPolicy.
var _ BackoffPolicy = Policy{}

// Policy defines the retry budget for an operation.
type Policy struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	MaxAttempts int
	// BaseDelay is the initial backoff duration.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff.
	MaxDelay time.Duration
}

// DefaultPolicy returns a reasonable retry policy for most operations.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 3,
		BaseDelay:   2 * time.Second,
		MaxDelay:    5 * time.Minute,
	}
}

// Classifier is the type for per-domain retry classification.
// Returns true when the error is transient and the operation should be retried.
type Classifier func(error) bool

// Backoff computes the exponential backoff duration for the given attempt
// (0-indexed). The result is capped at p.MaxDelay and includes jitter.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	backoff := float64(p.BaseDelay) * math.Pow(2, float64(attempt))
	if p.MaxDelay > 0 && backoff > float64(p.MaxDelay) {
		backoff = float64(p.MaxDelay)
	}
	// Add 10-50% jitter to avoid thundering-herd
	jitter := time.Duration(rand.Float64() * backoff * 0.5)
	return time.Duration(backoff) + jitter
}

// Delay implements BackoffPolicy. Attempt is 1-based (first call = 1);
// internally delegated to Backoff which expects a 0-indexed attempt.
func (p Policy) Delay(attempt int) time.Duration {
	return p.Backoff(attempt - 1)
}

// SleepWithContext sleeps for the given duration or until ctx is cancelled.
func SleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
