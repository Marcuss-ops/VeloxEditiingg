package remoteengine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// DefaultMalformedRetryLimit is the maximum number of retry attempts for
// MALFORMED_RESPONSE errors before the error is promoted to PERMANENT.
const DefaultMalformedRetryLimit = 2

// ErrMalformedRetryExceeded is the sentinel wrapped into the Cause chain
// when a MALFORMED_RESPONSE error is promoted to PERMANENT after
// exceeding MaxMalformedAttempts.
var ErrMalformedRetryExceeded = errors.New("remote engine: malformed response retry limit exceeded")

// RetryPolicy encapsulates the full retry decision logic for the remote
// engine client.
//
// MaxAttempts is the total number of execution attempts (initial + retries).
// MaxMalformedAttempts is the total number of malformed-response attempts
// before the error is promoted to PERMANENT.
type RetryPolicy struct {
	MaxAttempts          int
	MaxMalformedAttempts int
}

// DefaultRetryPolicy returns the standard policy derived from the given
// retry count. A Retries value of N means 1 initial attempt plus up to N
// retries, i.e. MaxAttempts = N + 1. If maxRetries is zero or negative,
// it defaults to 3 retries (4 total attempts).
func DefaultRetryPolicy(maxRetries int) RetryPolicy {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	maxAttempts := maxRetries + 1
	mr := DefaultMalformedRetryLimit
	if mr > maxAttempts {
		mr = maxAttempts
	}
	return RetryPolicy{
		MaxAttempts:          maxAttempts,
		MaxMalformedAttempts: mr,
	}
}

// ShouldStop returns true if the retry loop should break immediately.
func (p RetryPolicy) ShouldStop(err error, malformedAttempts int) (error, bool) {
	if err == nil {
		return nil, false
	}

	var re *RemoteError
	if !errors.As(err, &re) {
		// Untyped error: permanent by default; do not retry.
		return err, true
	}

	if re.IsPermanent() {
		return err, true
	}

	// MALFORMED_RESPONSE: limited retry, then permanent.
	if re.Class == RemoteErrorMalformed {
		if malformedAttempts >= p.MaxMalformedAttempts {
			promoted := &RemoteError{
				Class:      RemoteErrorPermanent,
				StatusCode: re.StatusCode,
				Code:       re.Code + "_RETRY_EXCEEDED",
				Message:    fmt.Sprintf("malformed response after %d attempts: %s", malformedAttempts, re.Message),
				Body:       re.Body,
				Cause:      fmt.Errorf("%w: %w", ErrMalformedRetryExceeded, re.Cause),
			}
			return promoted, true
		}
	}

	// RATE_LIMIT, TRANSIENT: keep retrying.
	return err, false
}

// withRetry executes fn up to MaxAttempts times.
//
//   - VALIDATION, AUTHENTICATION, PERMANENT → break immediately (no retry).
//   - RATE_LIMIT, TRANSIENT → retry with backoff up to MaxAttempts.
//   - MALFORMED_RESPONSE → retry up to MaxMalformedAttempts, then promote
//     to PERMANENT (limited retry, then permanent).
//
// The backoff schedule follows RetrySchedule (1s, 5s, 15s, 30s, 60s, 5m)
// with ±20% jitter. For RATE_LIMIT errors, RetryAfter is honoured if
// the remote service provided it.
func (c *Client) withRetry(ctx context.Context, fn func(attempt int) error) error {
	policy := DefaultRetryPolicy(c.config.Retries)
	var lastErr error
	malformedAttempts := 0

	if err := ctx.Err(); err != nil {
		return err
	}

	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		lastErr = fn(attempt)
		if lastErr == nil {
			return nil
		}

		// Track malformed-specific attempts.
		var re *RemoteError
		if errors.As(lastErr, &re) && re.Class == RemoteErrorMalformed {
			malformedAttempts++
		}

		// context.Canceled and context.DeadlineExceeded must stop immediately.
		if err := ctx.Err(); err != nil {
			return err
		}

		// Ask the policy whether to stop.
		var stop bool
		lastErr, stop = policy.ShouldStop(lastErr, malformedAttempts)
		if stop {
			if errors.As(lastErr, &re) {
				log.Printf("Remote engine stopping (attempt %d/%d): %s", attempt+1, policy.MaxAttempts, re)
			} else {
				log.Printf("Remote engine stopping (attempt %d/%d): %v", attempt+1, policy.MaxAttempts, lastErr)
			}
			return lastErr
		}

		// Log the retryable error.
		if errors.As(lastErr, &re) {
			log.Printf("Remote engine retryable error (attempt %d/%d, malformed %d/%d): %s",
				attempt+1, policy.MaxAttempts, malformedAttempts, policy.MaxMalformedAttempts, re)
		} else {
			log.Printf("Remote engine error (attempt %d/%d): %v", attempt+1, policy.MaxAttempts, lastErr)
		}

		// Compute backoff.
		backoff := RetrySchedule(attempt)
		if re != nil && re.RetryAfter > 0 {
			backoff = re.RetryAfter
		}
		backoff = AddJitter(backoff, int64(attempt)+time.Now().UnixNano())

		if attempt < policy.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return lastErr
}

// RetrySchedule returns the backoff duration for the given attempt index
// (0-based). The schedule follows the Area 2 specification:
//
//	attempt 0 → 1s
//	attempt 1 → 5s
//	attempt 2 → 15s
//	attempt 3 → 30s
//	attempt 4 → 60s
//	attempt 5+ → 5m
func RetrySchedule(attempt int) time.Duration {
	schedule := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(schedule) {
		return 5 * time.Minute
	}
	return schedule[attempt]
}

// AddJitter adds ±20% jitter to a duration to prevent thundering-herd
// polling when multiple runners interrogate the remote service.
func AddJitter(d time.Duration, seed int64) time.Duration {
	if d <= 0 {
		return d
	}
	// Deterministic jitter based on seed so tests are reproducible.
	// Range: 80% .. 120% of d.
	r := simpleRand(seed)
	factor := 0.8 + 0.4*r // 0.8 .. 1.2
	return time.Duration(float64(d) * factor)
}

// simpleRand returns a deterministic pseudo-random float in [0, 1).
// Uses a simple LCG — sufficient for jitter, not for crypto.
func simpleRand(seed int64) float64 {
	seed = seed*6364136223846793005 + 1442695040888963407
	// Use the upper 32 bits for the fraction.
	u := uint32(seed >> 32)
	return float64(u) / float64(1<<32)
}
