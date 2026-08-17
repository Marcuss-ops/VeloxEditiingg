package downloader

// retry.go — error classification and backoff scheduling for asset
// transfers. The transfer-level retry lifecycle (RETRY_WAIT loops, Range
// resume, .part reuse) is hardened in the resilience commit; what lives here
// today is the classification vocabulary both the worker transferer and the
// future retry loop share, so the wire-facing semantics (what is retryable,
// what is permanent, how long to wait) are defined in exactly one place.

import (
	"errors"
	"math/rand"
	"net/http"
	"time"
)

// DefaultMaxAttempts and DefaultBaseBackoff match the pre-manager worker
// download loop (4 attempts, exponential backoff starting at ~1s).
const (
	DefaultMaxAttempts = 4
	DefaultBaseBackoff = 1 * time.Second
)

// Classification errors the transferer can wrap to tell the manager whether a
// failed transfer should be retried by the transfer lifecycle (resilience
// commit) or surfaced as terminal.
var (
	// ErrRetryable marks a transient failure (timeout, reset, 401/429, 5xx).
	ErrRetryable = errors.New("downloader: retryable transfer failure")
	// ErrPermanent marks a failure that must never be retried (403/404,
	// verified hash mismatch, permanent size mismatch, missing metadata).
	ErrPermanent = errors.New("downloader: permanent transfer failure")
	// ErrVerify marks a verification failure (hash or size mismatch).
	ErrVerify = errors.New("downloader: asset verification failed")
)

// IsRetryableStatus reports whether an upstream HTTP status is safe to retry.
// Retryable: 401 (the worker's session token is re-issued on reconnect, so an
// auth failure during a master restart heals), 408, 429 (honour Retry-After),
// 500-599. Everything else is not.
func IsRetryableStatus(code int) bool {
	switch {
	case code == http.StatusUnauthorized:
		return true
	case code == http.StatusRequestTimeout:
		return true
	case code == http.StatusTooManyRequests:
		return true
	case code >= 500 && code < 600:
		return true
	default:
		return false
	}
}

// IsPermanentStatus reports whether an upstream HTTP status must never be
// retried: any 4xx that is not 401/408/429 (forbidden, not-found, and other
// client errors will not heal on retry). 401 is excluded because the worker's
// session token is re-issued on reconnect, so an auth failure during a master
// restart heals.
func IsPermanentStatus(code int) bool {
	if code < 400 || code >= 500 {
		return false
	}
	return !IsRetryableStatus(code)
}

// ClassifyHTTPStatus maps an upstream status onto ErrRetryable or ErrPermanent
// (nil for 2xx/3xx, which the caller should not classify as a failure).
func ClassifyHTTPStatus(code int) error {
	if code >= 200 && code < 400 {
		return nil
	}
	if IsRetryableStatus(code) {
		return ErrRetryable
	}
	return ErrPermanent
}

// BackoffSchedule returns the per-attempt sleep durations for maxAttempts
// attempts: base * 2^n (1s, 2s, 4s, 8s for the default base), scaled by
// (1 + jitter()) when jitter is non-nil. jitter must return a value in
// [0, 1); pass nil for a deterministic schedule in tests.
func BackoffSchedule(maxAttempts int, base time.Duration, jitter func() float64) []time.Duration {
	if maxAttempts <= 1 {
		return nil
	}
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	out := make([]time.Duration, 0, maxAttempts-1)
	for attempt := 1; attempt < maxAttempts; attempt++ {
		d := base * time.Duration(1<<uint(attempt-1))
		if jitter != nil {
			d += time.Duration(jitter() * float64(d))
		}
		out = append(out, d)
	}
	return out
}

// RetryAfter extracts the Retry-After header value as seconds. Returns 0 when
// absent or unparseable (caller falls back to the backoff schedule).
func RetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	raw := resp.Header.Get("Retry-After")
	if raw == "" {
		return 0
	}
	if secs, err := time.ParseDuration(raw + "s"); err == nil && secs > 0 {
		return secs
	}
	if secs, err := time.ParseDuration(raw); err == nil && secs > 0 {
		return secs
	}
	if when, err := http.ParseTime(raw); err == nil {
		if wait := time.Until(when); wait > 0 {
			return wait
		}
	}
	return 0
}

// DefaultJitter returns a uniform random value in [0, 0.25). Used by the
// worker transferer when no jitter source is injected.
func DefaultJitter() float64 {
	return rand.Float64() * 0.25
}
