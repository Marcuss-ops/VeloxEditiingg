package remoteengine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestWithRetry_ExactAttemptCount verifies that withRetry makes exactly
// MaxAttempts calls when every attempt returns a retryable TRANSIENT error.
func TestWithRetry_ExactAttemptCount(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
	}))
	defer srv.Close()

	// Retries=1 → MaxAttempts=2, so we expect exactly 2 calls.
	client := newTestClient(t, srv.URL, "token", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.GetPipelineStatus(ctx, "trace_exact_attempts")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	got := atomic.LoadInt32(&callCount)
	if got != 2 {
		t.Fatalf("call count: got %d, want 2", got)
	}

	re, ok := err.(*RemoteError)
	if !ok {
		t.Fatalf("expected *RemoteError, got %T: %v", err, err)
	}
	if re.Class != RemoteErrorTransient {
		t.Fatalf("class: got %s, want TRANSIENT", re.Class)
	}
}

// TestWithRetry_BackoffDuration verifies that the total elapsed time for a
// retryable TRANSIENT error stays within the expected backoff bounds.
// With Retries=1 (MaxAttempts=2) the schedule waits attempt 0's backoff
// (1s) with ±20% jitter, so the elapsed wall time should be in the range
// [800ms, 1200ms].
func TestWithRetry_BackoffDuration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
	}))
	defer srv.Close()

	// Retries=1 → MaxAttempts=2 → one retry after attempt 0's backoff.
	client := newTestClient(t, srv.URL, "token", 1)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, _ = client.GetPipelineStatus(ctx, "trace_max_duration")
	elapsed := time.Since(start)

	// RetrySchedule(0) == 1s; AddJitter allows ±20% → [0.8s, 1.2s].
	// Use a small margin to avoid flaky tests on slow CI runners.
	minDur := 700 * time.Millisecond
	maxDur := 1300 * time.Millisecond
	if elapsed < minDur || elapsed > maxDur {
		t.Fatalf("elapsed %s outside expected range [%s, %s]", elapsed, minDur, maxDur)
	}
}

// TestWithRetry_ContextCancelled_StopsImmediately verifies that a cancelled
// context stops the retry loop before the next attempt, not after waiting for
// the full backoff schedule. The elapsed time must be far less than the
// ~1s backoff that would otherwise be required.
func TestWithRetry_ContextCancelled_StopsImmediately(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 5)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel the context shortly after the first failed attempt returns,
	// well before the 1s backoff can elapse.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := client.GetPipelineStatus(ctx, "trace_cancelled")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// The full retry schedule would take at least 1s of backoff. If the
	// loop respected cancellation, it should return in well under that.
	if elapsed >= time.Second {
		t.Fatalf("cancellation did not stop retries promptly: elapsed %s", elapsed)
	}
}

// TestDefaultRetryPolicy_Semantics verifies that DefaultRetryPolicy maps a
// Retries value to the correct total number of attempts and clamps malformed
// attempts to MaxAttempts.
func TestDefaultRetryPolicy_Semantics(t *testing.T) {
	tests := []struct {
		retries          int
		wantMaxAttempts  int
		wantMaxMalformed int
	}{
		{0, 4, 2}, // zero/negative defaults to 3 retries → 4 attempts
		{1, 2, 2},
		{2, 3, 2},
		{5, 6, 2},
	}

	for _, tt := range tests {
		p := DefaultRetryPolicy(tt.retries)
		if p.MaxAttempts != tt.wantMaxAttempts {
			t.Fatalf("DefaultRetryPolicy(%d).MaxAttempts = %d, want %d", tt.retries, p.MaxAttempts, tt.wantMaxAttempts)
		}
		if p.MaxMalformedAttempts != tt.wantMaxMalformed {
			t.Fatalf("DefaultRetryPolicy(%d).MaxMalformedAttempts = %d, want %d", tt.retries, p.MaxMalformedAttempts, tt.wantMaxMalformed)
		}
	}

	// When malformed limit would exceed total attempts, it is clamped.
	p := DefaultRetryPolicy(0)
	if p.MaxMalformedAttempts > p.MaxAttempts {
		t.Fatalf("MaxMalformedAttempts (%d) should not exceed MaxAttempts (%d)", p.MaxMalformedAttempts, p.MaxAttempts)
	}
}

// TestShouldStop_PermanentByDefault verifies that untyped and permanent errors
// stop the retry loop immediately, while transient errors allow retries.
func TestShouldStop_PermanentByDefault(t *testing.T) {
	policy := DefaultRetryPolicy(3)

	tests := []struct {
		name    string
		err     error
		wantStop bool
	}{
		{"nil", nil, false},
		{"transient", &RemoteError{Class: RemoteErrorTransient}, false},
		{"rate limit", &RemoteError{Class: RemoteErrorRateLimit}, false},
		{"malformed", &RemoteError{Class: RemoteErrorMalformed}, false},
		{"validation", &RemoteError{Class: RemoteErrorValidation}, true},
		{"authentication", &RemoteError{Class: RemoteErrorAuthentication}, true},
		{"permanent", &RemoteError{Class: RemoteErrorPermanent}, true},
		{"untyped error", errorString{"boom"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, stop := policy.ShouldStop(tt.err, 0)
			if stop != tt.wantStop {
				t.Fatalf("ShouldStop(%v) stop=%v, want %v", tt.err, stop, tt.wantStop)
			}
		})
	}
}

// TestShouldStop_MalformedPromotedToPermanent verifies that a MALFORMED_RESPONSE
// error is retried only up to MaxMalformedAttempts, after which it is promoted
// to PERMANENT.
func TestShouldStop_MalformedPromotedToPermanent(t *testing.T) {
	policy := DefaultRetryPolicy(3)
	malformedErr := &RemoteError{Class: RemoteErrorMalformed}

	for i := 0; i < policy.MaxMalformedAttempts; i++ {
		_, stop := policy.ShouldStop(malformedErr, i)
		if stop {
			t.Fatalf("malformed attempt %d should not stop (under limit)", i)
		}
	}

	// Once malformed attempts reach MaxMalformedAttempts, the error is
	// promoted to PERMANENT and the loop stops.
	promoted, stop := policy.ShouldStop(malformedErr, policy.MaxMalformedAttempts)
	if !stop {
		t.Fatal("malformed retry limit exceeded should stop")
	}
	re, ok := promoted.(*RemoteError)
	if !ok || re.Class != RemoteErrorPermanent {
		t.Fatalf("expected promoted PERMANENT error, got %v", promoted)
	}
	if !strings.Contains(re.Code, "_RETRY_EXCEEDED") {
		t.Fatalf("expected code to contain _RETRY_EXCEEDED, got %s", re.Code)
	}
}

// TestClassifyNetworkError_ContextCancelled_StopsRetry verifies that
// caller-initiated context cancellation is classified as PERMANENT so the
// retry loop stops immediately.
func TestClassifyNetworkError_ContextCancelled_StopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	re := ClassifyNetworkError(ctx.Err())
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("class: got %s, want PERMANENT", re.Class)
	}
	if re.IsRetryable() {
		t.Fatal("context.Canceled should not be retryable")
	}
}

// errorString is a simple error type used to simulate an untyped error.
type errorString struct {
	s string
}

func (e errorString) Error() string { return e.s }
