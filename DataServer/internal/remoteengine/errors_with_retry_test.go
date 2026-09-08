package remoteengine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRetryPolicy_ShouldStop_UntypedError(t *testing.T) {
	policy := DefaultRetryPolicy(3)
	err := errors.New("some random error")
	got, stop := policy.ShouldStop(err, 0)
	if !stop {
		t.Fatal("untyped error should STOP (treated as permanent)")
	}
	if got != err {
		t.Fatal("ShouldStop should return the same untyped error")
	}
}

// ── Integration: withRetry limited-then-permanent for MALFORMED ───────────────

func TestWithRetry_MalformedPromotedAfterLimit(t *testing.T) {
	// Create a client with Retries=5 but MaxMalformedAttempts=2.
	// The fn always returns a MALFORMED_RESPONSE error.
	// After 2 malformed attempts, the error should be promoted to PERMANENT.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	// Replace the httpClient so we never actually make network calls.
	// withRetry uses fn, not httpClient, so this is safe.

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorMalformed,
			Code:    "DECODE",
			Message: "truncated json",
		}
	})

	// Should have been called exactly MaxMalformedAttempts times.
	// attempt 0 → malformedAttempts=1, ShouldStop(1) → no
	// attempt 1 → malformedAttempts=2, ShouldStop(2) → stop
	// So 2 calls, then stops.
	if callCount != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}

	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("final error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("promoted class: got %s, want PERMANENT", re.Class)
	}
	if !errors.Is(err, ErrMalformedRetryExceeded) {
		t.Fatal("final error should wrap ErrMalformedRetryExceeded")
	}
}

func TestWithRetry_PermanentStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorValidation,
			Code:    "HTTP_400",
			Message: "bad request",
		}
	})

	if callCount != 1 {
		t.Fatalf("permanent error should only call fn once, got %d", callCount)
	}

	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorValidation {
		t.Fatalf("class: got %s, want VALIDATION", re.Class)
	}
}

func TestWithRetry_TransientRetriesUntilSuccess(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		if callCount >= 2 {
			return nil // success on 2nd attempt (1 retry with ~1s backoff)
		}
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_500",
			Message: "server error",
		}
	})

	if err != nil {
		t.Fatalf("should succeed after retry, got %v", err)
	}
	if callCount != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}
}

func TestWithRetry_RateLimitRetriesWithRetryAfter(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 3})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		if callCount >= 2 {
			return nil
		}
		return &RemoteError{
			Class:      RemoteErrorRateLimit,
			Code:       "HTTP_429",
			Message:    "rate limited",
			RetryAfter: 1 * time.Millisecond, // very short for test speed
		}
	})

	if err != nil {
		t.Fatalf("should succeed after retry, got %v", err)
	}
	if callCount != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}
}

// ── Sentinel errors ──────────────────────────────────────────────────────────

func TestSentinelErrors(t *testing.T) {
	if !errors.Is(ErrNotConfigured, ErrNotConfigured) {
		t.Fatal("ErrNotConfigured should match itself")
	}
	if !errors.Is(ErrMalformedResponse, ErrMalformedResponse) {
		t.Fatal("ErrMalformedResponse should match itself")
	}
}

// ── Unwrap ───────────────────────────────────────────────────────────────────

func TestRemoteError_Unwrap(t *testing.T) {
	cause := errors.New("underlying network error")
	re := &RemoteError{
		Class:   RemoteErrorTransient,
		Message: "request failed",
		Cause:   cause,
	}

	if !errors.Is(re, cause) {
		t.Fatal("errors.Is should reach wrapped cause")
	}

	var target *RemoteError
	if !errors.As(re, &target) {
		t.Fatal("errors.As should match *RemoteError")
	}
	if target.Class != RemoteErrorTransient {
		t.Fatalf("target class: got %s, want TRANSIENT", target.Class)
	}
}

// ── Integration: 429 with Retry-After ────────────────────────────────────────

func TestClassifyHTTPResponse_429_WithRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
		w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	defer resp.Body.Close()

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)

	re := classifyHTTPResponse(resp, body[:n], nil)
	if re.Class != RemoteErrorRateLimit {
		t.Fatalf("class: got %s, want RATE_LIMIT", re.Class)
	}
	if re.RetryAfter != 30*time.Second {
		t.Fatalf("RetryAfter: got %v, want 30s", re.RetryAfter)
	}
	if !re.IsRetryable() {
		t.Fatal("RATE_LIMIT should be retryable")
	}
}

// ── Retry: exact attempt count and context behaviour ─────────────────────────
