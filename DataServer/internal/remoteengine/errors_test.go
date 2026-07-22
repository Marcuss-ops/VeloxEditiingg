package remoteengine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ── ClassifyHTTPError ────────────────────────────────────────────────────────

func TestClassifyHTTPError(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantClass  RemoteErrorClass
		wantRetry  bool
	}{
		{"400 → VALIDATION", 400, RemoteErrorValidation, false},
		{"422 → VALIDATION", 422, RemoteErrorValidation, false},
		{"401 → AUTHENTICATION", 401, RemoteErrorAuthentication, false},
		{"403 → AUTHENTICATION", 403, RemoteErrorAuthentication, false},
		{"429 → RATE_LIMIT", 429, RemoteErrorRateLimit, true},
		{"408 → TRANSIENT", 408, RemoteErrorTransient, true},
		{"500 → TRANSIENT", 500, RemoteErrorTransient, true},
		{"502 → TRANSIENT", 502, RemoteErrorTransient, true},
		{"503 → TRANSIENT", 503, RemoteErrorTransient, true},
		{"504 → TRANSIENT", 504, RemoteErrorTransient, true},
		{"404 → PERMANENT", 404, RemoteErrorPermanent, false},
		{"405 → PERMANENT (other 4xx)", 405, RemoteErrorPermanent, false},
		{"413 → PERMANENT (payload too large)", 413, RemoteErrorPermanent, false},
		{"415 → PERMANENT (unsupported media type)", 415, RemoteErrorPermanent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := ClassifyHTTPError(tt.statusCode, "error body", nil)
			if re.Class != tt.wantClass {
				t.Fatalf("class: got %s, want %s", re.Class, tt.wantClass)
			}
			if re.IsRetryable() != tt.wantRetry {
				t.Fatalf("IsRetryable: got %v, want %v", re.IsRetryable(), tt.wantRetry)
			}
			if re.IsPermanent() == tt.wantRetry {
				t.Fatalf("IsPermanent: got %v, want %v", re.IsPermanent(), !tt.wantRetry)
			}
			if re.StatusCode != tt.statusCode {
				t.Fatalf("StatusCode: got %d, want %d", re.StatusCode, tt.statusCode)
			}
			if re.Code == "" {
				t.Fatal("Code should not be empty")
			}
		})
	}
}

// ── RemoteError.Error() ──────────────────────────────────────────────────────

func TestRemoteError_Error(t *testing.T) {
	t.Run("with status code", func(t *testing.T) {
		re := &RemoteError{
			Class:      RemoteErrorValidation,
			StatusCode: 400,
			Message:    "bad request",
		}
		s := re.Error()
		if !strings.Contains(s, "VALIDATION") {
			t.Fatalf("Error() should contain class: %s", s)
		}
		if !strings.Contains(s, "400") {
			t.Fatalf("Error() should contain status code: %s", s)
		}
		if !strings.Contains(s, "bad request") {
			t.Fatalf("Error() should contain message: %s", s)
		}
	})

	t.Run("without status code (network)", func(t *testing.T) {
		re := &RemoteError{
			Class:   RemoteErrorTransient,
			Message: "connection refused",
		}
		s := re.Error()
		if !strings.Contains(s, "TRANSIENT") {
			t.Fatalf("Error() should contain class: %s", s)
		}
		if strings.Contains(s, "HTTP") {
			t.Fatalf("Error() should NOT contain HTTP for network error: %s", s)
		}
	})

	t.Run("nil error", func(t *testing.T) {
		var re *RemoteError
		s := re.Error()
		if s != "<nil>" {
			t.Fatalf("nil Error(): got %q, want <nil>", s)
		}
	})
}

// ── IsRetryable / IsPermanent ────────────────────────────────────────────────

func TestRemoteError_IsRetryable_IsPermanent(t *testing.T) {
	tests := []struct {
		class     RemoteErrorClass
		retryable bool
		permanent bool
	}{
		{RemoteErrorValidation, false, true},
		{RemoteErrorAuthentication, false, true},
		{RemoteErrorRateLimit, true, false},
		{RemoteErrorTransient, true, false},
		{RemoteErrorPermanent, false, true},
		{RemoteErrorMalformed, true, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.class), func(t *testing.T) {
			re := &RemoteError{Class: tt.class}
			if re.IsRetryable() != tt.retryable {
				t.Fatalf("IsRetryable: got %v, want %v", re.IsRetryable(), tt.retryable)
			}
			if re.IsPermanent() != tt.permanent {
				t.Fatalf("IsPermanent: got %v, want %v", re.IsPermanent(), tt.permanent)
			}
		})
	}

	t.Run("nil receiver", func(t *testing.T) {
		var re *RemoteError
		if re.IsRetryable() {
			t.Fatal("nil IsRetryable should be false")
		}
		if re.IsPermanent() {
			t.Fatal("nil IsPermanent should be false")
		}
	})
}

// ── ClassifyNetworkError ─────────────────────────────────────────────────────

func TestClassifyNetworkError(t *testing.T) {
	t.Run("nil cause returns nil", func(t *testing.T) {
		if re := ClassifyNetworkError(nil); re != nil {
			t.Fatalf("nil cause should return nil, got %v", re)
		}
	})

	t.Run("generic network error is TRANSIENT", func(t *testing.T) {
		cause := errors.New("connection refused")
		re := ClassifyNetworkError(cause)
		if re.Class != RemoteErrorTransient {
			t.Fatalf("class: got %s, want TRANSIENT", re.Class)
		}
		if !re.IsRetryable() {
			t.Fatal("should be retryable")
		}
		if re.Code != "NETWORK" {
			t.Fatalf("code: got %s, want NETWORK", re.Code)
		}
	})

	t.Run("context.Canceled is PERMANENT", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		re := ClassifyNetworkError(ctx.Err())
		if re.Class != RemoteErrorPermanent {
			t.Fatalf("class: got %s, want PERMANENT", re.Class)
		}
		if re.IsRetryable() {
			t.Fatal("context.Canceled should NOT be retryable")
		}
	})

	t.Run("context.DeadlineExceeded is PERMANENT", func(t *testing.T) {
		_, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		// Wait for the context to expire.
		time.Sleep(10 * time.Millisecond)
		re := ClassifyNetworkError(context.DeadlineExceeded)
		if re.Class != RemoteErrorPermanent {
			t.Fatalf("class: got %s, want PERMANENT", re.Class)
		}
		if re.IsRetryable() {
			t.Fatal("context.DeadlineExceeded should NOT be retryable")
		}
	})
}

// ── ClassifyDecodeError ──────────────────────────────────────────────────────

func TestClassifyDecodeError(t *testing.T) {
	t.Run("nil cause returns nil", func(t *testing.T) {
		if re := ClassifyDecodeError(nil, ""); re != nil {
			t.Fatalf("nil cause should return nil, got %v", re)
		}
	})

	t.Run("decode error is MALFORMED", func(t *testing.T) {
		cause := errors.New("unexpected end of JSON")
		re := ClassifyDecodeError(cause, `{"truncated"`)
		if re.Class != RemoteErrorMalformed {
			t.Fatalf("class: got %s, want MALFORMED_RESPONSE", re.Class)
		}
		if !re.IsRetryable() {
			t.Fatal("MALFORMED should be retryable (limited)")
		}
		if re.Code != "DECODE" {
			t.Fatalf("code: got %s, want DECODE", re.Code)
		}
		if re.Body != `{"truncated"` {
			t.Fatalf("body should be preserved: got %q", re.Body)
		}
	})
}

// ── ParseRetryAfter ──────────────────────────────────────────────────────────

func TestParseRetryAfter(t *testing.T) {
	t.Run("empty header returns 0", func(t *testing.T) {
		if d := ParseRetryAfter(""); d != 0 {
			t.Fatalf("empty: got %v, want 0", d)
		}
	})

	t.Run("delta-seconds", func(t *testing.T) {
		if d := ParseRetryAfter("120"); d != 120*time.Second {
			t.Fatalf("120: got %v, want 120s", d)
		}
	})

	t.Run("zero seconds", func(t *testing.T) {
		if d := ParseRetryAfter("0"); d != 0 {
			t.Fatalf("0: got %v, want 0", d)
		}
	})

	t.Run("negative seconds returns 0", func(t *testing.T) {
		if d := ParseRetryAfter("-5"); d != 0 {
			t.Fatalf("-5: got %v, want 0", d)
		}
	})

	t.Run("HTTP-date in the future", func(t *testing.T) {
		future := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC1123)
		d := ParseRetryAfter(future)
		if d <= 0 || d > 2*time.Hour+10*time.Second {
			t.Fatalf("future date: got %v, want ~2h", d)
		}
	})

	t.Run("HTTP-date in the past returns 0", func(t *testing.T) {
		past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC1123)
		if d := ParseRetryAfter(past); d != 0 {
			t.Fatalf("past date: got %v, want 0", d)
		}
	})

	t.Run("garbage returns 0", func(t *testing.T) {
		if d := ParseRetryAfter("not-a-date"); d != 0 {
			t.Fatalf("garbage: got %v, want 0", d)
		}
	})
}

// ── RetrySchedule ────────────────────────────────────────────────────────────

func TestRetrySchedule(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 5 * time.Second},
		{2, 15 * time.Second},
		{3, 30 * time.Second},
		{4, 60 * time.Second},
		{5, 5 * time.Minute},
		{10, 5 * time.Minute},
		{100, 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(string(rune('0'+tt.attempt)), func(t *testing.T) {
			if got := RetrySchedule(tt.attempt); got != tt.want {
				t.Fatalf("attempt %d: got %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}

	t.Run("negative attempt is treated as 0", func(t *testing.T) {
		if got := RetrySchedule(-1); got != 1*time.Second {
			t.Fatalf("negative: got %v, want 1s", got)
		}
	})
}

// ── AddJitter ────────────────────────────────────────────────────────────────

func TestAddJitter(t *testing.T) {
	base := 10 * time.Second

	for seed := int64(0); seed < 100; seed++ {
		jittered := AddJitter(base, seed)
		min := time.Duration(float64(base) * 0.8)
		max := time.Duration(float64(base) * 1.2)
		if jittered < min || jittered > max {
			t.Fatalf("seed %d: jittered %v outside [%v, %v]", seed, jittered, min, max)
		}
	}

	t.Run("zero duration returns zero", func(t *testing.T) {
		if d := AddJitter(0, 42); d != 0 {
			t.Fatalf("zero: got %v, want 0", d)
		}
	})

	t.Run("deterministic for same seed", func(t *testing.T) {
		d1 := AddJitter(base, 42)
		d2 := AddJitter(base, 42)
		if d1 != d2 {
			t.Fatalf("same seed should be deterministic: %v vs %v", d1, d2)
		}
	})
}

// ── truncateBody ─────────────────────────────────────────────────────────────

func TestTruncateBody(t *testing.T) {
	t.Run("short body unchanged", func(t *testing.T) {
		body := "hello"
		if got := truncateBody(body, 256); got != body {
			t.Fatalf("got %q, want %q", got, body)
		}
	})

	t.Run("long body truncated with ellipsis", func(t *testing.T) {
		body := strings.Repeat("x", 500)
		got := truncateBody(body, 256)
		// 256 bytes of "x" + 3 bytes for UTF-8 ellipsis (…) = 259 bytes total.
		if len(got) != 259 {
			t.Fatalf("length: got %d, want 259", len(got))
		}
		if !strings.HasSuffix(got, "…") {
			t.Fatalf("should end with ellipsis: %q", got)
		}
	})

	t.Run("multi-byte characters not split", func(t *testing.T) {
		// Each character is 3 bytes in UTF-8.
		body := strings.Repeat("€", 100)
		got := truncateBody(body, 50)
		runes := []rune(got)
		if len(runes) != 51 { // 50 € + 1 …
			t.Fatalf("rune count: got %d, want 51", len(runes))
		}
	})
}

// ── RetryPolicy ──────────────────────────────────────────────────────────────

func TestDefaultRetryPolicy(t *testing.T) {
	// Retries=5 means 1 initial attempt + 5 retries = 6 total attempts.
	p := DefaultRetryPolicy(5)
	if p.MaxAttempts != 6 {
		t.Fatalf("MaxAttempts: got %d, want 6", p.MaxAttempts)
	}
	if p.MaxMalformedAttempts != DefaultMalformedRetryLimit {
		t.Fatalf("MaxMalformedAttempts: got %d, want %d", p.MaxMalformedAttempts, DefaultMalformedRetryLimit)
	}

	// When maxRetries + 1 is less than the malformed limit, the malformed
	// limit is clamped to MaxAttempts so we never exceed the overall cap.
	p2 := DefaultRetryPolicy(1)
	if p2.MaxMalformedAttempts != 2 {
		t.Fatalf("clamped MaxMalformedAttempts: got %d, want 2", p2.MaxMalformedAttempts)
	}

	// Zero or negative maxRetries defaults to 3 retries (4 total attempts).
	p3 := DefaultRetryPolicy(0)
	if p3.MaxAttempts != 4 {
		t.Fatalf("zero MaxAttempts: got %d, want 4", p3.MaxAttempts)
	}
}

func TestRetryPolicy_ShouldStop_Permanent(t *testing.T) {
	policy := DefaultRetryPolicy(5)

	tests := []RemoteErrorClass{
		RemoteErrorValidation,
		RemoteErrorAuthentication,
		RemoteErrorPermanent,
	}
	for _, class := range tests {
		t.Run(string(class), func(t *testing.T) {
			err := &RemoteError{Class: class, Message: "test"}
			got, stop := policy.ShouldStop(err, 0)
			if !stop {
				t.Fatal("ShouldStop should return true for permanent errors")
			}
			if got != err {
				t.Fatal("ShouldStop should return the same error for permanent")
			}
		})
	}
}

func TestRetryPolicy_ShouldStop_Transient(t *testing.T) {
	policy := DefaultRetryPolicy(5)
	err := &RemoteError{Class: RemoteErrorTransient, Message: "timeout"}
	got, stop := policy.ShouldStop(err, 0)
	if stop {
		t.Fatal("TRANSIENT should NOT stop")
	}
	if got != err {
		t.Fatal("ShouldStop should return the same error for transient")
	}
}

func TestRetryPolicy_ShouldStop_Malformed_LimitedRetry(t *testing.T) {
	policy := DefaultRetryPolicy(10) // MaxMalformedAttempts = 2

	// Simulate a realistic error from ClassifyDecodeError which wraps
	// ErrMalformedResponse in the Cause chain.
	err := &RemoteError{
		Class:   RemoteErrorMalformed,
		Code:    "DECODE",
		Message: "bad json",
		Cause:   fmt.Errorf("%w: %s", ErrMalformedResponse, "unexpected end of JSON"),
	}

	// Below the limit: keep retrying.
	got, stop := policy.ShouldStop(err, 0)
	if stop {
		t.Fatal("malformed attempt 0 should NOT stop")
	}
	got, stop = policy.ShouldStop(err, 1)
	if stop {
		t.Fatal("malformed attempt 1 should NOT stop")
	}

	// At the limit: promote to PERMANENT and stop.
	got, stop = policy.ShouldStop(err, 2)
	if !stop {
		t.Fatal("malformed attempt 2 should STOP")
	}

	var re *RemoteError
	if !errors.As(got, &re) {
		t.Fatalf("promoted error should be *RemoteError, got %T", got)
	}
	if re.Class != RemoteErrorPermanent {
		t.Fatalf("promoted class: got %s, want PERMANENT", re.Class)
	}
	if re.Code != "DECODE_RETRY_EXCEEDED" {
		t.Fatalf("promoted code: got %s, want DECODE_RETRY_EXCEEDED", re.Code)
	}
	// The promoted error should wrap ErrMalformedRetryExceeded.
	if !errors.Is(got, ErrMalformedRetryExceeded) {
		t.Fatal("promoted error should wrap ErrMalformedRetryExceeded")
	}
	// The original cause should still be discoverable.
	if !errors.Is(got, ErrMalformedResponse) {
		t.Fatal("promoted error should still wrap ErrMalformedResponse")
	}
}

func TestRetryPolicy_ShouldStop_NilError(t *testing.T) {
	policy := DefaultRetryPolicy(3)
	got, stop := policy.ShouldStop(nil, 0)
	if stop {
		t.Fatal("nil error should NOT stop")
	}
	if got != nil {
		t.Fatalf("nil error should return nil, got %v", got)
	}
}

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

func TestWithRetry_TransientExactAttempts(t *testing.T) {
	// Retries=3 means 1 initial attempt + 3 retries = 4 total attempts.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 3})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})

	if callCount != 4 {
		t.Fatalf("callCount: got %d, want 4", callCount)
	}

	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("error should be *RemoteError, got %T", err)
	}
	if re.Class != RemoteErrorTransient {
		t.Fatalf("class: got %s, want TRANSIENT", re.Class)
	}
}

func TestWithRetry_RetriesEqualsOneMeansTwoAttempts(t *testing.T) {
	// Retries=1 means 1 initial attempt + 1 retry = 2 total attempts.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 1})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})

	if callCount != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
}

func TestWithRetry_UntypedErrorStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return errors.New("untyped failure")
	})

	if callCount != 1 {
		t.Fatalf("untyped error should only call fn once, got %d", callCount)
	}
	if err == nil || err.Error() != "untyped failure" {
		t.Fatalf("expected untyped error, got %v", err)
	}
}

func TestWithRetry_ContextCanceledStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	callCount := 0
	err := client.withRetry(ctx, func(attempt int) error {
		callCount++
		return nil
	})

	if callCount != 0 {
		t.Fatalf("canceled context should not call fn, got %d", callCount)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestWithRetry_ContextDeadlineExceededStopsImmediately(t *testing.T) {
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	callCount := 0
	err := client.withRetry(context.Background(), func(attempt int) error {
		callCount++
		return context.DeadlineExceeded
	})

	if callCount != 1 {
		t.Fatalf("context.DeadlineExceeded should only call fn once, got %d", callCount)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWithRetry_MaxDuration(t *testing.T) {
	// Backoff schedule starts at 1s; with a 100ms context timeout the
	// retry loop should return before the first backoff completes.
	client := NewClient(Config{URL: "http://localhost:0", Retries: 5})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	callCount := 0
	start := time.Now()
	err := client.withRetry(ctx, func(attempt int) error {
		callCount++
		return &RemoteError{
			Class:   RemoteErrorTransient,
			Code:    "HTTP_503",
			Message: "server busy",
		}
	})
	elapsed := time.Since(start)

	if callCount < 1 {
		t.Fatal("expected at least one attempt")
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("expected to stop near context timeout, took %v", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// ── Integration: 401 Authentication ──────────────────────────────────────────

func TestClassifyHTTPResponse_401_Authentication(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"invalid token"}`))
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
	if re.Class != RemoteErrorAuthentication {
		t.Fatalf("class: got %s, want AUTHENTICATION", re.Class)
	}
	if re.IsRetryable() {
		t.Fatal("AUTHENTICATION should NOT be retryable")
	}
	if !re.IsPermanent() {
		t.Fatal("AUTHENTICATION should be permanent")
	}
}
