package downloader

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestRetryAfterParsesSeconds(t *testing.T) {
	resp := &http.Response{Header: http.Header{"Retry-After": []string{"3"}}}
	if got := RetryAfter(resp); got != 3*time.Second {
		t.Fatalf("RetryAfter seconds = %v, want 3s", got)
	}
}

// TestIsRetryableStatus pins the classification vocabulary: 401 is retryable
// (the worker's session token is re-issued on reconnect, so an auth failure
// during a master restart heals), while 403/404/other client errors stay
// permanent.
func TestIsRetryableStatus(t *testing.T) {
	for _, code := range []int{401, 408, 429, 500, 502, 503, 599} {
		if !IsRetryableStatus(code) {
			t.Errorf("IsRetryableStatus(%d) = false, want true", code)
		}
		if IsPermanentStatus(code) {
			t.Errorf("IsPermanentStatus(%d) = true, want false", code)
		}
	}
	for _, code := range []int{400, 403, 404, 409, 410, 422} {
		if IsRetryableStatus(code) {
			t.Errorf("IsRetryableStatus(%d) = true, want false", code)
		}
		if !IsPermanentStatus(code) {
			t.Errorf("IsPermanentStatus(%d) = false, want true", code)
		}
	}
}

func TestRetryAfterParsesHTTPDate(t *testing.T) {
	when := time.Now().Add(2 * time.Second).UTC().Truncate(time.Second)
	resp := &http.Response{Header: http.Header{"Retry-After": []string{when.Format(http.TimeFormat)}}}
	got := RetryAfter(resp)
	if got <= 0 || got > 3*time.Second {
		t.Fatalf("RetryAfter HTTP-date = %v, want positive duration <= 3s", got)
	}
}

func TestHTTPStatusErrorClassificationIsTyped(t *testing.T) {
	if !IsRetryableError(NewHTTPStatusError(http.StatusUnauthorized, "restart", 0)) {
		t.Fatal("401 must be retryable")
	}
	if IsRetryableError(NewHTTPStatusError(http.StatusForbidden, "denied", 0)) {
		t.Fatal("403 must be permanent")
	}
	if IsRetryableError(fmt.Errorf("wrapped status 500-looking text")) {
		t.Fatal("retryability must not be inferred from error text")
	}
}

func TestRetryOwnsAttemptWaitAndFinalAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts int
	err := Retry(ctx, RetryConfig{
		MaxAttempts: 2,
		Backoff:     []time.Duration{0},
	}, func(attempt int) AttemptResult {
		attempts++
		if attempt == 0 {
			return AttemptResult{Err: fmt.Errorf("transient"), Retry: true}
		}
		return AttemptResult{Err: fmt.Errorf("terminal")}
	})
	if err == nil || err.Error() != "terminal" {
		t.Fatalf("Retry error = %v, want terminal", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}
