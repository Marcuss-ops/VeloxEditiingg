package downloader

import (
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
