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

func TestRetryAfterParsesHTTPDate(t *testing.T) {
	when := time.Now().Add(2 * time.Second).UTC().Truncate(time.Second)
	resp := &http.Response{Header: http.Header{"Retry-After": []string{when.Format(http.TimeFormat)}}}
	got := RetryAfter(resp)
	if got <= 0 || got > 3*time.Second {
		t.Fatalf("RetryAfter HTTP-date = %v, want positive duration <= 3s", got)
	}
}
