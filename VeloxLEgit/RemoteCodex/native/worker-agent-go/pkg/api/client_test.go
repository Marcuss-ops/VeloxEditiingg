// Package api provides a small HTTP client used by the data-plane bridges
// (uploads, asset downloads). All control-plane traffic between the
// worker and the master flows over the gRPC `WorkerControl` bidi stream
// — there are no HTTP control endpoints to exercise here.
package api

import (
	"context"
	"testing"
	"time"
)

// TestNewClient tests client creation with options.
func TestNewClient(t *testing.T) {
	client := NewClient("http://localhost:8000",
		WithTimeout(10*time.Second),
		WithWorkerID("test-worker"),
		WithRetry(3, 5*time.Second),
	)

	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	if client.baseURL != "http://localhost:8000" {
		t.Errorf("Expected baseURL http://localhost:8000, got %s", client.baseURL)
	}

	if client.retryCount != 3 {
		t.Errorf("Expected retry count 3, got %d", client.retryCount)
	}

	if client.headers["X-Worker-ID"] != "test-worker" {
		t.Errorf("Expected X-Worker-ID header to be set")
	}
}

// TestIsRetryableError tests retryable error detection.
func TestIsRetryableError(t *testing.T) {
	if !isRetryableError(context.DeadlineExceeded) {
		t.Error("Expected DeadlineExceeded to be retryable")
	}

	if isRetryableError(nil) {
		t.Error("Expected nil error to not be retryable")
	}
}

// TestAuthTokenSetGet verifies the worker can attach a bearer token used by
// data-plane uploads. The token is no longer sourced from a HTTP register
// roundtrip (gRPC replaces it); this test pins the Set/AuthToken round-trip.
func TestAuthTokenSetGet(t *testing.T) {
	c := NewClient("http://localhost:8000")
	if c.AuthToken() != "" {
		t.Fatalf("Expected empty auth token initially, got %q", c.AuthToken())
	}
	c.SetAuthToken("test-token-abc")
	if c.AuthToken() != "test-token-abc" {
		t.Fatalf("Expected auth token to be 'test-token-abc', got %q", c.AuthToken())
	}
}
