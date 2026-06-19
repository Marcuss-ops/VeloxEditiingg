// Package api provides a small HTTP client used by the data-plane bridges
// (uploads, asset downloads, health probes). All control-plane traffic
// between the worker and the master flows over the gRPC `WorkerControl`
// bidi stream — there are no HTTP control endpoints to exercise here.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// TestHealthCheck tests the readiness probe endpoint used at bootstrap.
func TestHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("Expected path /health, got %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.HealthCheck(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestAPIError tests handling of API errors.
func TestAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.HealthCheck(ctx)
	if err == nil {
		t.Error("Expected error on 500 response")
	}
}

// TestRetryOnFailure tests retry behavior on failures.
func TestRetryOnFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++

		if attempts < 3 {
			// Fail first 2 attempts
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Succeed on 3rd attempt
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL,
		WithRetry(3, 10*time.Millisecond), // Fast retry for testing
	)
	ctx := context.Background()

	err := client.HealthCheck(ctx)
	if err != nil {
		t.Errorf("Expected no error after retries, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

// TestContextCancellation tests that requests respect context cancellation.
func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // Slow response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, WithTimeout(5*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := client.HealthCheck(ctx)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}
}

// TestWithHeader tests custom header option.
func TestWithHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom-Header") != "custom-value" {
			t.Errorf("Expected X-Custom-Header to be set")
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL, WithHeader("X-Custom-Header", "custom-value"))
	ctx := context.Background()

	_ = client.HealthCheck(ctx)
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
