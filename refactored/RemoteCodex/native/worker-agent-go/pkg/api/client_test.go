// Package api provides HTTP client for communicating with the Velox Master server.
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

// TestRegisterWorker tests worker registration.
func TestRegisterWorker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/register" {
			t.Errorf("Expected path /api/workers/register, got %s", r.URL.Path)
		}

		if r.Method != "POST" {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	info := &WorkerInfo{
		WorkerID:   "test-worker-001",
		WorkerName: "Test Worker",
		Capabilities: map[string]bool{
			"video_render": true,
		},
		Hostname: "test-host",
		Version:  "1.0.0",
	}

	err := client.RegisterWorker(ctx, info)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestSendHeartbeat tests heartbeat sending.
func TestSendHeartbeat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/heartbeat" {
			t.Errorf("Expected path /api/workers/heartbeat, got %s", r.URL.Path)
		}

		var payload HeartbeatPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Failed to decode heartbeat payload: %v", err)
		}

		if payload.WorkerID != "test-worker-001" {
			t.Errorf("Expected worker_id test-worker-001, got %s", payload.WorkerID)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	payload := &HeartbeatPayload{
		WorkerID: "test-worker-001",
		Status:   "idle",
	}

	err := client.SendHeartbeat(ctx, payload)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestGetJob tests job retrieval.
func TestGetJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/get" {
			t.Errorf("Expected path /api/jobs/get, got %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data: Job{
				JobID:    "job-001",
				JobRunID: "run-001",
				JobType:  "render",
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	job, err := client.GetJob(ctx, "test-worker-001")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if job == nil {
		t.Fatal("Expected non-nil job")
	}

	if job.JobID != "job-001" {
		t.Errorf("Expected job_id job-001, got %s", job.JobID)
	}

	if job.JobType != "render" {
		t.Errorf("Expected job_type render, got %s", job.JobType)
	}
	if job.JobRunID != "run-001" {
		t.Errorf("Expected job_run_id run-001, got %s", job.JobRunID)
	}
}

// TestGetJobNoJob tests job retrieval when no job is available.
func TestGetJobNoJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Message: "no jobs available",
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	job, err := client.GetJob(ctx, "test-worker-001")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if job != nil {
		t.Errorf("Expected nil job when no jobs available, got %v", job)
	}
}

// TestSubmitJobResult tests job result submission.
func TestSubmitJobResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/result" {
			t.Errorf("Expected path /api/jobs/result, got %s", r.URL.Path)
		}

		var result JobResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			t.Errorf("Failed to decode job result: %v", err)
		}

		if result.JobID != "job-001" {
			t.Errorf("Expected job_id job-001, got %s", result.JobID)
		}
		if result.JobRunID != "run-001" {
			t.Errorf("Expected job_run_id run-001, got %s", result.JobRunID)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	result := &JobResult{
		JobID:    "job-001",
		JobRunID: "run-001",
		WorkerID: "test-worker-001",
		Status:   "success",
	}

	err := client.SubmitJobResult(ctx, result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestHealthCheck tests health check endpoint.
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

// TestUnregisterWorker tests worker unregistration.
func TestUnregisterWorker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/unregister" {
			t.Errorf("Expected path /api/workers/unregister, got %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(APIResponse{Success: true})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	ctx := context.Background()

	err := client.UnregisterWorker(ctx, "test-worker-001")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
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
