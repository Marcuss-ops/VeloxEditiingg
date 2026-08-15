package remoteengine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stubRetryMetrics is a concurrency-safe RetryMetrics capture for tests.
type stubRetryMetrics struct {
	mu       sync.Mutex
	requests map[string]int
	retries  map[string]int
	failures map[string]int
}

func newStubRetryMetrics() *stubRetryMetrics {
	return &stubRetryMetrics{
		requests: map[string]int{},
		retries:  map[string]int{},
		failures: map[string]int{},
	}
}

func (m *stubRetryMetrics) RecordRequest(method string) {
	m.mu.Lock()
	m.requests[method]++
	m.mu.Unlock()
}

func (m *stubRetryMetrics) RecordRetry(class string) {
	m.mu.Lock()
	m.retries[class]++
	m.mu.Unlock()
}

func (m *stubRetryMetrics) RecordFailure(class string) {
	m.mu.Lock()
	m.failures[class]++
	m.mu.Unlock()
}

func (m *stubRetryMetrics) snapshot() (map[string]int, map[string]int, map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	requests := map[string]int{}
	for k, v := range m.requests {
		requests[k] = v
	}
	retries := map[string]int{}
	for k, v := range m.retries {
		retries[k] = v
	}
	failures := map[string]int{}
	for k, v := range m.failures {
		failures[k] = v
	}
	return requests, retries, failures
}

// TestClient_RecordsRetryMetrics verifies the retry loop emits the request +
// retry observations on a transient-then-success sequence.
func TestClient_RecordsRetryMetrics(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&callCount, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
			return
		}
		_, _ = w.Write([]byte(`{"trace_id": "job_ok", "status": "completed"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	metrics := newStubRetryMetrics()
	client.WithMetrics(metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := client.GetPipelineStatus(ctx, "job_ok"); err != nil {
		t.Fatalf("GetPipelineStatus: %v", err)
	}

	requests, retries, failures := metrics.snapshot()
	if got := requests["get_pipeline_status"]; got != 2 {
		t.Fatalf("requests[get_pipeline_status] = %d, want 2", got)
	}
	if got := retries["TRANSIENT"]; got != 1 {
		t.Fatalf("retries[TRANSIENT] = %d, want 1", got)
	}
	if len(failures) != 0 {
		t.Fatalf("failures = %v, want empty (operation succeeded)", failures)
	}
}

// TestClient_RecordsRetryExhausted verifies the retry loop records a terminal
// failure when the budget is exhausted on a persistently retryable error.
func TestClient_RecordsRetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	metrics := newStubRetryMetrics()
	client.WithMetrics(metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := client.GetPipelineStatus(ctx, "job_down"); err == nil {
		t.Fatal("expected error, got nil")
	}

	requests, retries, failures := metrics.snapshot()
	if got := requests["get_pipeline_status"]; got != 2 {
		t.Fatalf("requests[get_pipeline_status] = %d, want 2", got)
	}
	if got := retries["TRANSIENT"]; got != 2 {
		t.Fatalf("retries[TRANSIENT] = %d, want 2", got)
	}
	if got := failures["TRANSIENT"]; got != 1 {
		t.Fatalf("failures[TRANSIENT] = %d, want 1", got)
	}
}

// TestClient_NilLoggerNilMetricsNoPanic verifies the nil-safety contract:
// a client with a nil logger and nil metrics emits structured observations
// without panicking.
func TestClient_NilLoggerNilMetricsNoPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error": "temporary outage"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	client.WithLogger(nil)
	client.WithMetrics(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := client.GetPipelineStatus(ctx, "job_nil"); err == nil {
		t.Fatal("expected error, got nil")
	}
	if _, err := client.StartPipeline(ctx, map[string]interface{}{"topic": "t"}, "run_nil"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestErrorClassOf_MapsUntypedToUnknown pins the label fallback so a nil or
// non-*RemoteError never produces an empty metric label.
func TestErrorClassOf_MapsUntypedToUnknown(t *testing.T) {
	if got := errorClassOf(nil); got != "UNKNOWN" {
		t.Fatalf("errorClassOf(nil) = %q, want UNKNOWN", got)
	}
	if got := errorClassOf(errorString{"boom"}); got != "UNKNOWN" {
		t.Fatalf("errorClassOf(untyped) = %q, want UNKNOWN", got)
	}
	if got := errorClassOf(&RemoteError{Class: RemoteErrorRateLimit}); got != "RATE_LIMIT" {
		t.Fatalf("errorClassOf(rate limit) = %q, want RATE_LIMIT", got)
	}
}
