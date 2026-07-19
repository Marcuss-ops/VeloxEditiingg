package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// =====================================================================
// Happy-path tests for the per-worker read-model client methods.
//
// Each test stands up an httptest.NewServer returning a canned JSON
// payload that mirrors the master response shape field-for-field,
// then verifies: (1) the client issues a GET to the canonical path,
// (2) the options struct round-trips into the query string correctly,
// (3) the response unmarshals into the typed DTO and the structural
// fields are reconstructed without drift.
//
// These tests pin the JSON-shape mirror at
// RemoteCodex/native/worker-agent-go/pkg/api/workers_read.go vs
// DataServer/internal/handlers/server/api/workers_dto.go. Any shape
// drift between the two files (e.g. an `omitempty` flip, a JSON
// tag rename, a pointer-→-value type change) MUST be reflected in
// both files in the same atomic commit, otherwise these tests fail.
// =====================================================================

// TestGetWorkerMetrics_HappyPath exercises GetWorkerMetrics with a
// non-zero WorkerMetricsListOptions{Limit=50}, verifies the GET
// hits /api/v1/workers/{worker_id}/metrics, the `limit` query
// parameter is present, and the typed MetricSample pointer
// fields (LoadAverage) round-trip correctly.
func TestGetWorkerMetrics_HappyPath(t *testing.T) {
	canned := map[string]any{
		"worker_id": "w-1",
		"count":     1,
		"metrics": []map[string]any{
			{
				"sampled_at":            "2026-07-19T00:00:00Z",
				"connection_status":     "CONNECTED",
				"active_tasks":          float64(2),
				"task_slots":            float64(4),
				"cpu_utilization_ratio": 0.42,
				"memory_used_bytes":     float64(1234),
				"disk_free_bytes":       float64(5678),
				"load_average":          1.5,
			},
		},
	}

	var (
		gotPath  string
		gotQuery string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(canned)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL)
	got, err := c.GetWorkerMetrics(context.Background(), "w-1", WorkerMetricsListOptions{Limit: 50})
	if err != nil {
		t.Fatalf("GetWorkerMetrics: %v", err)
	}
	if gotPath != "/api/v1/workers/w-1/metrics" {
		t.Errorf("request path = %q, want /api/v1/workers/w-1/metrics", gotPath)
	}
	if !strings.Contains(gotQuery, "limit=50") {
		t.Errorf("query = %q, missing limit=50", gotQuery)
	}
	if got.WorkerID != "w-1" {
		t.Errorf("WorkerID = %q, want w-1", got.WorkerID)
	}
	if got.Count != 1 || len(got.Metrics) != 1 {
		t.Fatalf("Count=%d len(Metrics)=%d, want 1/1", got.Count, len(got.Metrics))
	}
	if got.Metrics[0].ConnectionStatus != "CONNECTED" {
		t.Errorf("ConnectionStatus = %q, want CONNECTED", got.Metrics[0].ConnectionStatus)
	}
	if got.Metrics[0].LoadAverage == nil || *got.Metrics[0].LoadAverage != 1.5 {
		t.Errorf("LoadAverage = %v, want pointer to 1.5", got.Metrics[0].LoadAverage)
	}
	// Numeric types round-trip cleanly through interface{} when
	// unmarshalled into a typed struct.
	if got.Metrics[0].ActiveTasks != 2 || got.Metrics[0].TaskSlots != 4 {
		t.Errorf("ActiveTasks=%d TaskSlots=%d, want 2/4", got.Metrics[0].ActiveTasks, got.Metrics[0].TaskSlots)
	}
}

// TestGetWorkerSessions_HappyPath exercises GetWorkerSessions with
// IncludeRevoked=true to verify the boolean query parameter is
// serialised, the JSON response unmarshals into the typed Session
// correctly, and an omitted field (BundleVersion) stays an empty
// string in the typed struct when the server doesn't set it.
func TestGetWorkerSessions_HappyPath(t *testing.T) {
	canned := map[string]any{
		"worker_id": "w-2",
		"count":     1,
		"sessions": []map[string]any{
			{
				"session_id":       "s-123",
				"session_type":     "control",
				"status":           "ACTIVE",
				"ip_address":       "203.0.113.7",
				"revoked":          false,
				"protocol_version": "v2",
				"created_at":       "2026-07-19T00:00:00Z",
				"expires_at":       "2026-07-19T00:05:00Z",
			},
		},
	}

	var (
		gotPath  string
		gotQuery string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(canned)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL)
	got, err := c.GetWorkerSessions(context.Background(), "w-2", WorkerSessionsListOptions{IncludeRevoked: true, Limit: 25})
	if err != nil {
		t.Fatalf("GetWorkerSessions: %v", err)
	}
	if gotPath != "/api/v1/workers/w-2/sessions" {
		t.Errorf("request path = %q, want /api/v1/workers/w-2/sessions", gotPath)
	}
	if !strings.Contains(gotQuery, "include_revoked=true") {
		t.Errorf("query = %q, missing include_revoked=true", gotQuery)
	}
	if !strings.Contains(gotQuery, "limit=25") {
		t.Errorf("query = %q, missing limit=25", gotQuery)
	}
	if got.WorkerID != "w-2" {
		t.Errorf("WorkerID = %q, want w-2", got.WorkerID)
	}
	if got.Count != 1 || len(got.Sessions) != 1 {
		t.Fatalf("Count=%d len(Sessions)=%d, want 1/1", got.Count, len(got.Sessions))
	}
	if got.Sessions[0].SessionID != "s-123" {
		t.Errorf("SessionID = %q, want s-123", got.Sessions[0].SessionID)
	}
	if got.Sessions[0].SessionType != "control" {
		t.Errorf("SessionType = %q, want control", got.Sessions[0].SessionType)
	}
	if got.Sessions[0].Revoked {
		t.Errorf("Revoked = true, want false")
	}
	// BundleVersion is omitempty on the DTO; absent from the canned
	// payload ⇒ empty string in the typed struct.
	if got.Sessions[0].BundleVersion != "" {
		t.Errorf("BundleVersion = %q, want empty (omitempty)", got.Sessions[0].BundleVersion)
	}
}

// TestGetWorkerEvents_HappyPath exercises GetWorkerEvents with all
// three options set (EventType, Since, Limit) to verify the query
// parameter encoding handles multiple values cleanly, and the typed
// Event response unmarshals correctly (incl. the `details` map
// staying map[string]any).
func TestGetWorkerEvents_HappyPath(t *testing.T) {
	canned := map[string]any{
		"worker_id": "w-3",
		"count":     1,
		"events": []map[string]any{
			{
				"event_id":    "e-9",
				"event_type":  "WORKER_STALE_DETECTED",
				"severity":    "warn",
				"reason_code": "heartbeat_stale",
				"details":     map[string]any{"age_seconds": float64(312)},
				"created_at":  "2026-07-19T00:00:00Z",
			},
		},
	}

	var (
		gotPath  string
		gotQuery string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(canned)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL)
	got, err := c.GetWorkerEvents(context.Background(), "w-3",
		WorkerEventsListOptions{
			EventType: "WORKER_STALE_DETECTED",
			Since:     "2026-07-19T00:00:00Z",
			Limit:     10,
		})
	if err != nil {
		t.Fatalf("GetWorkerEvents: %v", err)
	}
	if gotPath != "/api/v1/workers/w-3/events" {
		t.Errorf("request path = %q, want /api/v1/workers/w-3/events", gotPath)
	}
	for _, want := range []string{"event_type=WORKER_STALE_DETECTED", "since=2026-07-19T00", "limit=10"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("query = %q, missing %s", gotQuery, want)
		}
	}
	if got.WorkerID != "w-3" {
		t.Errorf("WorkerID = %q, want w-3", got.WorkerID)
	}
	if got.Count != 1 || len(got.Events) != 1 {
		t.Fatalf("Count=%d len(Events)=%d, want 1/1", got.Count, len(got.Events))
	}
	if got.Events[0].EventID != "e-9" {
		t.Errorf("EventID = %q, want e-9", got.Events[0].EventID)
	}
	if got.Events[0].EventType != "WORKER_STALE_DETECTED" {
		t.Errorf("EventType = %q, want WORKER_STALE_DETECTED", got.Events[0].EventType)
	}
	if got.Events[0].ReasonCode != "heartbeat_stale" {
		t.Errorf("ReasonCode = %q, want heartbeat_stale", got.Events[0].ReasonCode)
	}
	if got.Events[0].Details == nil {
		t.Fatalf("Details = nil, want populated map")
	}
	if v, ok := got.Events[0].Details["age_seconds"]; !ok || v != float64(312) {
		t.Errorf("Details[age_seconds] = %v (ok=%v), want 312", v, ok)
	}
}
