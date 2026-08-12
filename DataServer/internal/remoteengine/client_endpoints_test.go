package remoteengine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ── 18. CancelPipeline success ───────────────────────────────────────────────

func TestClient_CancelPipeline_Success(t *testing.T) {
	var method string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	err := client.CancelPipeline(context.Background(), "job_to_cancel")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if method != http.MethodDelete {
		t.Fatalf("method: got %s, want DELETE", method)
	}
}

// ── 19. CancelPipeline not found ─────────────────────────────────────────────

func TestClient_CancelPipeline_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	err := client.CancelPipeline(context.Background(), "nonexistent")
	assertRemoteError(t, err, RemoteErrorPermanent)
}

// ── 20. Successful StartPipeline with complete result ────────────────────────

func TestClient_StartPipeline_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_success",
			"status": "queued",
			"ok":     true,
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	result, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_20")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["job_id"] != "job_success" {
		t.Fatalf("job_id: got %v, want job_success", result["job_id"])
	}
	if result["status"] != "queued" {
		t.Fatalf("status: got %v, want queued", result["status"])
	}
}

// ── 21. Successful GetPipelineStatus ─────────────────────────────────────────

func TestClient_GetPipelineStatus_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job": map[string]interface{}{
				"id":        "job_status",
				"status":    "running",
				"progress":  42,
				"createdAt": "2026-07-17T12:00:00Z",
				"updatedAt": "2026-07-17T12:01:00Z",
			},
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	resp, err := client.GetPipelineStatus(context.Background(), "job_status")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.TraceID != "job_status" {
		t.Fatalf("TraceID: got %q, want job_status", resp.TraceID)
	}
	if resp.Status != "running" {
		t.Fatalf("Status: got %q, want running", resp.Status)
	}
	if resp.Progress != 42 {
		t.Fatalf("Progress: got %v, want 42", resp.Progress)
	}
}

// ── 26. GenerateSimpleScript success ─────────────────────────────────────────

func TestClient_GenerateSimpleScript_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/script-simple" {
			t.Errorf("path: got %s, want /api/script-simple", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":       true,
			"script":   "Once upon a time...",
			"title":    "Test Script",
			"trace_id": "trace_simple",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	resp, err := client.GenerateSimpleScript(context.Background(), SimpleScriptRequest{
		Topic: "test topic",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.OK {
		t.Fatal("OK should be true")
	}
	if resp.Script != "Once upon a time..." {
		t.Fatalf("Script: got %q", resp.Script)
	}
	if resp.Title != "Test Script" {
		t.Fatalf("Title: got %q", resp.Title)
	}
}

// ── 27. GenerateBatchScripts success ─────────────────────────────────────────

func TestClient_GenerateBatchScripts_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/script-multiple" {
			t.Errorf("path: got %s, want /api/script-multiple", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok": true,
			"scripts": []interface{}{
				map[string]interface{}{"topic": "t1", "script": "s1", "title": "T1"},
				map[string]interface{}{"topic": "t2", "script": "s2", "title": "T2"},
			},
			"trace_id": "trace_batch",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	resp, err := client.GenerateBatchScripts(context.Background(), BatchScriptRequest{
		Topics: []string{"t1", "t2"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.OK {
		t.Fatal("OK should be true")
	}
	if len(resp.Scripts) != 2 {
		t.Fatalf("Scripts: got %d, want 2", len(resp.Scripts))
	}
	if resp.Scripts[0].Script != "s1" {
		t.Fatalf("Scripts[0].Script: got %q", resp.Scripts[0].Script)
	}
}

func TestClient_GenerateSimpleScript_RetryKeepsIdempotencyKey(t *testing.T) {
	var keys []string
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if calls == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"script":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(Config{URL: server.URL, Retries: 1})
	_, err := client.GenerateSimpleScript(context.Background(), SimpleScriptRequest{Topic: "same"})
	if err != nil {
		t.Fatalf("GenerateSimpleScript() error = %v", err)
	}
	if calls != 2 || len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("attempts=%d idempotency keys=%v, want two equal non-empty keys", calls, keys)
	}
}
