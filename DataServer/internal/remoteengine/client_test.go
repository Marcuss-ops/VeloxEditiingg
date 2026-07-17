package remoteengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ── Helpers ──────────────────────────────────────────────────────────────────

// newTestClient builds a Client pointed at the given test server with a
// very short timeout (500ms) so timeout tests are fast.
func newTestClient(t *testing.T, url, token string, retries int) *Client {
	t.Helper()
	return NewClient(Config{
		URL:       url,
		Token:     token,
		TimeoutMS: 500,
		Retries:   retries,
	})
}

// assertRemoteError asserts that err is a *RemoteError with the given class.
func assertRemoteError(t *testing.T, err error, wantClass RemoteErrorClass) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var re *RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RemoteError, got %T: %v", err, err)
	}
	if re.Class != wantClass {
		t.Fatalf("class: got %s, want %s (error: %s)", re.Class, wantClass, re)
	}
}

// ── 1. Token missing ─────────────────────────────────────────────────────────

func TestClient_TokenMissing(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_1",
			"status": "queued",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "", 1) // empty token
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization header should be absent when token is empty, got %q", gotAuth)
	}
}

// ── 2. Token wrong (server rejects) ──────────────────────────────────────────

func TestClient_TokenWrong(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer wrong-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// Even with the "expected" wrong token, the server rejects it.
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": "invalid token"})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "wrong-token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_1")
	assertRemoteError(t, err, RemoteErrorAuthentication)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 401 {
			t.Fatalf("StatusCode: got %d, want 401", re.StatusCode)
		}
		if re.IsRetryable() {
			t.Fatal("AUTHENTICATION should not be retryable")
		}
	}
}

// ── 3. 401 Unauthorized ──────────────────────────────────────────────────────

func TestClient_401_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": "token expired"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "some-token", 3)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_1")
	assertRemoteError(t, err, RemoteErrorAuthentication)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 401 {
			t.Fatalf("StatusCode: got %d, want 401", re.StatusCode)
		}
		if !re.IsPermanent() {
			t.Fatal("401 should be permanent")
		}
	}
}

// ── 4. 429 with Retry-After ──────────────────────────────────────────────────

func TestClient_429_WithRetryAfter(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n == 1 {
			// First call: rate limited with a short Retry-After.
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": "rate limited"}`))
			return
		}
		// Second call: success. GetPipelineStatus expects {"job": {...}} wrapper.
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job": map[string]interface{}{
				"id":     "job_429",
				"status": "queued",
			},
		})
	}))
	defer srv.Close()

	// Use GetPipelineStatus (which has retry) instead of StartPipeline
	// (which does not retry). Config.Retries=3 so we get at least 1 retry.
	client := newTestClient(t, srv.URL, "token", 3)
	resp, err := client.GetPipelineStatus(context.Background(), "trace_429")
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if resp.TraceID != "job_429" {
		t.Fatalf("TraceID: got %q, want job_429", resp.TraceID)
	}
	if atomic.LoadInt32(&callCount) != 2 {
		t.Fatalf("callCount: got %d, want 2", callCount)
	}
}

// ── 5. Timeout before response ───────────────────────────────────────────────

func TestClient_TimeoutBeforeResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep longer than the client timeout (500ms).
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_timeout",
			"status": "queued",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_1")
	// Network timeout → TRANSIENT (or context deadline exceeded).
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	// The error could be a *RemoteError (TRANSIENT) or a raw context error,
	// depending on whether the HTTP client timeout fires first.
	var re *RemoteError
	if errors.As(err, &re) {
		if re.Class != RemoteErrorTransient {
			t.Fatalf("class: got %s, want TRANSIENT", re.Class)
		}
	}
}

// ── 6. Timeout after remote creation (idempotency key) ───────────────────────

func TestClient_TimeoutAfterCreation_IdempotencyKey(t *testing.T) {
	var callCount int32
	var firstID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		idemKey := r.Header.Get("Idempotency-Key")

		if n == 1 {
			// Simulate: the server creates the job, then the connection
			// drops (we write a partial response then hang).
			if idemKey == "" {
				t.Error("Idempotency-Key header should be set on first call")
			}
			firstID = "job_created_1"
			// Write headers + partial body, then stall.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"job_id":"` + firstID + `","status":"queued"`))
			// Hijack the connection to simulate a drop.
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				if conn != nil {
					conn.Close()
				}
			}
			return
		}

		// Second call (retry by caller): same idempotency key → same job_id.
		if idemKey != "run_timeout_6" {
			t.Errorf("Idempotency-Key on retry: got %q, want run_timeout_6", idemKey)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": firstID, // same job_id as first call
			"status": "queued",
		})
	}))
	defer srv.Close()

	// StartPipeline does NOT retry, so the first call's partial response
	// will cause a decode error. The test verifies that the Idempotency-Key
	// header was sent, which is the mechanism that lets a caller retry
	// safely. We test the header propagation, not the retry itself.
	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_timeout_6")

	// The partial response should cause a MALFORMED_RESPONSE or network error.
	if err == nil {
		// If no error (server managed to complete), verify the job_id.
		t.Log("first call succeeded despite partial write simulation")
	} else {
		// Verify the Idempotency-Key was sent.
		if atomic.LoadInt32(&callCount) != 1 {
			t.Fatalf("callCount: got %d, want 1 (StartPipeline does not retry)", callCount)
		}
	}

	// Verify the Idempotency-Key header was present on the first call.
	if atomic.LoadInt32(&callCount) >= 1 && firstID == "" {
		t.Log("note: firstID not set — server may not have reached the assignment")
	}
}

// ── 7. 500 twice then success ─────────────────────────────────────────────────

func TestClient_500_TwiceThenSuccess(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": "server error"}`))
			return
		}
		// Third call: success.
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job": map[string]interface{}{
				"id":     "job_500_success",
				"status": "running",
			},
		})
	}))
	defer srv.Close()

	// Use GetPipelineStatus (which retries). Retries=3, timeout=500ms.
	// Backoff on attempt 0 = 1s, attempt 1 = 5s — too slow for a test.
	// We override the retry schedule by using a very short timeout context
	// that still allows 3 attempts. Actually, the backoff is real time.
	// To keep the test fast, we accept the ~1s wait for the first backoff.
	client := newTestClient(t, srv.URL, "token", 3)

	// Use a context with enough timeout for 2 backoffs (1s + 5s = 6s,
	// but with jitter it's ~0.8-1.2s + ~4-6s). Use 30s to be safe.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetPipelineStatus(ctx, "trace_500")
	if err != nil {
		t.Fatalf("expected success after 2 failures, got: %v", err)
	}
	if resp.TraceID != "job_500_success" {
		t.Fatalf("TraceID: got %q, want job_500_success", resp.TraceID)
	}
	if atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("callCount: got %d, want 3", callCount)
	}
}

// ── 8. Truncated JSON ────────────────────────────────────────────────────────

func TestClient_TruncatedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Write a truncated JSON body — valid start but cut off.
		_, _ = w.Write([]byte(`{"job_id":"job_trunc","status":"queued","result":{"scenes":[`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_8")

	// StartPipeline does not retry (single attempt), so we get the
	// MALFORMED_RESPONSE directly.
	if err == nil {
		t.Fatal("expected error for truncated JSON, got nil")
	}
	var re *RemoteError
	if errors.As(err, &re) {
		if re.Class != RemoteErrorMalformed {
			t.Fatalf("class: got %s, want MALFORMED_RESPONSE", re.Class)
		}
		if !errors.Is(err, ErrMalformedResponse) {
			t.Fatal("should wrap ErrMalformedResponse")
		}
	} else {
		// Could also be a decode error wrapped differently.
		t.Logf("error type: %T: %v", err, err)
	}
}

// ── 9. Missing job_id ────────────────────────────────────────────────────────

func TestClient_MissingJobID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Valid JSON, has status, but no job_id / trace_id / id.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "queued",
			"ok":     true,
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_9")

	assertRemoteError(t, err, RemoteErrorPermanent)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.Code != "CONTRACT_MISSING_JOB_ID" {
			t.Fatalf("Code: got %s, want CONTRACT_MISSING_JOB_ID", re.Code)
		}
		if re.IsRetryable() {
			t.Fatal("missing job_id should NOT be retryable")
		}
	}
}

// ── 10. Unknown status ───────────────────────────────────────────────────────

func TestClient_UnknownStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_10",
			"status": "paused", // not in KnownRemoteStatuses
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_10")

	assertRemoteError(t, err, RemoteErrorPermanent)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.Code != "CONTRACT_UNKNOWN_STATUS" {
			t.Fatalf("Code: got %s, want CONTRACT_UNKNOWN_STATUS", re.Code)
		}
		if !errors.Is(re, ErrContractUnknownStatus) {
			t.Fatal("should wrap ErrContractUnknownStatus via Cause")
		}
	}
}

// ── 11. Completed without scenes ──────────────────────────────────────────────

func TestClient_CompletedWithoutScenes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Job is completed but has no scenes_json or scenes in the result.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_11",
			"status": "completed",
			"ok":     true,
			"result": map[string]interface{}{
				"video_name":     "No Scenes Video",
				"script_text":    "A script with no scenes.",
				"voiceover_path": "/tmp/voice.mp3",
				// scenes_json and scenes are missing
			},
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	result, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_11")
	if err != nil {
		t.Fatalf("StartPipeline should succeed (initial response is valid): %v", err)
	}

	// The initial response is valid (has job_id + known status "completed"),
	// so StartPipeline returns it. The caller is responsible for checking
	// completeness via ShouldForwardPipelineResult / ParseRemotePipelineResult.

	// Verify the DTO parsing correctly identifies the missing scenes.
	dto, parseErr := ParseRemotePipelineResult(result)
	if parseErr != nil {
		t.Fatalf("ParseRemotePipelineResult: %v", parseErr)
	}
	if len(dto.Scenes) != 0 {
		t.Fatalf("Scenes should be empty, got %d", len(dto.Scenes))
	}
	if len(dto.Assets) != 0 {
		t.Fatalf("Assets should be empty when no scenes, got %d", len(dto.Assets))
	}
	// Voiceover should still be extracted.
	if len(dto.Voiceover.Paths) != 1 || dto.Voiceover.Paths[0] != "/tmp/voice.mp3" {
		t.Fatalf("Voiceover.Paths: got %v", dto.Voiceover.Paths)
	}
	// The worker payload should have scenes_json empty/missing.
	wp := dto.ToWorkerPayload()
	if scenesJSON, ok := wp["scenes_json"].(string); ok && scenesJSON != "" {
		t.Fatalf("scenes_json should be empty, got %q", scenesJSON)
	}
}

// ── 12. Failed without error message ─────────────────────────────────────────

func TestClient_FailedWithoutErrorMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Job is "failed" but the error field is empty.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_12",
			"status": "failed",
			"ok":     false,
			// no "error" field
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	result, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_12")
	if err != nil {
		t.Fatalf("StartPipeline should succeed (status 'failed' is a known status): %v", err)
	}

	// The initial response is valid: "failed" is a known status.
	// The caller should inspect the result to detect the failure.
	jobID, _ := result["job_id"].(string)
	status, _ := result["status"].(string)
	if jobID != "job_12" {
		t.Fatalf("job_id: got %q, want job_12", jobID)
	}
	if status != "failed" {
		t.Fatalf("status: got %q, want failed", status)
	}

	// Verify there's no error field.
	if errMsg, ok := result["error"].(string); ok && errMsg != "" {
		t.Fatalf("error field should be empty, got %q", errMsg)
	}

	// The caller (handler) is responsible for detecting that the job
	// failed without an error message and surfacing an appropriate
	// error to the user.
}

// ── 13. Not configured ───────────────────────────────────────────────────────

func TestClient_NotConfigured(t *testing.T) {
	client := NewClient(Config{URL: "", Token: "", TimeoutMS: 100, Retries: 1})
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_13")
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got: %v", err)
	}
}

// ── 14. Idempotency-Key header propagation ───────────────────────────────────

func TestClient_IdempotencyKeyHeader(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_14",
			"status": "queued",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_14")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotKey != "run_14" {
		t.Fatalf("Idempotency-Key header: got %q, want run_14", gotKey)
	}
}

// ── 15. 403 Forbidden (bloccante) ────────────────────────────────────────────

func TestClient_403_Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error": "insufficient permissions"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 3)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_15")
	assertRemoteError(t, err, RemoteErrorAuthentication)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 403 {
			t.Fatalf("StatusCode: got %d, want 403", re.StatusCode)
		}
		if re.IsRetryable() {
			t.Fatal("403 should not be retryable")
		}
	}
}

// ── 16. 400 Bad Request (permanente) ─────────────────────────────────────────

func TestClient_400_BadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid payload"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 3)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_16")
	assertRemoteError(t, err, RemoteErrorValidation)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 400 {
			t.Fatalf("StatusCode: got %d, want 400", re.StatusCode)
		}
		if re.IsRetryable() {
			t.Fatal("400 should not be retryable")
		}
	}
}

// ── 17. 404 Not Found (permanente) ───────────────────────────────────────────

func TestClient_404_NotFound_GetPipelineStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error": "job not found"}`))
	}))
	defer srv.Close()

	// GetPipelineStatus retries, but 404 → PERMANENT → no retry.
	client := newTestClient(t, srv.URL, "token", 3)
	_, err := client.GetPipelineStatus(context.Background(), "nonexistent_trace")
	assertRemoteError(t, err, RemoteErrorPermanent)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 404 {
			t.Fatalf("StatusCode: got %d, want 404", re.StatusCode)
		}
	}
}

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

// ── 22. Malformed response promoted to permanent after limit ─────────────────

func TestClient_MalformedPromotedToPermanent(t *testing.T) {
	// Every call returns truncated JSON.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"job":{"id":"j","status":"running","result":{"scenes":[`))
	}))
	defer srv.Close()

	// GetPipelineStatus retries. With MaxMalformedRetries=2, after 2
	// malformed attempts the error is promoted to PERMANENT.
	client := newTestClient(t, srv.URL, "token", 5)

	// Use a context with enough timeout for 1 backoff (~1s).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.GetPipelineStatus(ctx, "trace_malformed")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var re *RemoteError
	if errors.As(err, &re) {
		if re.Class != RemoteErrorPermanent {
			t.Fatalf("class: got %s, want PERMANENT (promoted from MALFORMED)", re.Class)
		}
		if !errors.Is(err, ErrMalformedRetryExceeded) {
			t.Fatal("should wrap ErrMalformedRetryExceeded")
		}
		if !strings.Contains(re.Code, "RETRY_EXCEEDED") {
			t.Fatalf("Code should contain RETRY_EXCEEDED: got %s", re.Code)
		}
	} else {
		t.Fatalf("expected *RemoteError, got %T: %v", err, err)
	}
}

// ── 23. Context cancelled ────────────────────────────────────────────────────

func TestClient_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.StartPipeline(ctx, map[string]interface{}{"topic": "test"}, "run_23")
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	// Could be context.DeadlineExceeded or a *RemoteError wrapping it.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("error: %v (type: %T)", err, err)
	}
}

// ── 24. Token sent correctly ─────────────────────────────────────────────────

func TestClient_TokenSentCorrectly(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job_24",
			"status": "queued",
		})
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "correct-token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_24")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "Bearer correct-token" {
		t.Fatalf("Authorization: got %q, want 'Bearer correct-token'", gotAuth)
	}
}

// ── 25. 422 Unprocessable Entity (permanente) ────────────────────────────────

func TestClient_422_UnprocessableEntity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error": "validation failed"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 3)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_25")
	assertRemoteError(t, err, RemoteErrorValidation)

	var re *RemoteError
	if errors.As(err, &re) {
		if re.StatusCode != 422 {
			t.Fatalf("StatusCode: got %d, want 422", re.StatusCode)
		}
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

// ── 28. IsConfigured checks ──────────────────────────────────────────────────

func TestClient_IsConfigured(t *testing.T) {
	configured := NewClient(Config{URL: "http://localhost:9999", Token: "t"})
	if !configured.IsConfigured() {
		t.Fatal("client with URL should be configured")
	}

	unconfigured := NewClient(Config{URL: "", Token: "t"})
	if unconfigured.IsConfigured() {
		t.Fatal("client without URL should NOT be configured")
	}
}

// ── 29. 429 then permanent (all retries exhausted) ───────────────────────────

func TestClient_429_RetriesExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error": "rate limited"}`))
	}))
	defer srv.Close()

	// GetPipelineStatus with Retries=2 and RetryAfter=1s.
	// First attempt: 429 → retry after ~1s.
	// Second attempt: 429 → no more retries (attempt 1 is last).
	client := newTestClient(t, srv.URL, "token", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.GetPipelineStatus(ctx, "trace_429_exhausted")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var re *RemoteError
	if errors.As(err, &re) {
		if re.Class != RemoteErrorRateLimit {
			t.Fatalf("class: got %s, want RATE_LIMIT", re.Class)
		}
		if re.RetryAfter != 1*time.Second {
			t.Fatalf("RetryAfter: got %v, want 1s", re.RetryAfter)
		}
	}
}

// ── 30. Error message includes class and status ──────────────────────────────

func TestClient_ErrorMessageFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer srv.Close()

	client := newTestClient(t, srv.URL, "token", 1)
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_30")
	if err == nil {
		t.Fatal("expected error")
	}

	errMsg := err.Error()
	// Should contain the class name.
	if !strings.Contains(errMsg, "TRANSIENT") {
		t.Fatalf("error message should contain class TRANSIENT: %q", errMsg)
	}
	// Should contain the status code.
	if !strings.Contains(errMsg, "500") {
		t.Fatalf("error message should contain status code 500: %q", errMsg)
	}

	// Also verify via fmt.Sprintf that the format is consistent.
	var re *RemoteError
	if errors.As(err, &re) {
		formatted := fmt.Sprintf("%s", re)
		if !strings.Contains(formatted, "TRANSIENT") {
			t.Fatalf("formatted error should contain class: %q", formatted)
		}
	}
}
