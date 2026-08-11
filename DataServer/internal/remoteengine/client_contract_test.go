package remoteengine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
			"status": "paused", // not in knownRemoteStatuses
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
	wp, err := dto.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("ToWorkerPayloadChecked: %v", err)
	}
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
