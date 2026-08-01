package remoteengine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
