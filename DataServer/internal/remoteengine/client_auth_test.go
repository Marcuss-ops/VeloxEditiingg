package remoteengine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
