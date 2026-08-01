package remoteengine

import (
	"context"
	"errors"
	"testing"
)

// Package remoteengine / client_test.go — remote engine client tests.
//
// The client test suite is split by client behavior:
//
//	client_test.go           — shared helpers (newTestClient, assertRemoteError)
//	                          + configuration behavior (NotConfigured, IsConfigured).
//	client_auth_test.go      — authentication/token behavior (TokenMissing,
//	                          TokenWrong, 401, 403, TokenSentCorrectly).
//	client_retry_test.go     — retry / timeout / idempotency behavior
//	                          (429 retry, 500 twice, malformed promoted,
//	                          context cancelled, idempotency key).
//	client_contract_test.go  — response-contract validation (truncated JSON,
//	                          missing job_id, unknown status, completed without
//	                          scenes, failed without error message).
//	client_http_errors_test.go — HTTP status-code error mapping (400, 404, 422,
//	                          error message format).
//	client_endpoints_test.go — per-endpoint success (CancelPipeline,
//	                          StartPipeline, GetPipelineStatus,
//	                          GenerateSimpleScript, GenerateBatchScripts).

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

// ── 13. Not configured ───────────────────────────────────────────────────────

func TestClient_NotConfigured(t *testing.T) {
	client := NewClient(Config{URL: "", Token: "", TimeoutMS: 100, Retries: 1})
	_, err := client.StartPipeline(context.Background(), map[string]interface{}{"topic": "test"}, "run_13")
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got: %v", err)
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
