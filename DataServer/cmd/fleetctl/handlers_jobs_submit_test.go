package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// submitMockServer simulates the Master surface used by job submit:
// M2M key issue (admin token), job POST (m2m token), key disable
// (admin token). It records the observed request paths and the token
// used for the job POST so tests can assert the least-privilege flow.
func submitMockServer(t *testing.T) (*fleetClient, *httptest.Server, *submitCalls) {
	t.Helper()
	calls := &submitCalls{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/admin/m2m/keys":
			calls.record("issue:" + auth)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"client_id":        "fleetctl-job-test-123",
				"plaintext_secret": "m2m-secret-abc",
				"scopes":           []string{"jobs.submit"},
			})
		case r.Method == "POST" && r.URL.Path == "/api/v1/jobs":
			calls.record("submit:" + auth)
			body, _ := io.ReadAll(r.Body)
			var payload map[string]any
			_ = json.Unmarshal(body, &payload)
			if payload["idempotency_key"] == nil || payload["placement_pin_worker_id"] == nil {
				t.Errorf("job POST missing idempotency_key/placement_pin_worker_id: %s", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"job_id": "job-1", "ok": true})
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v1/admin/m2m/keys/"):
			calls.record("disable:" + auth)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == "GET" && r.URL.Path == "/api/v1/admin/workers":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"workers": []map[string]any{
					{"worker_id": "worker-a"},
					{"worker_id": "worker-b"},
				},
			})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/admin/jobs/"):
			_ = json.NewEncoder(w).Encode(map[string]any{"job": map[string]any{"status": "SUCCEEDED"}})
		default:
			t.Errorf("unexpected request: %s %s (auth=%s)", r.Method, r.URL.Path, auth)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	c := &fleetClient{
		baseURL: srv.URL,
		token:   "admin-token",
		verbose: false,
		http:    &http.Client{Transport: &http.Transport{}},
	}
	return c, srv, calls
}

type submitCalls struct {
	mu    sync.Mutex
	paths []string
}

func (s *submitCalls) record(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paths = append(s.paths, path)
}

func (s *submitCalls) count(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, p := range s.paths {
		if strings.HasPrefix(p, prefix) {
			n++
		}
	}
	return n
}

func writePayloadFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "payload.json")
	if err := os.WriteFile(path, []byte(`{"scene_id":"scene-x","render_plan":{"fps":30}}`), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	return path
}

func TestRunJobSubmit_UsesLeastPrivilegeM2MFlow(t *testing.T) {
	client, srv, calls := submitMockServer(t)
	defer srv.Close()
	payloadFile := writePayloadFile(t, t.TempDir())

	if got := runJobSubmit(client, []string{"--payload", payloadFile, "--workers", "worker-1"}, false); got != ExitOK {
		t.Fatalf("runJobSubmit exit = %d, want %d", got, ExitOK)
	}
	// Admin token used for key issue + disable; M2M token for the job POST.
	if calls.count("issue:Bearer admin-token") != 1 {
		t.Errorf("expected exactly one key issue with admin token; got %v", calls.paths)
	}
	if calls.count("submit:Bearer m2m-secret-abc") != 1 {
		t.Errorf("job POST must use the m2m token, not admin; got %v", calls.paths)
	}
	if calls.count("disable:Bearer admin-token") != 1 {
		t.Errorf("ephemeral key must be disabled on exit; got %v", calls.paths)
	}
}

func TestRunJobSubmit_SelectionAllResolvesWorkers(t *testing.T) {
	client, srv, calls := submitMockServer(t)
	defer srv.Close()
	payloadFile := writePayloadFile(t, t.TempDir())

	if got := runJobSubmit(client, []string{"--payload", payloadFile, "--workers", "all"}, false); got != ExitOK {
		t.Fatalf("runJobSubmit exit = %d, want %d", got, ExitOK)
	}
	if got := calls.count("submit:Bearer m2m-secret-abc"); got != 2 {
		t.Errorf("expected 2 job POSTs for worker-a+worker-b; got %d (%v)", got, calls.paths)
	}
}

func TestRunJobSubmit_MissingPayloadIsMisuse(t *testing.T) {
	client, srv, _ := submitMockServer(t)
	defer srv.Close()
	if got := runJobSubmit(client, []string{"--workers", "worker-1"}, false); got != ExitMisuse {
		t.Fatalf("runJobSubmit without payload exit = %d, want %d", got, ExitMisuse)
	}
}

func TestRunJobSubmit_InvalidPayloadIsMisuse(t *testing.T) {
	client, srv, _ := submitMockServer(t)
	defer srv.Close()
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`not json`), 0o600); err != nil {
		t.Fatalf("write bad payload: %v", err)
	}
	if got := runJobSubmit(client, []string{"--payload", bad, "--workers", "worker-1"}, false); got != ExitMisuse {
		t.Fatalf("runJobSubmit bad payload exit = %d, want %d", got, ExitMisuse)
	}
}

func TestRunJobSubmit_UnknownFlagIsMisuse(t *testing.T) {
	client, srv, _ := submitMockServer(t)
	defer srv.Close()
	payloadFile := writePayloadFile(t, t.TempDir())
	if got := runJobSubmit(client, []string{"--payload", payloadFile, "--bogus"}, false); got != ExitMisuse {
		t.Fatalf("runJobSubmit bogus flag exit = %d, want %d", got, ExitMisuse)
	}
}

func TestParseJobSubmitArgs_Defaults(t *testing.T) {
	opts, err := parseJobSubmitArgs([]string{"--payload", "/tmp/p.json"})
	if err != nil {
		t.Fatalf("parseJobSubmitArgs: %v", err)
	}
	if opts.selection != "all" {
		t.Errorf("default selection = %q, want all", opts.selection)
	}
	if !strings.HasPrefix(opts.idemPrefix, "fleetctl-") {
		t.Errorf("default idempotency prefix = %q, want fleetctl-*", opts.idemPrefix)
	}
	if opts.wait {
		t.Errorf("default wait = true, want false")
	}
}

func TestParseJobSubmitArgs_EqualsForms(t *testing.T) {
	opts, err := parseJobSubmitArgs([]string{
		"--payload=/tmp/p.json",
		"--workers=worker-1,worker-2",
		"--idempotency-prefix=my-run",
		"--wait",
	})
	if err != nil {
		t.Fatalf("parseJobSubmitArgs: %v", err)
	}
	if opts.payloadFile != "/tmp/p.json" || opts.selection != "worker-1,worker-2" || opts.idemPrefix != "my-run" || !opts.wait {
		t.Errorf("unexpected opts: %+v", opts)
	}
}

func TestParseJobSubmitArgs_RequiresPayload(t *testing.T) {
	if _, err := parseJobSubmitArgs([]string{"--workers", "worker-1"}); err == nil {
		t.Fatal("expected error when --payload is missing")
	}
}

// certifyMockServer additionally serves the M2M-scoped job status GET
// used by waitForJob (returns SUCCEEDED immediately) and the admin job
// snapshot used by printFinalJobSnapshot.
func certifyMockServer(t *testing.T) (*fleetClient, *httptest.Server, *submitCalls) {
	t.Helper()
	calls := &submitCalls{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		switch {
		case r.Method == "POST" && r.URL.Path == "/api/v1/admin/m2m/keys":
			calls.record("issue:" + auth)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"client_id":        "fleetctl-job-test-123",
				"plaintext_secret": "m2m-secret-abc",
				"scopes":           []string{"jobs.submit"},
			})
		case r.Method == "POST" && r.URL.Path == "/api/v1/jobs":
			calls.record("submit:" + auth)
			_ = json.NewEncoder(w).Encode(map[string]any{"job_id": "job-1", "ok": true})
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v1/admin/m2m/keys/"):
			calls.record("disable:" + auth)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == "GET" && r.URL.Path == "/api/v1/jobs/job-1":
			calls.record("status:" + auth)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "SUCCEEDED"})
		case r.Method == "GET" && r.URL.Path == "/api/v1/admin/jobs/job-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"job": map[string]any{"status": "SUCCEEDED"}})
		default:
			t.Errorf("unexpected request: %s %s (auth=%s)", r.Method, r.URL.Path, auth)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	c := &fleetClient{
		baseURL: srv.URL,
		token:   "admin-token",
		verbose: false,
		http:    &http.Client{Transport: &http.Transport{}},
	}
	return c, srv, calls
}

func TestRunJobSubmit_CertifyWaitsForTerminalAndPrintsSnapshot(t *testing.T) {
	t.Setenv("FLEETCTL_JOB_TIMEOUT_SECONDS", "5")
	t.Setenv("FLEETCTL_JOB_POLL_SECONDS", "1")
	client, srv, calls := certifyMockServer(t)
	defer srv.Close()
	payloadFile := writePayloadFile(t, t.TempDir())

	if got := runJobSubmit(client, []string{"--payload", payloadFile, "--workers", "worker-1"}, true); got != ExitOK {
		t.Fatalf("runJobSubmit(certify) exit = %d, want %d", got, ExitOK)
	}
	// certify forces the wait path: status polled with the m2m token,
	// final snapshot fetched with the admin token, key disabled.
	if calls.count("status:Bearer m2m-secret-abc") < 1 {
		t.Errorf("certify must poll job status with the m2m token; got %v", calls.paths)
	}
	if calls.count("disable:Bearer admin-token") != 1 {
		t.Errorf("ephemeral key must be disabled on certify exit; got %v", calls.paths)
	}
}

func TestRunJobSubmit_WaitFlagPolls(t *testing.T) {
	t.Setenv("FLEETCTL_JOB_TIMEOUT_SECONDS", "5")
	t.Setenv("FLEETCTL_JOB_POLL_SECONDS", "1")
	client, srv, calls := certifyMockServer(t)
	defer srv.Close()
	payloadFile := writePayloadFile(t, t.TempDir())

	if got := runJobSubmit(client, []string{"--payload", payloadFile, "--workers", "worker-1", "--wait"}, false); got != ExitOK {
		t.Fatalf("runJobSubmit(--wait) exit = %d, want %d", got, ExitOK)
	}
	if calls.count("status:Bearer m2m-secret-abc") < 1 {
		t.Errorf("--wait must poll job status; got %v", calls.paths)
	}
}
