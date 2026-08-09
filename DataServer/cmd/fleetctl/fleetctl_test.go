// fleetctl_test.go — minimal coverage for Step 15/15 fleetctl
// per design Q10 of the thinker call.
//
// Coverage map:
//
//	TestValidateDigest_AcceptsCanonicalLowercase
//	  ^sha256:[0-9a-f]{64}$ → nil err.
//
//	TestValidateDigest_RejectsUppercase
//	  uppercase hex fails the regex (Cosign emits lowercase;
//	  mixed-case input is operator error).
//
//	TestValidateDigest_RejectsLengthTooShort
//	  sha256: + <64 chars → exit-code 7 surface via error message.
//
//	TestValidateDigest_RejectsMobileRefs
//	  :latest / :main / :stable get a specific message rather
//	  than generic regex mismatch (better operator UX).
//
//	TestRunStatus_StatusOK_PrettyPrintsCard
//	  Mock HTTP server returns worker list; handler prints
//	  status table; assert ExitOK + stdout contains worker_id.
//
//	TestRunInspect_StatusOK_WorkerNotFound
//	  Mock returns 404; handler returns ExitWorkerNotFound (4).
//
//	TestRunUpdate_BadDigestReturnsExitImageInvalid
//	  Handler called with --digest=:latest → exit 7 without
//	  hitting HTTP at all (client-side gate).
//
//	TestMapHTTPStatusToOpExit
//	  Pin the matrix: 404 → 4, 409 → 5, 422 → 7, 401 → 2, 500
//	  → 1.
//
//	TestMapOperationKindToExit
//	  smoke→6, rollback→8, drain→1 (generic).
//
//	TestResolveTokenAdvanced
//	  env var + file precedence stable.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------- digest regex ----------

func TestValidateDigest_AcceptsCanonicalLowercase(t *testing.T) {
	canonical := "sha256:" + strings.Repeat("a", 64)
	if err := validateDigest(canonical); err != nil {
		t.Errorf("canonical lowercase digest must validate, got %v", err)
	}
}

func TestValidateDigest_RejectsUppercase(t *testing.T) {
	upper := "sha256:" + strings.Repeat("A", 64)
	if err := validateDigest(upper); err == nil {
		t.Errorf("uppercase hex must fail regex (Cosign emits lowercase; mixed-case is operator error)")
	}
}

func TestValidateDigest_RejectsLengthTooShort(t *testing.T) {
	short := "sha256:" + strings.Repeat("a", 63)
	err := validateDigest(short)
	if err == nil {
		t.Fatalf("63-hex digest must fail length check")
	}
	if !strings.Contains(err.Error(), "wrong length") {
		t.Errorf("error must mention length specifically: %v", err)
	}
}

func TestValidateDigest_RejectsMobileRefs(t *testing.T) {
	for _, ref := range []string{
		"ghcr.io/foo/bar:latest",
		"ghcr.io/foo/bar:main",
		"ghcr.io/foo/bar:stable",
	} {
		err := validateDigest(ref)
		if err == nil {
			t.Errorf("mobile ref %q must be rejected with mobile-ref-specific message", ref)
			continue
		}
		if !strings.Contains(err.Error(), "mobile ref") {
			t.Errorf("error message must mention 'mobile ref' for %q: %v", ref, err)
		}
	}
}

// ---------- exit-code matrix ----------

func TestMapHTTPStatusToOpExit(t *testing.T) {
	cases := []struct {
		httpStatus int
		wantExit   int
	}{
		{404, ExitWorkerNotFound},
		{409, ExitLeaseUnavailable},
		{400, ExitImageInvalid},
		{422, ExitImageInvalid},
		{401, ExitMisuse},
		{403, ExitMisuse},
		{500, ExitUnexpected},
		{502, ExitUnexpected},
		{503, ExitUnexpected},
		{418, ExitUnexpected}, // unhandled → unexpected
	}
	for _, c := range cases {
		got := MapHTTPStatusToOpExit(c.httpStatus)
		if got != c.wantExit {
			t.Errorf("MapHTTPStatusToOpExit(%d) = %d, want %d", c.httpStatus, got, c.wantExit)
		}
	}
}

func TestMapOperationKindToExit(t *testing.T) {
	cases := map[string]int{
		"smoke":    ExitSmokeFailed,
		"rollback": ExitRollbackFailed,
		"update":   ExitUnexpected,
		"drain":    ExitUnexpected,
		"resume":   ExitUnexpected,
		"unknown":  ExitUnexpected,
	}
	for kind, want := range cases {
		got := MapOperationKindToExit(kind)
		if got != want {
			t.Errorf("MapOperationKindToExit(%q) = %d, want %d", kind, got, want)
		}
	}
}

// ---------- HTTP client mock + handler end-to-end ----------

// newMockClient builds a fleetClient whose http.Client hits a
// recorded mock server. Returns (client, server). Tests use
// server.Close() in t.Cleanup.
func newMockClient(handler http.HandlerFunc) (*fleetClient, *httptest.Server) {
	srv := httptest.NewServer(handler)
	t := &http.Transport{}
	c := &fleetClient{
		baseURL: srv.URL,
		token:   "test-token",
		verbose: false,
		http:    &http.Client{Transport: t},
	}
	return c, srv
}

func TestRunStatus_StatusOK_PrettyPrintsCard(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers" {
			http.Error(w, "wrong path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "missing auth", http.StatusForbidden)
			return
		}
		// Return a 2-worker list with WorkerCard-shaped rows.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"workers": []map[string]any{
				{
					"worker_id":         "velox-worker-13197",
					"worker_name":       "velox-worker-01",
					"status":            "CONNECTED",
					"health":            "HEALTHY",
					"executor":          "scene.composite.v1",
					"executor_version":  "1",
					"active_jobs":       0.0,
					"max_active_jobs":   1.0,
					"last_smoke_status": "SUCCEEDED",
				},
				{
					"worker_id":         "velox-worker-523925eb",
					"worker_name":       "velox-worker-02",
					"status":            "CONNECTED",
					"health":            "DEGRADED",
					"executor":          "scene.composite.v1",
					"executor_version":  "1",
					"active_jobs":       1.0,
					"max_active_jobs":   1.0,
					"last_smoke_status": "FAILED",
				},
			},
			"count": 2,
		})
	})
	defer srv.Close()

	// Capture stdout.
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runStatus(c)
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK {
		t.Errorf("happy status code = %d, want %d", ec, ExitOK)
	}
	if !strings.Contains(string(out), "velox-worker-13197") {
		t.Errorf("stdout must list both workers; got:\n%s", out)
	}
	if !strings.Contains(string(out), "velox-worker-01") {
		t.Errorf("stdout must render worker_name; got:\n%s", out)
	}
	if !strings.Contains(string(out), "HEALTHY") {
		t.Errorf("stdout must render HEALTHY health; got:\n%s", out)
	}
	if !strings.Contains(string(out), "DEGRADED") {
		t.Errorf("stdout must render DEGRADED health; got:\n%s", out)
	}
}

func TestDigestFromRef_RequiresImmutableImageDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	if got := digestFromRef("ghcr.io/example/velox-worker@" + digest); got != digest {
		t.Fatalf("digestFromRef immutable ref = %q, want %q", got, digest)
	}
	if got := digestFromRef("ghcr.io/example/velox-worker:latest"); got != "" {
		t.Fatalf("digestFromRef mutable tag = %q, want empty", got)
	}
}

func TestRunStatusProduction_DriftReturnsNonZero(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(workerListResponse{Count: 1, Workers: []map[string]any{{
			"worker_id": "worker-1", "worker_name": "velox_worker_1",
			"target_digest": "ghcr.io/example/velox-worker@" + digest,
			"image_digest":  "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("b", 64),
		}}})
	})
	defer srv.Close()
	if got := runStatusMode(c, true); got == ExitOK {
		t.Fatal("production status must fail on digest drift")
	}
}

func TestRunSSHCheck_AllReady_ReturnsExitOK(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers/ssh-check" {
			http.Error(w, "wrong path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"checked_at":       "2026-08-08T00:00:00Z",
			"key_file":         "/etc/velox/ssh/id_ed25519_velox",
			"known_hosts_file": "/etc/velox/ssh/known_hosts",
			"workers": []map[string]any{
				{"worker_id": "velox-worker-13197", "ssh": "PASS", "hostkey": "PASS", "sudo": "PASS", "detail": ""},
				{"worker_id": "velox-worker-523925eb", "ssh": "PASS", "hostkey": "PASS", "sudo": "PASS", "detail": ""},
			},
			"summary": map[string]any{"total": 2, "ssh_pass": 2, "key_pass": 2, "sudo_pass": 2, "ready": 2},
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runSSHCheck(c)
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK {
		t.Errorf("all-ready ssh-check code = %d, want %d", ec, ExitOK)
	}
	if !strings.Contains(string(out), "2/2 READY") {
		t.Errorf("stdout must render 2/2 READY; got:\n%s", out)
	}
	if !strings.Contains(string(out), "velox-worker-523925eb") {
		t.Errorf("stdout must list each worker; got:\n%s", out)
	}
}

func TestRunSSH_NotAllReady_ReturnsExitUnexpected(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"workers": []map[string]any{
				{"worker_id": "velox-worker-13197", "ssh": "FAIL", "hostkey": "PASS", "sudo": "SKIP", "detail": "ssh: unreachable"},
			},
			"summary": map[string]any{"total": 1, "ssh_pass": 0, "key_pass": 1, "sudo_pass": 0, "ready": 0},
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runSSHCheck(c)
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitUnexpected {
		t.Errorf("not-ready ssh-check code = %d, want %d", ec, ExitUnexpected)
	}
	if !strings.Contains(string(out), "NOT-READY") {
		t.Errorf("stdout must render NOT-READY verdict; got:\n%s", out)
	}
}

func TestRunInspect_NotFoundReturnsExitWorkerNotFound(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	defer srv.Close()
	ec := runInspect(c, []string{"missing-worker-id"})
	if ec != ExitWorkerNotFound {
		t.Errorf("404 must map to exit %d, got %d", ExitWorkerNotFound, ec)
	}
}

func TestRunUpdate_BadDigestReturnsExitImageInvalid(t *testing.T) {
	// No HTTP server needed — handler must short-circuit before
	// any network call.
	tmp := t.TempDir()
	tokPath := filepath.Join(tmp, "tok")
	_ = os.WriteFile(tokPath, []byte("test-token"), 0600)
	t.Setenv("VELOX_ADMIN_TOKEN", "")
	cfg := &clientConfig{MasterURL: "http://127.0.0.1:1", Token: "test", Verbose: false}
	c, _ := newFleetClient(cfg)

	ec := runUpdate(c, []string{"velox-worker-13197", "--digest=ghcr.io/foo/bar:latest"})
	if ec != ExitImageInvalid {
		t.Errorf("--digest=ghcr.io/foo/bar:latest must surface ExitImageInvalid (7), got %d", ec)
	}
}

// ---------- token resolution precedence ----------

func TestResolveTokenAdvanced_EnvPrecedence(t *testing.T) {
	// Env var is consulted before canonicalTokenPaths in
	// resolveTokenAdvanced only when no explicitFile is set;
	// explicit file beats env.
	t.Setenv("VELOX_ADMIN_TOKEN", "env-token")
	got, err := resolveTokenAdvanced("")
	if err != nil {
		t.Fatalf("env token must resolve without error: %v", err)
	}
	if got != "env-token" {
		t.Errorf("resolveTokenAdvanced with empty file returns %q, want %q", got, "env-token")
	}
}

func TestResolveTokenAdvanced_ExplicitFileBeatsEnv(t *testing.T) {
	tmp := t.TempDir()
	tokPath := filepath.Join(tmp, "tok")
	_ = os.WriteFile(tokPath, []byte("file-token"), 0600)
	t.Setenv("VELOX_ADMIN_TOKEN", "env-token")
	got, err := resolveTokenAdvanced(tokPath)
	if err != nil {
		t.Fatalf("explicit file must resolve: %v", err)
	}
	if got != "file-token" {
		t.Errorf("explicit file beats env: got %q, want %q", got, "file-token")
	}
}

// Guard so context import stays useful if tests slim down.
var _ = context.Background
var _ = fmt.Sprintf
