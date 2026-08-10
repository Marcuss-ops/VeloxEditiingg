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
	"time"
)

// ---------- digest regex ----------

func TestNormalizeDigestArgAcceptsPinnedImageReference(t *testing.T) {
	ref := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	got, err := normalizeDigestArg(ref)
	if err != nil {
		t.Fatalf("normalize pinned image: %v", err)
	}
	want := "sha256:" + strings.Repeat("a", 64)
	if got != want {
		t.Fatalf("normalized digest = %q, want %q", got, want)
	}
}

func TestNormalizeDigestArgRejectsInvalidPinnedImage(t *testing.T) {
	if _, err := normalizeDigestArg("ghcr.io/example/velox-worker:latest"); err == nil {
		t.Fatal("mutable image reference must be rejected")
	}
}

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

func TestRunStatus_JSONEmitsWorkerList(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workerListResponse{
			Workers: []map[string]any{{"worker_id": "worker-json", "status": "CONNECTED"}},
			Count:   1,
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runStatusModeWithOutput(c, false, true)
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK {
		t.Fatalf("JSON status code = %d, want %d", ec, ExitOK)
	}
	var got workerListResponse
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("JSON status output: %v; output=%s", err, out)
	}
	if got.Count != 1 || len(got.Workers) != 1 || got.Workers[0]["worker_id"] != "worker-json" {
		t.Fatalf("unexpected JSON status output: %+v", got)
	}
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

func TestParseImageMutationArgsSupportsLegacyAndFlagForms(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		name   string
		args   []string
		action string
		worker string
		image  string
		reason string
	}{
		{
			name: "legacy positional",
			args: []string{"worker-1", pinned, "manual update"}, action: "update",
			worker: "worker-1", image: pinned, reason: "manual update",
		},
		{
			name: "flags after worker",
			args: []string{"worker-1", "--digest", pinned, "--reason", "manual rollback", "--master=http://master"}, action: "rollback",
			worker: "worker-1", image: pinned, reason: "manual rollback",
		},
		{
			name: "flags before worker",
			args: []string{"--reason=ordered", "--digest=" + pinned, "worker-1"}, action: "update",
			worker: "worker-1", image: pinned, reason: "ordered",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker, image, reason, err := parseImageMutationArgs(tc.action, tc.args)
			if err != nil {
				t.Fatalf("parse args: %v", err)
			}
			if worker != tc.worker || image != tc.image || reason != tc.reason {
				t.Fatalf("parsed = (%q, %q, %q), want (%q, %q, %q)", worker, image, reason, tc.worker, tc.image, tc.reason)
			}
		})
	}
}

func TestRunUpdate_PreservesPinnedReferenceAndPollsToSuccess(t *testing.T) {
	pinned := "ghcr.io/example/custom-worker@sha256:" + strings.Repeat("b", 64)
	var postBody map[string]any
	var postPath string
	var pollCount int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/admin/workers/worker-1/update":
			postPath = r.URL.Path
			if err := json.NewDecoder(r.Body).Decode(&postBody); err != nil {
				t.Errorf("decode update body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-update-1","worker_id":"worker-1","queued_at":"now"}`))
		case "GET /api/v1/admin/operations/op-update-1":
			pollCount++
			_ = json.NewEncoder(w).Encode(polledOperationRow{
				OperationID: "op-update-1", WorkerID: "worker-1", Status: "SUCCEEDED",
			})
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	defer srv.Close()

	ec := runUpdate(c, []string{"worker-1", "--digest", pinned, "--reason", "custom image"})
	if ec != ExitOK {
		t.Fatalf("update exit code = %d, want %d", ec, ExitOK)
	}
	if postPath != "/api/v1/admin/workers/worker-1/update" || pollCount != 1 {
		t.Fatalf("requests = path %q, polls %d; want update path and one terminal poll", postPath, pollCount)
	}
	if got, _ := postBody["target_digest"].(string); got != pinned {
		t.Fatalf("target_digest = %q, want original pinned reference %q", got, pinned)
	}
	if got, _ := postBody["reason"].(string); got != "custom image" {
		t.Fatalf("reason = %q, want custom image", got)
	}
}

func TestRunRollback_PositionalReferenceUsesUpdateEndpoint(t *testing.T) {
	pinned := "ghcr.io/example/previous-worker@sha256:" + strings.Repeat("c", 64)
	var postPath, pollPath string
	var gotBody map[string]any
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/admin/workers/worker-1/update":
			postPath = r.URL.Path
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Errorf("decode rollback body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-rollback-1","worker_id":"worker-1"}`))
		case "GET /api/v1/admin/operations/op-rollback-1":
			pollPath = r.URL.Path
			_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-rollback-1", Status: "SUCCEEDED"})
		default:
			http.Error(w, "unexpected rollback request", http.StatusNotFound)
		}
	})
	defer srv.Close()

	ec := runRollback(c, []string{"worker-1", pinned, "restore known-good"})
	if ec != ExitOK {
		t.Fatalf("rollback exit code = %d, want %d", ec, ExitOK)
	}
	if postPath != "/api/v1/admin/workers/worker-1/update" || pollPath != "/api/v1/admin/operations/op-rollback-1" {
		t.Fatalf("paths = POST %q, GET %q; want update POST and operation GET", postPath, pollPath)
	}
	if got, _ := gotBody["target_digest"].(string); got != pinned {
		t.Fatalf("rollback target_digest = %q, want %q", got, pinned)
	}
	if got, _ := gotBody["reason"].(string); got != "restore known-good" {
		t.Fatalf("rollback reason = %q, want restore known-good", got)
	}
}

func TestPollOperationLedgerWithInterval_ConvergesThroughRunning(t *testing.T) {
	statuses := []string{"QUEUED", "RUNNING", "SUCCEEDED"}
	var calls int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/operations/op-sequence" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		status := statuses[calls]
		calls++
		_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-sequence", Status: status})
	})
	defer srv.Close()

	row, err := pollOperationLedgerWithInterval(context.Background(), c, "op-sequence", time.Second, false, time.Millisecond)
	if err != nil {
		t.Fatalf("poll sequence: %v", err)
	}
	if row.Status != "SUCCEEDED" || calls != len(statuses) {
		t.Fatalf("final row/calls = %q/%d, want SUCCEEDED/%d", row.Status, calls, len(statuses))
	}
}

func TestPollOperationLedgerWithInterval_ReturnsTerminalFailure(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(polledOperationRow{
			OperationID: "op-failed", Status: "FAILED", ErrorMessage: "activation failed",
		})
	})
	defer srv.Close()

	row, err := pollOperationLedgerWithInterval(context.Background(), c, "op-failed", time.Second, false, time.Millisecond)
	if err != nil {
		t.Fatalf("terminal failure must return row without polling error: %v", err)
	}
	if row.Status != "FAILED" || row.ErrorMessage != "activation failed" {
		t.Fatalf("failure row = %+v", row)
	}
}

func TestRunOperations_PreservesFiltersAndEnvelope(t *testing.T) {
	var gotWorker, gotStatus string
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/operations" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		gotWorker = r.URL.Query().Get("worker_id")
		gotStatus = r.URL.Query().Get("status")
		_ = json.NewEncoder(w).Encode(operationsListResponse{
			Count:      1,
			Operations: []operationListRow{{OperationID: "op-1", WorkerID: "worker-1", Status: "RUNNING"}},
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runOperations(c, []string{"worker-1", "RUNNING"})
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK || gotWorker != "worker-1" || gotStatus != "RUNNING" {
		t.Fatalf("operations result = exit %d worker=%q status=%q", ec, gotWorker, gotStatus)
	}
	var response operationsListResponse
	if err := json.Unmarshal(out, &response); err != nil {
		t.Fatalf("operations output is not JSON: %v; output=%s", err, out)
	}
	if response.Count != 1 || len(response.Operations) != 1 || response.Operations[0].OperationID != "op-1" {
		t.Fatalf("unexpected operations response: %+v", response)
	}
}

func TestRunWaitReady_ConvergesAfterWorkerBecomesReady(t *testing.T) {
	var calls int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers/worker-ready" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		calls++
		ready := calls > 1
		readiness := map[string]any{"status": "starting"}
		health, connection := "DEGRADED", "CONNECTING"
		if ready {
			readiness["status"] = "ok"
			health, connection = "HEALTHY", "CONNECTED"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": "worker-ready", "readiness": readiness,
			"health": health, "connection_state": connection,
			"image_digest": "sha256:" + strings.Repeat("a", 64),
		})
	})
	defer srv.Close()

	ec := runWaitReadyWithInterval(c, "worker-ready", "sha256:"+strings.Repeat("a", 64), time.Second, time.Millisecond)
	if ec != ExitOK || calls != 2 {
		t.Fatalf("wait-ready exit/calls = %d/%d, want 0/2", ec, calls)
	}
}

func TestRunWaitReady_MissingReadinessDoesNotPass(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": "worker-no-readiness", "health": "HEALTHY", "connection_state": "CONNECTED",
		})
	})
	defer srv.Close()

	ec := runWaitReadyWithInterval(c, "worker-no-readiness", "", 5*time.Millisecond, time.Millisecond)
	if ec != ExitUnexpected {
		t.Fatalf("missing readiness exit = %d, want %d", ec, ExitUnexpected)
	}
}

func TestParseOperationsArgsRejectsDuplicateFilterSources(t *testing.T) {
	if _, err := parseOperationsArgs([]string{"worker-1", "--worker-id=worker-2"}); err == nil {
		t.Fatal("worker_id positional and flag filters must not be combined")
	}
	if _, err := parseOperationsArgs([]string{"worker-1", "RUNNING", "--status=FAILED"}); err == nil {
		t.Fatal("status positional and flag filters must not be combined")
	}
}

func TestRunOperationsRejectsInconsistentCount(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(operationsListResponse{
			Count:      2,
			Operations: []operationListRow{{OperationID: "op-1"}},
		})
	})
	defer srv.Close()
	if got := runOperations(c, nil); got != ExitUnexpected {
		t.Fatalf("inconsistent operations count exit = %d, want %d", got, ExitUnexpected)
	}
}

func TestRunWaitReady_DigestMismatchTimesOut(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": "worker-mismatch",
			"readiness": map[string]any{"status": "ok"},
			"health":    "HEALTHY", "connection_state": "CONNECTED",
			"image_digest": "sha256:" + strings.Repeat("b", 64),
		})
	})
	defer srv.Close()

	ec := runWaitReadyWithInterval(c, "worker-mismatch", "sha256:"+strings.Repeat("a", 64), 5*time.Millisecond, time.Millisecond)
	if ec != ExitUnexpected {
		t.Fatalf("digest mismatch exit = %d, want %d", ec, ExitUnexpected)
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
