package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
func TestRunJobWatch_ParsesFlagsAndConverges(t *testing.T) {
	jobID, timeout, interval, jsonOutput, err := parseJobWatchArgs([]string{"job-1", "--timeout", "10", "--poll=1", "--json"})
	if err != nil {
		t.Fatalf("parse job watch: %v", err)
	}
	if jobID != "job-1" || timeout != 10*time.Second || interval != time.Second || !jsonOutput {
		t.Fatalf("parsed job watch = %q/%s/%s/%t", jobID, timeout, interval, jsonOutput)
	}

	var calls int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/jobs/job-1/events" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		calls++
		status := "RUNNING"
		if calls > 1 {
			status = "SUCCEEDED"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": status,
			"events": []map[string]any{{"timestamp": "2026-08-10T00:00:00Z", "event": "started"}},
		})
	})
	defer srv.Close()
	if got := runJobWatchWithInterval(c, jobID, time.Second, time.Millisecond, true); got != ExitOK || calls != 2 {
		t.Fatalf("job watch exit/calls = %d/%d, want 0/2", got, calls)
	}
}
func TestRunJobInspect_JSONUsesEscapedJobPath(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/api/v1/admin/jobs/job%2Fone" {
			http.Error(w, "unexpected escaped path: "+r.URL.EscapedPath(), http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"job": map[string]any{"status": "SUCCEEDED"}})
	})
	defer srv.Close()
	if got := runJob(c, []string{"inspect", "job/one", "--json"}); got != ExitOK {
		t.Fatalf("job inspect exit = %d, want %d", got, ExitOK)
	}
}
func TestRunJobWatch_FailedReturnsNonZero(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "FAILED"})
	})
	defer srv.Close()
	if got := runJobWatchWithInterval(c, "job-failed", time.Second, time.Millisecond, false); got != ExitUnexpected {
		t.Fatalf("failed job watch exit = %d, want %d", got, ExitUnexpected)
	}
}
func TestRunJobWatch_CompletedInputAssemblyStatusIsNotJobSuccess(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "COMPLETED"})
	})
	defer srv.Close()
	if got := runJobWatchWithInterval(c, "job-completed", time.Second, time.Millisecond, false); got != ExitUnexpected {
		t.Fatalf("completed input-assembly watch exit = %d, want %d", got, ExitUnexpected)
	}
}
func TestRunJobWatch_CancelledReturnsNonZero(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "CANCELLED"})
	})
	defer srv.Close()
	if got := runJobWatchWithInterval(c, "job-cancelled", time.Second, time.Millisecond, false); got != ExitUnexpected {
		t.Fatalf("cancelled job watch exit = %d, want %d", got, ExitUnexpected)
	}
}
func TestRunJobMetrics_UsesMetricsEndpoint(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/jobs/job-1/metrics" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"duration_ms": 42})
	})
	defer srv.Close()
	if got := runJob(c, []string{"metrics", "job-1"}); got != ExitOK {
		t.Fatalf("job metrics exit = %d, want %d", got, ExitOK)
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
