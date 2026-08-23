package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

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
	raw := strings.Repeat("b", 64)
	if got := digestFromRef(raw); got != "sha256:"+raw {
		t.Fatalf("digestFromRef raw digest = %q, want sha256-prefixed digest", got)
	}
}
func TestRunStatusProduction_DriftReturnsNonZero(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(workerListResponse{Count: 1, Workers: []map[string]any{{
			"worker_id": "worker-1", "worker_name": "velox_worker_1",
			"image_state": map[string]any{
				"target_digest":  "ghcr.io/example/velox-worker@" + digest,
				"running_digest": "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("b", 64),
				"digest_match":   false,
			},
			"image_digest": "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("b", 64),
		}}})
	})
	defer srv.Close()
	if got := runStatusMode(c, true); got == ExitOK {
		t.Fatal("production status must fail on digest drift")
	}
}
func TestRunStatusProduction_ImageStateTargetMatchesDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(workerListResponse{Count: 1, Workers: []map[string]any{{
			"worker_id": "worker-1", "worker_name": "velox_worker_1",
			"image_state": map[string]any{
				"target_digest":  "ghcr.io/example/velox-worker@" + digest,
				"running_digest": "ghcr.io/example/velox-worker@" + digest,
				"digest_match":   true,
			},
			"image_digest": "ghcr.io/example/velox-worker@" + digest,
		}}})
	})
	defer srv.Close()
	if got := runStatusMode(c, true); got != ExitOK {
		t.Fatalf("production status with matching image_state must pass, got exit %d", got)
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
func TestRunInspect_PrintsImageAndOperationSections(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers/w-sec" {
			http.Error(w, "wrong path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": "w-sec", "worker_name": "velox-worker-01",
			"status": "CONNECTED", "health": "HEALTHY",
			"executor": "scene.composite.v1", "executor_version": "1",
			"active_jobs": 0.0, "max_active_jobs": 1.0,
			"image_digest": "sha256:" + strings.Repeat("a", 64),
			"image_state": map[string]any{
				"running_digest": "sha256:" + strings.Repeat("a", 64),
				"target_digest":  "sha256:" + strings.Repeat("a", 64),
				"digest_match":   true,
			},
			"operation_state": map[string]any{
				"operation_id": "deploy-1", "type": "update",
				"status": "FAILED", "error": "connection reset by peer",
				"started_at": "2026-08-11T12:00:00Z", "finished_at": "2026-08-11T12:05:00Z",
			},
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runInspect(c, []string{"w-sec"})
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK {
		t.Fatalf("inspect exit = %d, want %d", ec, ExitOK)
	}
	for _, want := range []string{
		"IMAGE",
		"running_digest = sha256:",
		"target_digest  = sha256:",
		"digest_match   = true",
		"LAST UPDATE OPERATION",
		"status       = FAILED",
		"reason       = connection reset by peer",
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("inspect output missing %q; got:\n%s", want, out)
		}
	}
}
func TestRunInspect_JSONFlagEmitsRawCard(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers/w-json" {
			http.Error(w, "wrong path: "+r.URL.Path, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"worker_id": "w-json", "status": "CONNECTED",
			"image_digest": "sha256:" + strings.Repeat("a", 64),
		})
	})
	defer srv.Close()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	ec := runInspect(c, []string{"--json", "w-json"})
	w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)

	if ec != ExitOK {
		t.Fatalf("inspect --json exit = %d, want %d", ec, ExitOK)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("inspect --json must emit a raw card JSON: %v; output=%s", err, out)
	}
	if parsed["worker_id"] != "w-json" {
		t.Fatalf("raw card worker_id = %v, want w-json", parsed["worker_id"])
	}
}
