package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestResolveRolloutWorkers_AllReadsInventory(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/workers" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(workerListResponse{
			Count: 2,
			Workers: []map[string]any{
				{"worker_id": "velox-worker-13197"},
				{"worker_id": "velox-worker-523925eb"},
			},
		})
	})
	defer srv.Close()

	workers, err := resolveRolloutWorkers(c, "all")
	if err != nil {
		t.Fatalf("resolve all: %v", err)
	}
	if len(workers) != 2 || workers[0] != "velox-worker-13197" || workers[1] != "velox-worker-523925eb" {
		t.Fatalf("resolved workers = %v", workers)
	}
}
func TestResolveRolloutWorkers_CommaListAppliedVerbatim(t *testing.T) {
	workers, err := resolveRolloutWorkers(nil, "worker-1, worker-2,worker-3")
	if err != nil {
		t.Fatalf("resolve list: %v", err)
	}
	want := []string{"worker-1", "worker-2", "worker-3"}
	if len(workers) != len(want) {
		t.Fatalf("resolved workers = %v, want %v", workers, want)
	}
	for i := range want {
		if workers[i] != want[i] {
			t.Fatalf("resolved workers = %v, want %v", workers, want)
		}
	}
}
func TestRunRollout_SerialUpdateStopsAtFailure(t *testing.T) {
	pinned := "ghcr.io/example/velox-worker@sha256:" + strings.Repeat("a", 64)
	var updateCalls, pollCalls int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /api/v1/admin/workers":
			_ = json.NewEncoder(w).Encode(workerListResponse{
				Count: 2,
				Workers: []map[string]any{
					{"worker_id": "worker-ok"},
					{"worker_id": "worker-fail"},
				},
			})
		case "POST /api/v1/admin/workers/worker-ok/update":
			updateCalls++
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-ok","worker_id":"worker-ok"}`))
		case "GET /api/v1/admin/operations/op-ok":
			pollCalls++
			_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-ok", Status: "SUCCEEDED"})
		case "POST /api/v1/admin/workers/worker-fail/update":
			updateCalls++
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-fail","worker_id":"worker-fail"}`))
		case "GET /api/v1/admin/operations/op-fail":
			pollCalls++
			_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-fail", Status: "FAILED", ErrorMessage: "activation failed"})
		default:
			http.Error(w, "unexpected rollout request", http.StatusNotFound)
		}
	})
	defer srv.Close()

	ec := runRollout(c, []string{"--digest", pinned, "--workers", "all"})
	if ec == ExitOK {
		t.Fatal("rollout with a failing worker must exit non-zero")
	}
	if updateCalls != 2 || pollCalls != 2 {
		t.Fatalf("rollout calls = updates %d polls %d, want both workers updated and polled", updateCalls, pollCalls)
	}
}
