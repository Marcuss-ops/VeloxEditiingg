package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestRunWorkerConfig_ReachesMasterWithFMP4Toggle pins the operator-facing
// rollout command for the fMP4 admission gate: fleetctl must put the toggle on
// the audited config operation instead of asking anyone to edit worker.env.
func TestRunWorkerConfig_ReachesMasterWithFMP4Toggle(t *testing.T) {
	var postPath string
	var postBody map[string]any
	var pollCount int
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/admin/workers/worker-1/config":
			postPath = r.URL.Path
			if err := json.NewDecoder(r.Body).Decode(&postBody); err != nil {
				t.Errorf("decode config body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-config-1","worker_id":"worker-1","queued_at":"now"}`))
		case "GET /api/v1/admin/operations/op-config-1":
			pollCount++
			_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-config-1", WorkerID: "worker-1", Status: "SUCCEEDED"})
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	defer srv.Close()

	ec := runWorkerConfig(c, []string{"set", "worker-1", "--fmp4-stream-profile", "1"})
	if ec != ExitOK {
		t.Fatalf("worker-config exit code = %d, want %d", ec, ExitOK)
	}
	if postPath != "/api/v1/admin/workers/worker-1/config" || pollCount != 1 {
		t.Fatalf("requests = path %q polls %d; want config path and one terminal poll", postPath, pollCount)
	}
	if got, ok := postBody["fmp4_stream_profile"].(float64); !ok || got != 1 {
		t.Fatalf("body fmp4_stream_profile = %v, want 1", postBody["fmp4_stream_profile"])
	}
}

// TestRunWorkerConfig_FMP4ToggleAcceptsEqualsFormAndSharedKnobs keeps the flag
// forms consistent with the audio-mix knobs it rides beside.
func TestRunWorkerConfig_FMP4ToggleAcceptsEqualsFormAndSharedKnobs(t *testing.T) {
	var postBody map[string]any
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/admin/workers/worker-1/config":
			if err := json.NewDecoder(r.Body).Decode(&postBody); err != nil {
				t.Errorf("decode config body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation_id":"op-config-2","worker_id":"worker-1"}`))
		case "GET /api/v1/admin/operations/op-config-2":
			_ = json.NewEncoder(w).Encode(polledOperationRow{OperationID: "op-config-2", Status: "SUCCEEDED"})
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	defer srv.Close()

	ec := runWorkerConfig(c, []string{"set", "worker-1", "--fmp4-stream-profile=0", "--audio-mix-strategy", "optimized"})
	if ec != ExitOK {
		t.Fatalf("worker-config exit code = %d, want %d", ec, ExitOK)
	}
	if got, ok := postBody["fmp4_stream_profile"].(float64); !ok || got != 0 {
		t.Fatalf("body fmp4_stream_profile = %v, want explicit 0", postBody["fmp4_stream_profile"])
	}
	if got, _ := postBody["audio_mix_strategy"].(string); got != "optimized" {
		t.Fatalf("body audio_mix_strategy = %q, want optimized", got)
	}
}

// TestRunWorkerConfig_RejectsInvalidFMP4Toggle keeps a bad toggle from ever
// reaching the Master.
func TestRunWorkerConfig_RejectsInvalidFMP4Toggle(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected HTTP call for rejected toggle: %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	})
	defer srv.Close()

	ec := runWorkerConfig(c, []string{"set", "worker-1", "--fmp4-stream-profile", "2"})
	if ec != ExitMisuse {
		t.Fatalf("exit code = %d, want ExitMisuse (%d)", ec, ExitMisuse)
	}
}

// TestRunWorkerConfig_RequiresAtLeastOneSetting keeps the empty invocation a
// misuse error rather than a silent restart.
func TestRunWorkerConfig_RequiresAtLeastOneSetting(t *testing.T) {
	c, srv := newMockClient(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unexpected", http.StatusInternalServerError)
	})
	defer srv.Close()

	ec := runWorkerConfig(c, []string{"set", "worker-1"})
	if ec != ExitMisuse {
		t.Fatalf("exit code = %d, want ExitMisuse (%d)", ec, ExitMisuse)
	}
}

// TestWorkerConfigUsageMentionsFMP4Toggle keeps the help text honest about the
// gate rollout knob.
func TestWorkerConfigUsageMentionsFMP4Toggle(t *testing.T) {
	if !strings.Contains(usage, "--fmp4-stream-profile") {
		t.Fatalf("usage text does not mention --fmp4-stream-profile:\n%s", usage)
	}
}
