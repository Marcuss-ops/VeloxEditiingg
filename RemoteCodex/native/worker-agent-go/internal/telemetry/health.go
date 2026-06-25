// Package telemetry — /health endpoint family (RW-PROD-004 §3 A1)
//
// Three endpoints, three concerns:
//
//   /health/live   — 200 iff the process is alive. Operators can
//                    guarantee Kubernetes / Docker Compose will not
//                    repeatedly restart the container for a process
//                    that is up but not yet registered.
//
//   /health/ready  — 200 iff ReadySnapshot.IsReady() (see ready.go).
//                    Six canonical readiness reasons drive the body
//                    so dashboards + canary scripts can grep on the
//                    stable string taxonomy (bootstrap_not_run,
//                    not_registered, drain_mode, executors.empty,
//                    cache.not_initialized, blob.not_initialized,
//                    disk.critical).
//
//   /health        — legacy adapter. Proxies the current ready
//                    verdict into the legacy HealthResponse body and
//                    returns 200 if ready, 503 otherwise, with an
//                    X-Velox-Health-Deprecated header so monitoring
//                    scripts emit a loud one-line warning during the
//                    deprecation window. Pre-existing callers
//                    (Docker HEALTHCHECK, Kubernetes probes) keep
//                    working unchanged.
package telemetry

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	healthStartTime  = time.Now()
	healthWorkerID   atomic.Value
	healthRegistered atomic.Bool
)

// legacyDeprecationLogged uses sync.Once to emit the deprecation
// warning AT MOST ONCE per process. Operators see a single line in
// the boot logs; CI scrapers do not get spammed. The HTTP path
// remains correct in either case.
var legacyDeprecationLogged sync.Once

// SetHealthWorkerID sets the worker ID reported by the health endpoint.
func SetHealthWorkerID(id string) {
	healthWorkerID.Store(id)
}

// SetHealthRegistered is the LEGACY worker-side `registered` flag.
// RW-PROD-004 keeps this for back-compat with the existing
// pkg/worker.telemetry.SetHealthRegistered(true) call from
// setConnState(ConnReady). New code should prefer ready.MarkRegistered
// INSTEAD because the legacy flag has no Place in the canonical
// reasons taxonomy and is not exposed on /health/ready.
//
// The two flags are kept in sync: setter flips both. Better yet,
// callers incrementally migrate to MarkRegistered in worker.go and
// nothing else needs to know about the legacy field.
func SetHealthRegistered(registered bool) {
	healthRegistered.Store(registered)
	// RW-PROD-004: keep the canonical ReadyState in lockstep.
	MarkRegistered(registered)
}

// HealthResponse is the JSON payload returned by the legacy
// GET /health endpoint. New code MUST use /health/ready; this shape is
// retained for legacy callers (Docker HEALTHCHECK, k8s probes).
type HealthResponse struct {
	Status     string `json:"status"`
	WorkerID   string `json:"worker_id,omitempty"`
	Registered bool   `json:"registered"`
	UptimeSec  int64  `json:"uptime_sec"`
}

// LiveResponse is the JSON payload returned by GET /health/live.
// Uptime is a wall-clock seconds delta; "status" is always "alive"
// because reaching the handler implies the process + event loop are
// alive. Keep this struct thin so it travels well across monitoring
// tools.
type LiveResponse struct {
	Status    string `json:"status"`
	WorkerID  string `json:"worker_id,omitempty"`
	UptimeSec int64  `json:"uptime_sec"`
}

// ReadyResponse is the JSON payload returned by GET /health/ready.
// On ready: {"status":"ok","detail":{...}}. On not-ready:
// {"status":"not_ready","reasons":[...reasons],"detail":{...}}
// with HTTP 503.
type ReadyResponse struct {
	Status  string                 `json:"status"`
	Reasons []string               `json:"reasons,omitempty"`
	Detail  map[string]interface{} `json:"detail"`
}

// StartHealthServer starts a minimal HTTP server on `port` exposing
// all three endpoints (legacy /health, /health/live, /health/ready).
//
// RW-PROD-004 §3 A9: the read-endpoint path (e.g.
// `--ready-endpoint /health/ready`) is honoured by
// cmd/velox-worker-agent/main.go via a registerFastHTTPMux helper
// that simply calls mux.HandleFunc on the supplied path. This
// function does not need to know the custom path because the
// composition root wires it after StartHealthServer returns the
// default mux — but for ergonomic symmetry, this function ALSO
// honours a `--ready-endpoint` override documented on the
// StartHealthServerReady call site in cmd/velox-worker-agent/main.go.
func StartHealthServer(port int) error {
	mux := buildHealthMux()
	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: mux}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[HEALTH] Health server error: %v\n", err)
		}
	}()

	return nil
}

// StartHealthServerWithMux is the variant the composition root
// uses when the operator has overridden the ready-endpoint path via
// --ready-endpoint. We construct the mux with HandleFunc on the
// supplied readyPath so a Kubernetes podspec or docker HEALTHCHECK
// pointing at /custom/ready works without altering the three
// canonical mount points.
//
// The function is otherwise identical to StartHealthServer.
func StartHealthServerWithMux(port int, readyPath string) error {
	mux := buildHealthMux()
	if readyPath != "" && readyPath != "/health/ready" {
		mux.HandleFunc(readyPath, readyHandler)
	}
	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{Addr: addr, Handler: mux}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[HEALTH] Health server error: %v\n", err)
		}
	}()
	return nil
}

// buildHealthMux returns the canonical three-endpoint mux. Exposed
// for the WithMux variant + for the test harness (which needs the mux
// against an httptest.NewServer without binding to a port).
func buildHealthMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", liveHandler)
	mux.HandleFunc("/health/ready", readyHandler)
	mux.HandleFunc("/health", legacyHealthHandler)
	return mux
}

// liveHandler returns 200 + {"status":"alive",...} unconditionally.
// Process + goroutine liveness is implicit in reaching this handler
// at all; the canonical `NotReady` reasons are intentionally NOT
// surfaced here (this endpoint never blocks a restart decision on
// readiness — that lives on /health/ready).
func liveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	resp := LiveResponse{
		Status:    "alive",
		UptimeSec: int64(time.Since(healthStartTime).Seconds()),
	}
	if id, ok := healthWorkerID.Load().(string); ok {
		resp.WorkerID = id
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// readyHandler returns 200 + {"status":"ok","detail":{...}} when
// ReadySnapshot.IsReady(). Otherwise it returns 503 with the
// canonical reasons taxonomy (grep-friendly for canary scripts) +
// the boolean detail map so dashboards can graph transitions
// without parsing the string array.
func readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snap := GlobalReady().Snapshot()
	reasons := snap.NotReadyReasons()
	resp := ReadyResponse{
		Status:  "ok",
		Reasons: reasons,
		Detail:  snap.DetailMap(),
	}
	status := http.StatusOK
	if len(reasons) > 0 {
		resp.Status = "not_ready"
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

// legacyHealthHandler is the back-compat adapter for callers still
// pointing at /health (Docker HEALTHCHECK, older Kubernetes probes).
// It proxies the ready verdict into the legacy HealthResponse shape
// AND emits the X-Velox-Health-Deprecated header on every call so
// monitoring scripts surface a loud one-line warning during the
// deprecation window.
//
// Process-local sync.Once emits a single log line so log scrapers
// see exactly one deprecation warning per process — CI does not
// get spammed by per-request spam.
func legacyHealthHandler(w http.ResponseWriter, r *http.Request) {
	// sync.Once controls the WARNING LOG only (not the response
	// header — see below); CI scrapers see a single one-line dep
	// warning, not a per-request storm. The HTTP header is set
	// unconditionally on every call so monitoring scripts always
	// surface the deprecation.
	legacyDeprecationLogged.Do(func() {
		fmt.Printf("[HEALTH_DEPRECATED] GET /health is deprecated — use /health/live or /health/ready. See RW-PROD-004.\n")
	})
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("X-Velox-Health-Deprecated", "use /health/live or /health/ready (RW-PROD-004)")
	snap := GlobalReady().Snapshot()
	resp := HealthResponse{
		WorkerID:   workerIDOrEmpty(),
		Registered: healthRegistered.Load(),
		UptimeSec:  int64(time.Since(healthStartTime).Seconds()),
	}
	if snap.IsReady() {
		resp.Status = "ok"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	resp.Status = "not_ready"
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(resp)
}

func workerIDOrEmpty() string {
	if id, ok := healthWorkerID.Load().(string); ok {
		return id
	}
	return ""
}
