// Package telemetry — RW-PROD-004 §3 A6 health endpoint tests
//
// These tests drive the /health/* endpoints through an httptest
// server backed by the canonical ReadySnapshot. Names are stable
// (TestHealth_{Live,Ready_*,LegacyAdapter_*}) so canary scripts can
// grep them and the audit pattern tests in RW-PROD-004 §6 have a
// stable contract.
package telemetry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// readyHealthServer wraps buildHealthMux behind httptest.NewServer so
// each test starts from a fresh mux with the global ReadyState
// already wiped via t.Cleanup. We use a helper because the mux is
// built once at process start (StartHealthServer binds once); tests
// need a fresh mux per case to assert post-Reset state.
func readyHealthServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(buildHealthMux())
	t.Cleanup(srv.Close)
	return srv
}

// TestHealth_LiveAlive verifies /health/live returns 200 even when
// the canonical ready snapshot is NOT ready (live ≠ ready; that's
// the whole point of splitting).
func TestHealth_LiveAlive(t *testing.T) {
	t.Cleanup(ResetForTest)
	// Intentionally leave ReadyState un-plumbed.
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	var out LiveResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "alive" {
		t.Fatalf("expected status=alive; got %q", out.Status)
	}
	if out.UptimeSec < 0 {
		t.Fatalf("uptime_sec must be non-negative; got %d", out.UptimeSec)
	}
}

// TestHealth_ReadyAfterHello_AllOK drives every ready gate to OK and
// asserts /health/ready returns 200 with status=ok and ZERO reasons.
func TestHealth_ReadyAfterHello_AllOK(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)
	SetHealthWorkerID("test-worker-ready")
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	var out ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != "ok" {
		t.Fatalf("expected status=ok; got %q", out.Status)
	}
	if len(out.Reasons) != 0 {
		t.Fatalf("expected zero reasons; got %v", out.Reasons)
	}
	if !out.Detail["registered"].(bool) {
		t.Fatalf("registered detail missing/incorrect: %v", out.Detail)
	}
	if !out.Detail["bootstrapped"].(bool) {
		t.Fatalf("bootstrapped detail missing/incorrect: %v", out.Detail)
	}
}

// TestHealth_ReadyDuringDrain_NotReady verifies a worker that has
// satisfied every other readiness precondition is still NOT ready
// under drain_mode (per RW-PROD-004 §3 Criteri 2).
func TestHealth_ReadyDuringDrain_NotReady(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)
	MarkDrainMode(true)
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503; got %d", resp.StatusCode)
	}
	var out ReadyResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	hasDrain := false
	for _, r := range out.Reasons {
		if r == "drain_mode" {
			hasDrain = true
		}
	}
	if !hasDrain {
		t.Fatalf("expected drain_mode reason; got %v", out.Reasons)
	}
	if out.Detail["drain_mode"].(bool) != true {
		t.Fatalf("drain_mode detail must be true: %v", out.Detail)
	}
}

// TestHealth_ReadyNoExecutors_NotReady verifies an empty executor
// registry drives the executors.empty reason (RW-PROD-004 §3
// Criteri 3). Composition-root misconfiguration path.
func TestHealth_ReadyNoExecutors_NotReady(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	// ExecutorsCount intentionally NOT set — defaults to 0.
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503; got %d", resp.StatusCode)
	}
	var out ReadyResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	hasEmpty := false
	for _, r := range out.Reasons {
		if r == "executors.empty" {
			hasEmpty = true
		}
	}
	if !hasEmpty {
		t.Fatalf("expected executors.empty reason; got %v", out.Reasons)
	}
}

// TestHealth_ReadyUnderDiskPressure_NotReady verifies the disk-watch
// signal flows through to /health/ready (RW-PROD-004 §3 Criteri 4).
func TestHealth_ReadyUnderDiskPressure_NotReady(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	// Threshold 1 GiB, free 100 MiB — critical.
	SetDiskState(100*1024*1024, 1<<30)
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503; got %d", resp.StatusCode)
	}
	var out ReadyResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	hasCritical := false
	for _, r := range out.Reasons {
		if r == "disk.critical" {
			hasCritical = true
		}
	}
	if !hasCritical {
		t.Fatalf("expected disk.critical reason; got %v", out.Reasons)
	}
	if v, ok := out.Detail["disk_free_bytes"].(float64); !ok || int64(v) != 100*1024*1024 {
		t.Fatalf("disk_free_bytes detail must be 100MiB; got %v", out.Detail["disk_free_bytes"])
	}
}

// TestHealth_LegacyAdapter_DeprecationHeader verifies the legacy
// /health endpoint proxies the ready verdict AND emits the
// X-Velox-Health-Deprecated header (RW-PROD-004 §3 A1).
func TestHealth_LegacyAdapter_DeprecationHeader(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(1)
	SetDiskState(1<<30, 256*1024*1024)
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("X-Velox-Health-Deprecated"); !strings.Contains(got, "/health/live") {
		t.Fatalf("legacy adapter must emit X-Velox-Health-Deprecated pointing at /health/live; got %q", got)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 (mirrors ready verdict); got %d", resp.StatusCode)
	}
	var out HealthResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Status != "ok" {
		t.Fatalf("legacy adapter body must mirror ready verdict; got %q", out.Status)
	}
}

// TestHealth_LegacyAdapter_DowngradeToNotReady verifies the legacy
// /health endpoint reflects not-ready from the canonical snapshot
// (status 503). This is the back-compat guarantee — pre-existing
// Docker HEALTHCHECK probes keep working unchanged while surfacing
// the canonical readiness reading.
func TestHealth_LegacyAdapter_DowngradeToNotReady(t *testing.T) {
	t.Cleanup(ResetForTest)
	MarkRegistered(true)
	MarkBootstrapped(true)
	MarkCacheReady(true)
	MarkBlobReady(true)
	SetExecutorsCount(0) // intentionally triggers executors.empty
	srv := readyHealthServer(t)

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 with executors.empty; got %d", resp.StatusCode)
	}
}

// TestHealth_ReadyMethodNotAllowed verifies method gating on /health/ready
// (POST/PUT/DELETE return 405). Out-of-scope for production but
// prevents screwups where a non-GET probe path silently 404s.
func TestHealth_ReadyMethodNotAllowed(t *testing.T) {
	t.Cleanup(ResetForTest)
	srv := readyHealthServer(t)

	resp, err := http.Post(srv.URL+"/health/ready", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST; got %d", resp.StatusCode)
	}
}
