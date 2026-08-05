// Package api — Step 1/15 tests for the canonical WorkerCard admin
// surface.
//
// Tests cover, in this order:
//  1. buildWorkerCard mapper (unit): populated, nil, no-executors,
//     no-metrics, omitempty discipline, sensitive-field posture.
//  2. ListAdminWorkers (HTTP): success path, sort stability, nil
//     registry (503), empty registry (200 + empty envelope).
//  3. GetAdminWorker (HTTP): success path, not-found (404), nil
//     registry (503), whitespace-trimmed worker_id semantics.
//
// The test helper `makeCardInfo` produces a fully-populated
// WorkerInfo so we exercise the mapper WITHOUT going through the
// registry.Get → hydrate pipeline on every test — keeping the
// mapper-level tests focused, fast, and deterministic on the same
// fixtures the diagnostic /api/v1/workers tests use.
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	workersreg "velox-server/internal/workers"
	"velox-shared/identity"
)

// makeCardInfo returns a fresh, post-hydration WorkerInfo populated
// with the typical fields the registry produces. Test cases mutate
// the result via `opts` so the happy-path copy stays small.
func makeCardInfo(id string, opts ...func(*workersreg.WorkerInfo)) workersreg.WorkerInfo {
	info := workersreg.WorkerInfo{
		WorkerID:         identity.ParseWorkerID(id),
		WorkerName:       "vps-" + id,
		IPAddress:        "10.0.0.5",
		CodeVersion:      "worker-v1.8.4",
		BundleVersion:    "20260901",
		LastHB:           time.Now().UTC().Format(time.RFC3339),
		ConnectionStatus: "CONNECTED",
		SessionActive:    true,
		Capabilities: map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
				map[string]interface{}{"id": "asset.prepare.v1", "version": float64(2)},
			},
		},
		Metrics: map[string]interface{}{
			"active_tasks": float64(0),
			"task_slots":   float64(2),
		},
	}
	for _, o := range opts {
		o(&info)
	}
	return info
}

// TestBuildWorkerCard_Populated asserts the full mapper happy path
// against a fully-populated WorkerInfo. It is the canonical place
// to extend when a new field lands in the WorkerCard DTO.
func TestBuildWorkerCard_Populated(t *testing.T) {
	// IPAddress override: use "host-10.0.0.5-vm" (a hostname-shaped
	// string with a SUBSTRING IPv4 inside) so the IPv4 REDACTION in
	// sanitiseHostname fires via regex.ReplaceAllString. A whole-
	// string IP like "10.0.0.5" would be caught by the post-regex
	// net.ParseIP defence and produce the [redacted-ip] token via the
	// early-return; that path is exercised independently in
	// TestWorkerCard_SensitiveFieldsDoNotLeak.
	info := makeCardInfo("velox-worker-13197", func(i *workersreg.WorkerInfo) {
		i.IPAddress = "host-10.0.0.5-vm"
	})
	card := buildWorkerCard(&info)

	if card.WorkerID != "velox-worker-13197" {
		t.Errorf("WorkerID = %q, want velox-worker-13197", card.WorkerID)
	}
	// WorkerName "vps-velox-worker-13197" is benign (alphanumeric +
	// dashes) so sanitiseHostname must return it unchanged.
	if card.Hostname != "vps-velox-worker-13197" {
		t.Errorf("Hostname = %q, want vps-velox-worker-13197", card.Hostname)
	}
	// IPAddress "host-10.0.0.5-vm" contains a SUBSTRING IPv4.
	// sanitiseHostname's ipv4RE (anchored with [^0-9] neighbours)
	// catches the dotted quad and replaces it with "[redacted-ipv4]".
	// The whole-string net.ParseIP defence does NOT fire because
	// "host-10.0.0.5-vm" is not a parseable IP.
	//
	// We assert SUBSTRING rather than equality because
	// redactIPv4 uses ReplaceAllString (surrounding context is
	// preserved) so the actual value will be "host-[redacted-ipv4]
	// -vm" — the regex replacement only swaps the matched span.
	// Substring check future-proofs the test against sanitiser
	// tweaks that trim or alter the surrounding context.
	if !strings.Contains(card.Host, "[redacted-ipv4]") {
		t.Errorf("Host = %q, want substring [redacted-ipv4] (sanitiseHostname redacts substring IPv4; surrounding context preserved by ReplaceAllString)", card.Host)
	}
	if card.Status != "CONNECTED" {
		t.Errorf("Status = %q, want CONNECTED", card.Status)
	}
	if !card.SessionActive {
		t.Errorf("SessionActive = false; want true (post-hydration signal)")
	}
	if card.Executor != "scene.composite.v1" {
		t.Errorf("Executor = %q, want scene.composite.v1 (FIRST entry, dispatcher parity)", card.Executor)
	}
	if card.ExecutorVersion != 1 {
		t.Errorf("ExecutorVersion = %d, want 1", card.ExecutorVersion)
	}
	if card.SoftwareVersion != "worker-v1.8.4" {
		t.Errorf("SoftwareVersion = %q, want worker-v1.8.4 (CodeVersion, NOT BundleVersion)", card.SoftwareVersion)
	}
	if card.LastHeartbeatAt == "" {
		t.Errorf("LastHeartbeatAt empty")
	}
	if card.ActiveJobs != 0 {
		t.Errorf("ActiveJobs = %d, want 0", card.ActiveJobs)
	}
	if card.MaxActiveJobs != 2 {
		t.Errorf("MaxActiveJobs = %d, want 2", card.MaxActiveJobs)
	}
	// Empty-until-populated fields: must remain zero. Slice of
	// pairs (NOT a map) so the failure messages stay deterministic
	// on field order — Go's map iteration order is randomised.
	// The JSON-side omitempty discipline is asserted separately in
	// TestWorkerCard_JSON_OmitsEmptyFields.
	for _, c := range []struct {
		field string
		val   string
	}{
		{"ImageDigest", card.ImageDigest},
		{"DesiredVersion", card.DesiredVersion},
		{"Health", card.Health},
		{"DeploymentState", card.DeploymentState},
		{"LastSmokeStatus", card.LastSmokeStatus},
		{"LastSmokeAt", card.LastSmokeAt},
		{"LastRestartAt", card.LastRestartAt},
	} {
		if c.val != "" {
			t.Errorf("%s = %q, want empty (must remain zero until followup commits)", c.field, c.val)
		}
	}
}

// TestBuildWorkerCard_NilInfo asserts the mapper does not panic and
// returns a zero-value card when the registry returns nil.
func TestBuildWorkerCard_NilInfo(t *testing.T) {
	card := buildWorkerCard(nil)
	if card.WorkerID != "" {
		t.Errorf("nil info → WorkerID = %q, want empty", card.WorkerID)
	}
	if card.Status != "" {
		t.Errorf("nil info → Status = %q, want empty", card.Status)
	}
	if card.ExecutorVersion != 0 {
		t.Errorf("nil info → ExecutorVersion = %d, want 0", card.ExecutorVersion)
	}
	if card.SessionActive {
		t.Errorf("nil info → SessionActive = true, want false (zero value)")
	}
}

// TestBuildWorkerCard_NoExecutors asserts graceful default when
// capabilities are nil or carry no "executors" key.
func TestBuildWorkerCard_NoExecutors(t *testing.T) {
	info := makeCardInfo("w-no-exec", func(i *workersreg.WorkerInfo) {
		i.Capabilities = nil
	})
	card := buildWorkerCard(&info)
	if card.Executor != "" {
		t.Errorf("nil Capabilities → Executor = %q, want empty", card.Executor)
	}
	if card.ExecutorVersion != 0 {
		t.Errorf("nil Capabilities → ExecutorVersion = %d, want 0", card.ExecutorVersion)
	}

	info2 := makeCardInfo("w-other-caps", func(i *workersreg.WorkerInfo) {
		i.Capabilities = map[string]interface{}{"other_key": "value"}
	})
	card2 := buildWorkerCard(&info2)
	if card2.Executor != "" || card2.ExecutorVersion != 0 {
		t.Errorf("no 'executors' key → Executor/Version = %q/%d, want empty/0", card2.Executor, card2.ExecutorVersion)
	}
}

// TestBuildWorkerCard_EmptyMetrics asserts default-zero counters
// when the metrics blob is missing. Existing WorkerResponse tests
// cover the same invariant for the diagnostic surface; pinning here
// so the admin card mapper does not regress when a worker omits the
// "metrics" map entirely.
func TestBuildWorkerCard_EmptyMetrics(t *testing.T) {
	info := makeCardInfo("w-no-metrics", func(i *workersreg.WorkerInfo) {
		i.Metrics = nil
	})
	card := buildWorkerCard(&info)
	if card.ActiveJobs != 0 {
		t.Errorf("nil Metrics → ActiveJobs = %d, want 0", card.ActiveJobs)
	}
	if card.MaxActiveJobs != 0 {
		t.Errorf("nil Metrics → MaxActiveJobs = %d, want 0", card.MaxActiveJobs)
	}
}

// TestBuildWorkerCard_BundleVersionNotUsed guards a footgun: the
// mapper must map software_version to CodeVersion (worker-reported),
// NOT BundleVersion (the staging-bundle label). Reverting this would
// make the operator dashboard answer the wrong question.
func TestBuildWorkerCard_BundleVersionNotUsed(t *testing.T) {
	info := makeCardInfo("w-version", func(i *workersreg.WorkerInfo) {
		i.CodeVersion = "worker-v1.8.4"
		i.BundleVersion = "release-2025-09-01-abcdef"
	})
	card := buildWorkerCard(&info)
	if card.SoftwareVersion != "worker-v1.8.4" {
		t.Errorf("SoftwareVersion = %q, want CodeVersion worker-v1.8.4 (NOT BundleVersion)", card.SoftwareVersion)
	}
}

func TestBuildWorkerCard_RuntimeTelemetry(t *testing.T) {
	info := makeCardInfo("w-runtime", func(i *workersreg.WorkerInfo) {
		i.ImageDigest = "sha256:" + strings.Repeat("a", 64)
		i.DesiredVersion = "worker-v1.9.0"
		i.DeploymentState = "CURRENT"
		i.CurrentJob = "job-123"
		i.Metrics = map[string]interface{}{
			"active_tasks":          float64(1),
			"task_slots":            float64(2),
			"cpu_utilization_ratio": 0.75,
			"memory_used_bytes":     float64(1024),
			"disk_free_bytes":       float64(2048),
			"load1":                 1.5,
			"active_jobs":           []interface{}{},
		}
	})
	card := buildWorkerCard(&info)
	if card.ImageDigest != info.ImageDigest || card.DesiredVersion != "worker-v1.9.0" || card.DeploymentState != "CURRENT" {
		t.Fatalf("runtime identity not mapped: %+v", card)
	}
	if card.CPUUtilizationRatio != 0.75 || card.MemoryUsedBytes != 1024 || card.DiskFreeBytes != 2048 || card.Load1 != 1.5 {
		t.Fatalf("runtime metrics not mapped: %+v", card)
	}
	if card.CurrentJob != "job-123" || card.ActiveJobs != 1 || card.MaxActiveJobs != 2 {
		t.Fatalf("job telemetry not mapped: %+v", card)
	}
}

// TestWorkerCard_JSON_OmitsEmptyFields pins the omitempty discipline:
// the empty-default fields MUST NOT appear in the JSON envelope so
// dashboards can render an empty column without render-side
// "null"/"" branching. Re-introducing a field without omitempty would
// break this test.
func TestWorkerCard_JSON_OmitsEmptyFields(t *testing.T) {
	info := makeCardInfo("w-omt")
	card := buildWorkerCard(&info)
	b, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(b)
	notExpected := []string{
		`"image_digest":`,
		`"desired_version":`,
		`"health":`,
		`"deployment_state":`,
		`"last_smoke_status":`,
		`"last_smoke_at":`,
		`"last_restart_at":`,
	}
	for _, k := range notExpected {
		if strings.Contains(js, k) {
			t.Errorf("WorkerCard JSON leaked empty-default field %q: %s", k, js)
		}
	}
	mustHave := []string{
		`"worker_id":`,
		`"hostname":`,
		`"host":`,
		`"status":`,
		`"session_active":`,
		`"executor":`,
		`"executor_version":`,
		`"software_version":`,
		`"last_heartbeat_at":`,
		`"active_jobs":`,
		`"max_active_jobs":`,
	}
	for _, k := range mustHave {
		if !strings.Contains(js, k) {
			t.Errorf("WorkerCard JSON missing required field %q: %s", k, js)
		}
	}
}

// TestWorkerCard_SensitiveFieldsDoNotLeak is the security-posture
// boundary test: even when the WorkerInfo carries sensitive values
// (IPv4 internal, long-hex SHA halves, credential-ish strings), the
// WorkerCard JSON must NOT leak them.
//
// The mapper only reads the whitelisted fields (WorkerID, WorkerName,
// IPAddress, ConnectionStatus, SessionActive, Capabilities[executors],
// CodeVersion, LastHB, Metrics); fields like BootID, HostFingerprint,
// CertificateFingerprint are deliberaTely NOT carried over. The test
// pins BOTH invariants: (1) the mapper does not extract the sensitive
// fields, (2) sanitiseHostname redacts any sensitive content that
// happens to land in the whitelisted WorkerName / IPAddress slots.
func TestWorkerCard_SensitiveFieldsDoNotLeak(t *testing.T) {
	info := workersreg.WorkerInfo{
		WorkerID:               identity.ParseWorkerID("w-leak"),
		WorkerName:             "vps-leak",
		IPAddress:              "192.168.99.99",
		ConnectionStatus:       "CONNECTED",
		LastHB:                 time.Now().UTC().Format(time.RFC3339),
		BootID:                 "boot-secret-xyz",
		HostFingerprint:        "abcdef0123456789abcdef0123456789abcdef01",
		CertificateFingerprint: "deadbeefcafebabe1234567890abcdef0123456789",
	}

	// Defence line 1: the mapper only extracts whitelisted fields
	// (WorkerID, WorkerName, IPAddress, ConnectionStatus,
	// SessionActive, Capabilities[executors], CodeVersion, LastHB,
	// Metrics). BootID / HostFingerprint / CertificateFingerprint
	// never reach the JSON response. Defence line 2: sanitiseHostname
	// would still redact any credential-shaped / IPv4 / long-hex
	// content that lands in WorkerName or IPAddress — covered by
	// TestBuildWorkerCard_PositiveCredentialPathInWorkerName below
	// (ansible-pragmatic-misconfiguration defence-in-depth surface).
	card := buildWorkerCard(&info)
	b, _ := json.Marshal(card)
	js := string(b)

	leakTerms := []string{
		"192.168", "99.99",
		"abcdef0123456789",
		"deadbeef",
		"boot-secret",
		"boot_id",
		"host_fingerprint",
		"certificate_fingerprint",
		"/var/lib/velox/secrets",
	}
	for _, term := range leakTerms {
		if strings.Contains(js, term) {
			t.Errorf("WorkerCard JSON leaked sensitive value %q: %s", term, js)
		}
	}
}

// TestBuildWorkerCard_HealthFieldPropagates pins the Step 3/15
// wire: admin WorkerCard.Health MUST reflect the
// registry-populated WorkerInfo.Health. Uses all 9-state fixtures
// (one sub-test each) so a future regression that drops a state
// from the Health vocabulary surfaces here.
func TestBuildWorkerCard_HealthFieldPropagates(t *testing.T) {
	cases := []struct {
		health string
	}{
		{workersreg.WorkerHealthHealthy},
		{workersreg.WorkerHealthBusy},
		{workersreg.WorkerHealthDraining},
		{workersreg.WorkerHealthUpdating},
		{workersreg.WorkerHealthRestarting},
		{workersreg.WorkerHealthDegraded},
		{workersreg.WorkerHealthOffline},
		{workersreg.WorkerHealthQuarantined},
		{workersreg.WorkerHealthRollback},
	}
	for _, tc := range cases {
		t.Run(tc.health, func(t *testing.T) {
			info := makeCardInfo("w-health", func(i *workersreg.WorkerInfo) {
				i.Health = tc.health
			})
			card := buildWorkerCard(&info)
			if card.Health != tc.health {
				t.Errorf("card.Health = %q, want %q (info.Health propagates unchanged)", card.Health, tc.health)
			}
		})
	}
}

// TestBuildWorkerCard_HealthOmitempty asserts the Step 1/15
// contract: when WorkerInfo.Health is empty, the JSON should
// drop the "health" field (no broken/null leaks to the
// dashboard). Pair with TestWorkerCard_JSON_OmitsEmptyFields.
func TestBuildWorkerCard_HealthOmitempty(t *testing.T) {
	info := makeCardInfo("w-empty-health")
	info.Health = "" // explicit zero
	card := buildWorkerCard(&info)
	if card.Health != "" {
		t.Errorf("expected empty Health for unset WorkerInfo.Health, got %q", card.Health)
	}
	b, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), `"health":`) {
		t.Errorf("WorkerCard JSON leaked empty 'health' field: %s", string(b))
	}
}

// ── HTTP-level tests (gin router) ─────────────────────────────────

// TestListAdminWorkers_Success asserts the list endpoint returns
// 200 OK with sorted worker_id and a Count field that agrees with
// len(workers).
func TestListAdminWorkers_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := workersreg.New(nil) // in-memory only (no SQLite store).
	reg.Heartbeat(nil, "worker-b", "vps-b",		"", nil)
	reg.Heartbeat(nil, "worker-a", "vps-a",		"", nil)

	h := NewAdminWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/admin/workers", h.ListAdminWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp AdminWorkersListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("Count = %d, want 2", resp.Count)
	}
	if len(resp.Workers) != 2 {
		t.Fatalf("len(Workers) = %d, want 2", len(resp.Workers))
	}
	if resp.Workers[0].WorkerID != "worker-a" || resp.Workers[1].WorkerID != "worker-b" {
		t.Errorf("workers not sorted by WorkerID asc: %s", w.Body.String())
	}
}

// TestListAdminWorkers_NilRegistry asserts the 503 path when the
// handler is mounted without a registry. A misconfigured bootstrap
// that mounts the route without a registry must NOT serve 500.
func TestListAdminWorkers_NilRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &AdminWorkersHandler{reg: nil}
	r := gin.New()
	r.GET("/api/v1/admin/workers", h.ListAdminWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

// TestListAdminWorkers_EmptyRegistry asserts the legitimate empty
// fleet state (no workers registered yet returns 200 + empty
// envelope). Operators must NOT interpret a 200 + count=0 as a
// misconfigured server.
func TestListAdminWorkers_EmptyRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewAdminWorkersHandler(workersreg.New(nil))
	r := gin.New()
	r.GET("/api/v1/admin/workers", h.ListAdminWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp AdminWorkersListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("Count = %d, want 0 (empty registry)", resp.Count)
	}
	if len(resp.Workers) != 0 {
		t.Errorf("len(Workers) = %d, want 0", len(resp.Workers))
	}
	// Must NOT be `null` — empty array is the canonical envelope.
	if !strings.Contains(w.Body.String(), `"workers":[]`) {
		t.Errorf("envelope should be JSON null-safe empty array, got: %s", w.Body.String())
	}
}

// TestGetAdminWorker_Success asserts the per-worker endpoint maps
// the registry hydraTed WorkerInfo into the canonical card.
func TestGetAdminWorker_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := workersreg.New(nil)
	reg.Heartbeat(nil, "wicket", "vps-wicket",		"", nil)

	h := NewAdminWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/admin/workers/:worker_id", h.GetAdminWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers/wicket", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var card WorkerCard
	if err := json.Unmarshal(w.Body.Bytes(), &card); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if card.WorkerID != "wicket" {
		t.Errorf("WorkerID = %q, want wicket", card.WorkerID)
	}
	if card.Hostname != "vps-wicket" {
		t.Errorf("Hostname = %q, want vps-wicket", card.Hostname)
	}
}

// TestGetAdminWorker_NotFound asserts the canonical 404 path when
// the worker_id matches no registered worker.
func TestGetAdminWorker_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := NewAdminWorkersHandler(workersreg.New(nil))
	r := gin.New()
	r.GET("/api/v1/admin/workers/:worker_id", h.GetAdminWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers/ghost", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGetAdminWorker_NilRegistry asserts the 503 path when the
// handler is mounted without a registry.
func TestGetAdminWorker_NilRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &AdminWorkersHandler{reg: nil}
	r := gin.New()
	r.GET("/api/v1/admin/workers/:worker_id", h.GetAdminWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers/x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

// TestBuildWorkerCard_FirstExecutorWins pins the canonical flatten
// rule documented in admin_workers_handler.go:buildWorkerCard — when
// a worker advertises multiple executors, the FIRST is the "primary"
// executor that the operator dashboard and the dispatch master agree
// on. A regression that flips this to "last" or concatenates all
// entries would mislead the operator about what the worker is
// currently running.
func TestBuildWorkerCard_FirstExecutorWins(t *testing.T) {
	info := makeCardInfo("w-multi", func(i *workersreg.WorkerInfo) {
		i.Capabilities = map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
				map[string]interface{}{"id": "asset.prepare.v1", "version": float64(7)},
				map[string]interface{}{"id": "voiceover.render.v1", "version": float64(2)},
			},
		}
	})
	card := buildWorkerCard(&info)
	if card.Executor != "scene.composite.v1" {
		t.Errorf("Executor = %q, want scene.composite.v1 (FIRST entry, dispatcher parity)", card.Executor)
	}
	if card.ExecutorVersion != 1 {
		t.Errorf("ExecutorVersion = %d, want 1", card.ExecutorVersion)
	}
}

// TestBuildWorkerCard_PositiveCredentialPathInWorkerName exercises
// the defence-in-depth surface explicitly: when WorkerName carries
// a credential path (the ansible-pragmatic-mistake scenario
// described in workers_handler_test.go:TestSanitiseHostname_Fuzz),
// sanitiseHostname MUST redact it before it lands in the response.
// Pins the operator-misconfiguration boundary test.
func TestBuildWorkerCard_PositiveCredentialPathInWorkerName(t *testing.T) {
	info := makeCardInfo("w-redact", func(i *workersreg.WorkerInfo) {
		i.WorkerName = "/var/lib/velox/secrets/worker-token"
	})
	card := buildWorkerCard(&info)
	if strings.Contains(card.Hostname, "/var/lib/velox/secrets") {
		t.Errorf("Hostname leaked credential path: %q", card.Hostname)
	}
	if card.Hostname != "[redacted-path]" {
		t.Errorf("Hostname = %q, want [redacted-path] (sanitiseHostname must redact credentials)", card.Hostname)
	}
}

// TestListAdminWorkers_EnvelopeShape locks the JSON wrapper to
// {"count":int, "workers":[...]}. A future regression that renames
// workers→fleet or count→total would slip through the typed
// unmarshal assertion below; this test pins the raw byte shape
// directly so the envelope cannot drift silently.
func TestListAdminWorkers_EnvelopeShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reg := workersreg.New(nil)
	reg.Heartbeat(nil, "w-only", "vps-w-only",		"", nil)
	h := NewAdminWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/admin/workers", h.ListAdminWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/workers", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	wantKeys := []string{`"count":1`, `"workers":[{`}
	for _, k := range wantKeys {
		if !strings.Contains(body, k) {
			t.Errorf("JSON envelope missing %q; body=%s", k, body)
		}
	}
	// Two top-level keys, no more: a regression that adds e.g. a
	// `next_page` field would surface here.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("raw unmarshal: %v", err)
	}
	if len(raw) != 2 {
		t.Errorf("envelope top-level keys = %d, want exactly 2 (count, workers); got keys=%v", len(raw), raw)
	}
	if _, ok := raw["count"]; !ok {
		t.Errorf("envelope missing key 'count'")
	}
	if _, ok := raw["workers"]; !ok {
		t.Errorf("envelope missing key 'workers'")
	}
}
