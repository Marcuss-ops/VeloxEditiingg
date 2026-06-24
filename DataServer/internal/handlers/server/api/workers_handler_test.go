package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

func TestHeartbeatAgeSeconds(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-10 * time.Second).Format(time.RFC3339)

	if got := heartbeatAgeSeconds(recent); got < 9 || got > 11 {
		t.Errorf("heartbeatAgeSeconds(recent 10s) = %d, want ~10", got)
	}
	if got := heartbeatAgeSeconds(""); got != 0 {
		t.Errorf("heartbeatAgeSeconds(empty) = %d, want 0", got)
	}
	if got := heartbeatAgeSeconds("bogus"); got != 0 {
		t.Errorf("heartbeatAgeSeconds(bogus) = %d, want 0", got)
	}
}

func TestSanitizeWorker(t *testing.T) {
	now := time.Now().UTC()
	recent := now.Add(-3 * time.Second).Format(time.RFC3339)
	firstSeen := now.Add(-1 * time.Hour).Format(time.RFC3339)

	raw := workersreg.WorkerInfo{
		WorkerID:        "worker-abc",
		WorkerName:      "render-node-1",
		// Post-hydration ConnectionStatus — sanitizeWorker trusts this
		// directly. The canonical derivation is `workers.ConnectionStatus`
		// (called via `ConnectionStatusForInfo` from `hydrate`/`hydrateBulk`
		// in `registry_query.go`).
		ConnectionStatus: "CONNECTED",
		LastHB:           recent,
		FirstSeen:        firstSeen,
		CurrentJob:       "task-456",
		Drain:            false,
		Schedulable:      true,
		Host:             "render-01.example.com",
		ProtocolVersion:  "v3",
		EngineVersion:    "1.2.0",
		BundleVersion:    "20260621",
		// These fields MUST be excluded from the response DTO.
		IPAddress: "10.0.0.5",
		BootID:    "boot-secret-123",
		Metrics: map[string]interface{}{
			"active_tasks":          float64(1),
			"task_slots":            float64(3),
			"cpu_utilization_ratio": float64(0.84),
			"memory_used_bytes":     float64(4294967296),
			"disk_free_bytes":       float64(120000000000),
			"jobs_completed":        float64(42),
			"jobs_failed":           float64(2),
		},
		Capabilities: map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
				map[string]interface{}{"id": "asset.prepare.v1", "version": float64(1)},
			},
		},
	}

	resp := sanitizeWorker(raw)

	// Identity
	if resp.WorkerID != "worker-abc" {
		t.Errorf("WorkerID = %q, want worker-abc", resp.WorkerID)
	}
	if resp.WorkerName != "render-node-1" {
		t.Errorf("WorkerName = %q, want render-node-1", resp.WorkerName)
	}
	if resp.Hostname != "render-01.example.com" {
		t.Errorf("Hostname = %q, want render-01.example.com", resp.Hostname)
	}

	// Status (recent HB, no drain → CONNECTED)
	if resp.Status != "CONNECTED" {
		t.Errorf("Status = %q, want CONNECTED", resp.Status)
	}

	// Version info
	if resp.ProtocolVersion != "v3" {
		t.Errorf("ProtocolVersion = %q, want v3", resp.ProtocolVersion)
	}
	if resp.EngineVersion != "1.2.0" {
		t.Errorf("EngineVersion = %q, want 1.2.0", resp.EngineVersion)
	}
	if resp.BundleVersion != "20260621" {
		t.Errorf("BundleVersion = %q, want 20260621", resp.BundleVersion)
	}

	// Timestamps
	if resp.ConnectedAt != firstSeen {
		t.Errorf("ConnectedAt = %q, want %q", resp.ConnectedAt, firstSeen)
	}
	if resp.LastHeartbeatAt != recent {
		t.Errorf("LastHeartbeatAt = %q, want %q", resp.LastHeartbeatAt, recent)
	}
	if resp.HeartbeatAgeSeconds < 2 || resp.HeartbeatAgeSeconds > 5 {
		t.Errorf("HeartbeatAgeSeconds ~ %d, want ~3", resp.HeartbeatAgeSeconds)
	}

	// Task info
	if resp.CurrentTaskID != "task-456" {
		t.Errorf("CurrentTaskID = %q, want task-456", resp.CurrentTaskID)
	}

	// Resource counters
	if resp.ActiveTasks != 1 {
		t.Errorf("ActiveTasks = %d, want 1", resp.ActiveTasks)
	}
	if resp.TaskSlots != 3 {
		t.Errorf("TaskSlots = %d, want 3", resp.TaskSlots)
	}
	if resp.CPUUtilizationRatio != 0.84 {
		t.Errorf("CPUUtilizationRatio = %f, want 0.84", resp.CPUUtilizationRatio)
	}
	if resp.MemoryUsedBytes != 4294967296 {
		t.Errorf("MemoryUsedBytes = %d, want 4294967296", resp.MemoryUsedBytes)
	}
	if resp.DiskFreeBytes != 120000000000 {
		t.Errorf("DiskFreeBytes = %d, want 120000000000", resp.DiskFreeBytes)
	}
	if resp.JobsCompleted != 42 {
		t.Errorf("JobsCompleted = %d, want 42", resp.JobsCompleted)
	}
	if resp.JobsFailed != 2 {
		t.Errorf("JobsFailed = %d, want 2", resp.JobsFailed)
	}

	// Executors
	if len(resp.Executors) != 2 {
		t.Fatalf("Executors len = %d, want 2", len(resp.Executors))
	}
	if resp.Executors[0].ID != "scene.composite.v1" || resp.Executors[0].Version != 1 {
		t.Errorf("Executors[0] = %+v, want scene.composite.v1@1", resp.Executors[0])
	}

	// --- Negative assertions: sensitive fields must NOT leak ---
	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	jsonStr := string(jsonBytes)
	sensitiveTerms := []string{
		"10.0.0.5", "ip_address",
		"boot-secret", "boot_id",
		"credential", "tls_cert", "tls_key", "tls_ca",
		"secret",
	}
	for _, term := range sensitiveTerms {
		if contains(jsonStr, term) {
			t.Errorf("WorkerResponse JSON leaks sensitive field %q", term)
		}
	}
}

func TestSanitizeWorker_Draining(t *testing.T) {
	// Post-hydration ConnectionStatus — sanitizeWorker trusts this
	// directly. The canonical derivation is `workers.ConnectionStatus`
	// (called via `ConnectionStatusForInfo` from `hydrate`/`hydrateBulk`
	// in `registry_query.go`).
	raw := workersreg.WorkerInfo{
		WorkerID:         "w-drain",
		LastHB:           time.Now().UTC().Format(time.RFC3339),
		Drain:            true,
		ConnectionStatus: "DRAINING",
	}
	resp := sanitizeWorker(raw)
	if resp.Status != "DRAINING" {
		t.Errorf("Status = %q, want DRAINING", resp.Status)
	}
}

// TestSanitizeWorker_NoDerivationInvariant pins the no-derivation
// contract: sanitizeWorker trusts WorkerInfo.ConnectionStatus directly,
// it MUST NOT derive status from heartbeat+drain.
//
// Exhaustive 4-state derivation coverage lives in TestConnectionStatus_*
// in `worker_info_test.go`; this test exists only to pin the handler-
// boundary invariant. If it ever fires, an inspector just re-introduced
// a heartbeat/drain derivation inside sanitizeWorker.
//
//   subcase                legacy fallback (now disallowed)  → required resp.Status
//   ────────────────────────────────────────────────────────────────────────────
//   Drain=true             "DRAINING"  (drain rank)            → ""
//   Drain=false + recent HB "CONNECTED" (recent-heartbeat branch) → ""
//
// A regression re-introducing the deleted
// `if resp.Status == "" { resp.Status = computeStatusLegacy(...) }`
// block inside sanitizeWorker would fail both subcases.
func TestSanitizeWorker_NoDerivationInvariant(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	cases := []struct {
		name string
		raw  workersreg.WorkerInfo
	}{
		{
			// Subcase name carries the legacy-fallback pattern so CI `-v`
			// output names the regression shape even without reading the
			// table comment above.
			"drain=true (legacy: DRAINING; new: empty)",
			workersreg.WorkerInfo{WorkerID: "w-noderive-1", LastHB: now, Drain: true},
		},
		{
			"drain=false, recent HB (legacy: CONNECTED; new: empty)",
			workersreg.WorkerInfo{WorkerID: "w-noderive-2", LastHB: now, Drain: false},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := sanitizeWorker(tc.raw)
			if resp.Status != "" {
				t.Errorf("resp.Status = %q, want \"\" (sanitizeWorker must trust ConnectionStatus; consult workers.ConnectionStatus for canonical derivation)", resp.Status)
			}
		})
	}
}

func TestSanitizeWorker_NilMaps(t *testing.T) {
	raw := workersreg.WorkerInfo{
		WorkerID: "w-nomaps",
		LastHB:   time.Now().UTC().Format(time.RFC3339),
	}
	resp := sanitizeWorker(raw)
	if resp.ActiveTasks != 0 || resp.TaskSlots != 0 || resp.CPUUtilizationRatio != 0 {
		t.Errorf("expected zero counters for nil maps, got active_tasks=%d task_slots=%d cpu=%.2f",
			resp.ActiveTasks, resp.TaskSlots, resp.CPUUtilizationRatio)
	}
	if len(resp.Executors) != 0 {
		t.Errorf("expected no executors for nil capabilities, got %d", len(resp.Executors))
	}
}

func TestSanitizeWorker_SessionActiveSurfacesInJSON(t *testing.T) {
	// PR review fix: SessionActive MUST serialize on the JSON response so
	// dashboards can distinguish session_active=false (offline) from
	// "field missing (legacy client)". ConnectionStatus omitempty is
	// preserved for the legacy/fallback path; here we set it explicitly.
	cases := []struct {
		name             string
		sessionActive    bool
		connectionStatus string
		wantFieldTrue    bool
	}{
		{"online worker", true, "CONNECTED", true},
		{"session dropped (heartbeat still fresh)", false, "DISCONNECTED", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recent := time.Now().UTC().Format(time.RFC3339)
			raw := workersreg.WorkerInfo{
				WorkerID:           "worker-conn",
				LastHB:             recent,
				SessionActive:      tc.sessionActive,
				ConnectionStatus:   tc.connectionStatus,
			}
			resp := sanitizeWorker(raw)
			if resp.SessionActive != tc.wantFieldTrue {
				t.Errorf("resp.SessionActive = %v, want %v", resp.SessionActive, tc.wantFieldTrue)
			}
			if resp.Status != tc.connectionStatus {
				t.Errorf("resp.Status = %q, want %q", resp.Status, tc.connectionStatus)
			}
			b, err := json.Marshal(resp)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			jsonStr := string(b)
			if tc.wantFieldTrue && !contains(jsonStr, "\"session_active\":true") {
				t.Errorf("JSON lost session_active=true: %s", jsonStr)
			}
			if !tc.wantFieldTrue && !contains(jsonStr, "\"session_active\":false") {
				t.Errorf("JSON lost session_active=false: %s", jsonStr)
			}
			if !contains(jsonStr, "\"status\":\""+tc.connectionStatus+"\"") {
				t.Errorf("JSON lost status=%s; got %s", tc.connectionStatus, jsonStr)
			}
		})
	}
}

func TestListWorkers_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := workersreg.New(nil) // nil store = in-memory only
	reg.Heartbeat(nil, "worker-a", "w-a", "idle", "", nil)
	reg.Heartbeat(nil, "worker-b", "w-b", "busy", "", nil)

	h := NewWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/workers", h.ListWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workers", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp WorkersListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Workers) != 2 {
		t.Fatalf("expected 2 workers, got %d", len(resp.Workers))
	}
	// Sorted by worker_id
	if resp.Workers[0].WorkerID != "worker-a" || resp.Workers[1].WorkerID != "worker-b" {
		t.Errorf("workers not sorted: %v", resp.Workers)
	}
	for _, wr := range resp.Workers {
		if wr.Status == "" {
			t.Errorf("worker %s has empty status", wr.WorkerID)
		}
	}
}

func TestGetWorker_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := workersreg.New(nil)
	h := NewWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/workers/:worker_id", h.GetWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/nonexistent", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetWorker_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := workersreg.New(nil)
	reg.Heartbeat(nil, "worker-x", "W X", "idle", "task-1", nil)

	h := NewWorkersHandler(reg)
	r := gin.New()
	r.GET("/api/v1/workers/:worker_id", h.GetWorker())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workers/worker-x", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp WorkerResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.WorkerID != "worker-x" {
		t.Errorf("WorkerID = %q, want worker-x", resp.WorkerID)
	}
	if resp.WorkerName != "W X" {
		t.Errorf("WorkerName = %q, want W X", resp.WorkerName)
	}
	if resp.CurrentTaskID != "task-1" {
		t.Errorf("CurrentTaskID = %q, want task-1", resp.CurrentTaskID)
	}

	// No DB wired (workersreg.New(nil) → dbStore=nil) so the hydrate path
	// returns session_active=false → canonical ConnectionStatus returns
	// DISCONNECTED for ALL workers that haven't hand-rolled a session
	// INSERT. Pin the assertion here so a regression that drops the
	// enum value (e.g. a future omitempty flip on WorkerResponse.Status)
	// is caught at test time.

	if resp.Status != "DISCONNECTED" {
		t.Errorf("Status = %q, want DISCONNECTED (no DB wired, conservative fallback)", resp.Status)
	}
	if resp.SessionActive {
		t.Errorf("SessionActive = true; want false (no DB; dbStore=nil; conservative fallback)")
	}
}

func TestListWorkers_NilRegistry(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &WorkersHandler{reg: nil}
	r := gin.New()
	r.GET("/api/v1/workers", h.ListWorkers())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/workers", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestExtractExecutors(t *testing.T) {
	cases := []struct {
		name string
		caps map[string]interface{}
		want []ExecutorEntry
	}{
		{"nil caps", nil, nil},
		{"empty caps", map[string]interface{}{}, nil},
		{"no executors key", map[string]interface{}{"other": 1}, nil},
		{"proto form", map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
				map[string]interface{}{"id": "asset.prepare.v1", "version": float64(2)},
			},
		}, []ExecutorEntry{{"scene.composite.v1", 1}, {"asset.prepare.v1", 2}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractExecutors(tc.caps)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
