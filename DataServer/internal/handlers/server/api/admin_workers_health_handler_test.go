// Package api — Step 10/15 admin workers health handler tests.
//
// The handler is a thin routing shell over the fleet pure-function
// probes (ProbeLevel{A,B,C,D}) which have their own
// per-level test matrix in health_probe_test.go. This file
// focuses on:
//
//   - the routing-decision matrix: ?level=A|B|C|D|invalid|absent
//   - the JSON envelope shape (HealthReport + AggregatedHealth)
//   - the per-probe HealthLevel vocabulary (exact uppercase)
//   - the HealthProbeDeps nil-tolerance contract
//
// HTTP-stack tests (?level routing through a real Gin engine +
// real *workersreg.Registry) are intententionally NOT included
// in this file because the registry plumbing needed to stub a
// worker-presence for a 404 path is significant. Those tests
// live with the registry-stub helpers and exercise the full
// HTTP surface end-to-end. The ProbeAll + per-level routing
// assertion below covers the routing decision without
// standing up a Gin engine.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	"velox-server/internal/smokerunstore"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"
	"velox-shared/identity"
)

// ── routing decision tests ───────────────────────────────────────────

func TestHealth_LevelRouting_Vocabulary(t *testing.T) {
	for _, c := range []struct {
		input   string
		want    bool
		comment string
	}{
		{"A", true, "valid uppercase"},
		{"B", true, "valid uppercase"},
		{"C", true, "valid uppercase"},
		{"D", true, "valid uppercase"},
		{"", false, "absent — handler treats as aggregated, NOT invalid"},
		{"Z", false, "out of vocabulary"},
		{"a", false, "lowercase rejected (vocabulary is uppercase)"},
		{"AB", false, "single-letter only"},
		{" A", false, "leading whitespace rejected by handler's TrimSpace + valid()"},
	} {
		got := fleet.HealthLevel(c.input).Valid()
		if got != c.want {
			t.Errorf("HealthLevel(%q).Valid() = %v, want %v (%s)", c.input, got, c.want, c.comment)
		}
	}
}

func TestHealth_ProbeDispatch_NoPanicOnNilDeps(t *testing.T) {
	// The handler passes individual nil-tolerant deps to each
	// probe. Verifying the per-probe contract here documents
	// the expected behavior under bootstrap misconfig.
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("level A nil-ssh", func(t *testing.T) {
		rep := fleet.ProbeLevelA(ctx, nil, "wkr-1", now)
		if rep.Healthy {
			t.Errorf("nil-ssh Level A must not be healthy")
		}
		if _, ok := rep.Checks["ssh_deps"]; !ok {
			t.Errorf("nil-ssh Level A must surface ssh_deps sentinel check")
		}
	})

	t.Run("level B nil-ssh", func(t *testing.T) {
		rep := fleet.ProbeLevelB(ctx, nil, nil, "wkr-1", now)
		if rep.Healthy {
			t.Errorf("nil-ssh Level B must not be healthy")
		}
	})
	t.Run("level B nil-deployments", func(t *testing.T) {
		// Provide a non-nil SSH stub so the probe progresses past
		// the ssh_deps early-return and reaches the deployments-nil
		// branch. The stub returns a fake digest so runningDigest
		// is non-empty (otherwise image_digest_match short-circuits
		// on the empty-running-digest check first).
		sshStub := fakeSSHForLevelB{runFn: func(cmd string) (string, error) {
			if strings.Contains(cmd, "{{.Config.Image}}") {
				return "sha256:fakedigest", nil
			}
			return "ok", nil
		}}
		rep := fleet.ProbeLevelB(ctx, sshStub, nil, "wkr-1", now)
		// Without deployments repo, the image_digest_match check
		// is unverified — Recorded as Passed=false with detail.
		c, ok := rep.Checks["image_digest_match"]
		if !ok {
			t.Errorf("Level B must surface image_digest_match even with nil deployments")
			return
		}
		if c.Passed {
			t.Errorf("nil-deployments image_digest_match must be unverified (Passed=false)")
		}
	})

	t.Run("level C nil-registry", func(t *testing.T) {
		rep := fleet.ProbeLevelC(ctx, nil, "wkr-1", now)
		if rep.Healthy {
			t.Errorf("nil-registry Level C must not be healthy")
		}
		if _, ok := rep.Checks["registry_deps"]; !ok {
			t.Errorf("nil-registry Level C must surface registry_deps sentinel check")
		}
	})

	t.Run("level D nil-smoke", func(t *testing.T) {
		rep := fleet.ProbeLevelD(ctx, nil, "wkr-1", now)
		if rep.Healthy {
			t.Errorf("nil-smoke Level D must not be healthy")
		}
		if _, ok := rep.Checks["smoke_deps"]; !ok {
			t.Errorf("nil-smoke Level D must surface smoke_deps sentinel check")
		}
	})
}

// ── JSON envelope shape tests ────────────────────────────────────────

func TestHealth_HealthReport_JSONShape(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	rep := fleet.HealthReport{
		WorkerID:    "wkr-1",
		Level:       fleet.HealthLevelA,
		Healthy:     false,
		Checks:      map[string]fleet.CheckResult{"ssh_deps": {Passed: false, Detail: "not wired"}},
		CollectedAt: now,
		DurationMs:  12,
	}
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Required JSON keys.
	for _, key := range []string{
		`"worker_id":"wkr-1"`,
		`"level":"A"`,
		`"healthy":false`,
		`"checks":{`,
		`"ssh_deps"`,
		`"collected_at":`,
		`"duration_ms":12`,
	} {
		if !strings.Contains(string(b), key) {
			t.Errorf("HealthReport JSON missing key %q in %s", key, string(b))
		}
	}
}

func TestHealth_AggregatedHealth_JSONShape(t *testing.T) {
	agg := AggregatedHealth{
		WorkerID: "wkr-1",
		Reports: []fleet.HealthReport{
			{Level: fleet.HealthLevelA, Healthy: false},
			{Level: fleet.HealthLevelB, Healthy: true},
			{Level: fleet.HealthLevelC, Healthy: true},
			{Level: fleet.HealthLevelD, Healthy: false},
		},
	}
	b, err := json.Marshal(agg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"worker_id":"wkr-1"`) {
		t.Errorf("AggregatedHealth JSON missing worker_id: %s", string(b))
	}
	if !strings.Contains(string(b), `"reports":[`) {
		t.Errorf("AggregatedHealth JSON missing reports array: %s", string(b))
	}
	// All 4 levels present in order.
	for _, lvl := range []string{`"level":"A"`, `"level":"B"`, `"level":"C"`, `"level":"D"`} {
		if !strings.Contains(string(b), lvl) {
			t.Errorf("AggregatedHealth JSON missing %s in %s", lvl, string(b))
		}
	}
}

func TestHealth_CheckResult_Omitempty(t *testing.T) {
	// Passed=true with no Value/Expected/Detail → omitempty trims them.
	c := fleet.CheckResult{Passed: true, Value: "ok"}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Value="ok" is non-empty so it must appear; expected and
	// detail are empty so they MUST be omitted to honor omitempty.
	if !strings.Contains(string(b), `"value":"ok"`) {
		t.Errorf("CheckResult JSON missing value field: %s", string(b))
	}
	if strings.Contains(string(b), `"expected"`) {
		t.Errorf("CheckResult JSON must omit empty Expected (omitempty): %s", string(b))
	}
	if strings.Contains(string(b), `"detail"`) {
		t.Errorf("CheckResult JSON must omit empty Detail (omitempty): %s", string(b))
	}
}

// ── HealthProbeDeps + handler constructor tests ──────────────────────

func TestHealth_NewAdminWorkersHealthHandler_NilDepsOK(t *testing.T) {
	// Constructor tolerant of nil reg + nil deps; deferred
	// check via the per-probe sentinel pattern (above).
	h := NewAdminWorkersHealthHandler(nil, HealthProbeDeps{})
	if h == nil {
		t.Fatalf("NewAdminWorkersHealthHandler(nil, empty deps) returned nil")
	}
	if h.reg != nil {
		t.Errorf("reg should be nil")
	}
}

// ── helpers ──────────────────────────────────────────────────────────

// fakeSSHForLevelB is a minimal BackendSSHClient stub used by
// TestHealth_ProbeDispatch_NoPanicOnNilDeps/level_B when the
// test wants the probe to reach the deployments-nil branch
// without short-circuiting on ssh_deps. The struct mirrors the
// fleet.stubSSH shape from health_probe_test.go but lives in
// the api test package to avoid an unneeded cross-package
// dependency on test internals.
type fakeSSHForLevelB struct {
	runFn func(cmd string) (string, error)
}

func (f fakeSSHForLevelB) Run(_ context.Context, _, cmd string) (string, error) {
	if f.runFn == nil {
		return "stub-default-ok", nil
	}
	return f.runFn(cmd)
}

// TestHealth_HTTPDispatchesAllLevels exercises the actual admin health route
// rather than only the pure probe functions. One registered worker is queried
// at A/B/C/D and through the aggregate endpoint; every response must preserve
// the canonical level vocabulary and JSON envelope.
func TestHealth_HTTPDispatchesAllLevels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	reg := workersreg.New(nil)
	workerID := "health-http-worker"
	caps := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	if err := reg.RegisterWorker(ctx, workerID, "health-http", "127.0.0.1", caps); err != nil {
		t.Fatal(err)
	}
	if err := reg.Heartbeat(ctx, workerID, "health-http", "", nil); err != nil {
		t.Fatal(err)
	}

	ssh := fakeSSHForLevelB{runFn: func(cmd string) (string, error) {
		switch {
		case cmd == "true":
			return "", nil
		case strings.HasPrefix(cmd, "awk '{print $1}' /proc/loadavg"):
			return "0.10", nil
		case strings.HasPrefix(cmd, "free -m"):
			return "4096", nil
		case strings.HasPrefix(cmd, "df --output=pcent"):
			return "20", nil
		case strings.HasPrefix(cmd, "docker info"):
			return "24.0.7", nil
		case strings.HasPrefix(cmd, "timedatectl"):
			return "yes", nil
		case strings.Contains(cmd, "State.Running"):
			return "true", nil
		case strings.Contains(cmd, "curl"):
			return "ok", nil
		case strings.Contains(cmd, "{{.Config.Image}}"):
			return "sha256:health-http", nil
		case strings.Contains(cmd, "RestartCount"):
			return "0", nil
		default:
			return "ok", nil
		}
	}}
	health := NewAdminWorkersHealthHandler(reg, HealthProbeDeps{
		SSH:         ssh,
		Deployments: healthHTTPDeployments{rec: &store.DeploymentRecord{TargetDigest: "sha256:health-http", Status: "SUCCEEDED"}},
		Registry:    healthHTTPRegistry{workerID: workerID},
		Smoke: fleet.NewSmokeRunHealthChecker(healthHTTPSmokeRuns{run: smokerunstore.SmokeRun{
			RunID: "smoke-health-http", WorkerID: workerID,
			StartedAt: time.Now().UTC(), Status: smokerunstore.SmokeStatusSucceeded,
			ArtifactDriveID: "smoke-health-http",
		}}),
	})
	r := gin.New()
	r.GET("/api/v1/admin/workers/:worker_id/health", health.GetWorkerHealth())

	expectedChecks := map[string][]string{
		"A": {"ssh_up", "cpu_load_1m", "memory_available_mb", "disk_used_pct", "docker_active", "ntp_synced"},
		"B": {"container_running", "health_ready", "image_digest_match", "no_restart_loop"},
		"C": {"worker_present", "status_connected", "session_active", "executor_advertised", "heartbeat_fresh", "deployment_state"},
		"D": {"smoke_ok"},
	}
	for _, level := range []string{"A", "B", "C", "D"} {
		w := httptest.NewRecorder()
		path := "/api/v1/admin/workers/" + workerID + "/health?level=" + level
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("level %s status=%d body=%s", level, w.Code, w.Body.String())
		}
		var report fleet.HealthReport
		if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
			t.Fatalf("level %s decode: %v", level, err)
		}
		if report.WorkerID != workerID || string(report.Level) != level {
			t.Fatalf("level %s report=%+v", level, report)
		}
		if !report.Healthy {
			t.Fatalf("level %s unhealthy: %+v", level, report.Checks)
		}
		for _, check := range expectedChecks[level] {
			result, ok := report.Checks[check]
			if !ok || !result.Passed {
				t.Fatalf("level %s check %s=%+v, want passed", level, check, result)
			}
		}
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/workers/"+workerID+"/health", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("aggregate status=%d body=%s", w.Code, w.Body.String())
	}
	var aggregate AggregatedHealth
	if err := json.Unmarshal(w.Body.Bytes(), &aggregate); err != nil {
		t.Fatal(err)
	}
	if len(aggregate.Reports) != 4 {
		t.Fatalf("aggregate reports=%d want 4", len(aggregate.Reports))
	}
	for i, want := range []fleet.HealthLevel{fleet.HealthLevelA, fleet.HealthLevelB, fleet.HealthLevelC, fleet.HealthLevelD} {
		if aggregate.Reports[i].Level != want {
			t.Fatalf("aggregate report[%d]=%+v", i, aggregate.Reports[i])
		}
		if !aggregate.Reports[i].Healthy {
			t.Fatalf("aggregate report[%d] unexpectedly unhealthy: %+v", i, aggregate.Reports[i])
		}
		for _, check := range expectedChecks[string(want)] {
			result, ok := aggregate.Reports[i].Checks[check]
			if !ok || !result.Passed {
				t.Fatalf("aggregate report[%d] check %s=%+v, want passed", i, check, result)
			}
		}
	}
}

type healthHTTPDeployments struct {
	rec *store.DeploymentRecord
}

func (d healthHTTPDeployments) GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error) {
	return d.rec, nil
}
func (healthHTTPDeployments) InsertDeploymentRecord(context.Context, store.DeploymentRecord) error {
	return nil
}
func (healthHTTPDeployments) MarkVerifiedSucceeded(context.Context, string, string, time.Time) error {
	return nil
}
func (healthHTTPDeployments) MarkFailed(context.Context, string, time.Time, string, string) error {
	return nil
}
func (healthHTTPDeployments) MarkDeploymentRolledBack(context.Context, string, time.Time, bool, string) error {
	return nil
}

type healthHTTPRegistry struct {
	workerID string
}

func (r healthHTTPRegistry) GetWorker(context.Context, string) (*workersreg.Worker, error) {
	caps, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{
		ID: "scene.composite.v1", Version: 1,
	})
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return &workersreg.Worker{
		WorkerID:             identity.ParseWorkerID(r.workerID),
		ConnectionStatus:     "CONNECTED",
		SessionActive:        true,
		LastHB:               now.Format(time.RFC3339Nano),
		DeploymentState:      "CURRENT",
		ExecutorCapabilities: caps,
	}, nil
}

type healthHTTPSmokeRuns struct {
	run smokerunstore.SmokeRun
}

func (s healthHTTPSmokeRuns) InsertSmokeRun(context.Context, smokerunstore.SmokeRun) error {
	return nil
}
func (s healthHTTPSmokeRuns) MarkSmokeSucceeded(context.Context, string, time.Time, int64, string) error {
	return nil
}
func (s healthHTTPSmokeRuns) MarkSmokeFailed(context.Context, string, time.Time, int64, string) error {
	return nil
}
func (s healthHTTPSmokeRuns) GetLatestSmokeForWorker(context.Context, string) (*smokerunstore.SmokeRun, error) {
	run := s.run
	return &run, nil
}
func (s healthHTTPSmokeRuns) ListRecentSmokesForWorker(context.Context, string, int) ([]smokerunstore.SmokeRun, error) {
	return []smokerunstore.SmokeRun{s.run}, nil
}
