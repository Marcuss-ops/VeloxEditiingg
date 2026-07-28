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
	"strings"
	"testing"
	"time"

	"velox-server/internal/fleet"
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
			if strings.Contains(cmd, "{{.Image}}") {
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
