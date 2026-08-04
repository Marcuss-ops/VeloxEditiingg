// Package fleet — Step 10/15 health_probe.go tests.
//
// Coverage (each per-level test exercises a single decision
// branch in the per-level pure-function contract):
//
//	Level A (host) — 8 tests:
//	  TestProbeLevelA_HappyPath          — all 6 sub-checks pass with golden values
//	  TestProbeLevelA_NilSSH             — nil ssh dep → single ssh_deps sentinel
//	  TestProbeLevelA_SSHUnreachable     — `true` returns error → ssh_up fail + cascades
//	  TestProbeLevelA_LoadOverThreshold  — /proc/loadavg = 5.0 → fail
//	  TestProbeLevelA_MemoryBelowMin     — MemAvailable = 64 → fail
//	  TestProbeLevelA_DiskOverMax        — Used% = 92 → fail
//	  TestProbeLevelA_DockerEmpty        — ServerVersion empty → fail
//	  TestProbeLevelA_NTPUnsynced        — NTPSynchronized=yes absent → fail
//
//	Level B (container) — 7 tests:
//	  TestProbeLevelB_HappyPath           — all 4 sub-checks pass
//	  TestProbeLevelB_NilSSH              — nil ssh dep → single ssh_deps sentinel
//	  TestProbeLevelB_ContainerNotRunning — State.Running=false → fail
//	  TestProbeLevelB_HealthReadyFail    — curl exit non-zero → fail
//	  TestProbeLevelB_ImageDigestNoRow    — no ledger row → fail with detail
//	  TestProbeLevelB_ImageDigestMatch    — running == ledger.TargetDigest → pass
//	  TestProbeLevelB_RestartLoop         — RestartCount=10 → fail
//	  TestProbeLevelB_PendingAccepted     — ledger status=PENDING → pass (in-flight)
//
//	Level C (master) — 7 tests:
//	  TestProbeLevelC_HappyPath           — all 6 sub-checks pass
//	  TestProbeLevelC_NilRegistry         — nil gater → registry_deps sentinel
//	  TestProbeLevelC_WorkerNotInReg      — GetWorker returns nil → fail
//	  TestProbeLevelC_StatusNotConnected  — ConnectionStatus=DISCONNECTED → fail
//	  TestProbeLevelC_SessionInactive     — SessionActive=false → fail
//	  TestProbeLevelC_NoExecutorAds       — empty Capabilities.supported_executors → fail
//	  TestProbeLevelC_HeartbeatStale      — LastHB 200s ago → fail
//
//	Level D (smoke) — 3 tests:
//	  TestProbeLevelD_HappyPath           — non-empty artifact_id without err → pass
//	  TestProbeLevelD_NilSmoke            — nil dep → smoke_deps sentinel
//	  TestProbeLevelD_RunnerError         — RunLevelD returns error → fail
package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ── Stub helpers ──────────────────────────────────────────────────────

// stubSSH is the per-test stub for BackendSSHClient. The
// runFn callback dispatches by command string. Returns
// canned (output, err) for the matching prefix; if no
// prefix matches, returns ("", nil). Lets each test pin
// only the commands the probe actually runs.
type stubSSH struct {
	runFn func(ctx context.Context, workerID, command string) (string, error)
}

func (s stubSSH) Run(ctx context.Context, workerID, command string) (string, error) {
	if s.runFn == nil {
		return "stub-default-ok", nil
	}
	return s.runFn(ctx, workerID, command)
}

// stubDeployments is the per-test stub for BackendDeploymentRepo.
// Only GetLatestDeploymentForWorker is exercised by ProbeLevelB.
type stubDeployments struct {
	rec *store.DeploymentRecord
	err error
}

func (s stubDeployments) GetLatestDeploymentForWorker(_ context.Context, _ string) (*store.DeploymentRecord, error) {
	return s.rec, s.err
}
func (s stubDeployments) InsertDeploymentRecord(_ context.Context, _ store.DeploymentRecord) error {
	return nil
}
func (s stubDeployments) MarkSucceeded(_ context.Context, _ string, _ time.Time) error { return nil }
func (s stubDeployments) MarkFailed(_ context.Context, _ string, _ time.Time, _ string) error {
	return nil
}
func (s stubDeployments) MarkDeploymentRolledBack(_ context.Context, _ string, _ time.Time, _ bool) error {
	return nil
}

// stubLevelCGater is the per-test stub for HealthLevelCGater.
type stubLevelCGater struct {
	info *workersreg.WorkerInfo
	err  error
}

func (s stubLevelCGater) GetWorker(_ context.Context, _ string) (*workersreg.WorkerInfo, error) {
	return s.info, s.err
}

// stubSmoke is the per-test stub for BackendSmokeRunner.
type stubSmoke struct {
	artifactID string
	err        error
}

func (s stubSmoke) RunLevelD(_ context.Context, _ string) (string, error) {
	return s.artifactID, s.err
}

// helper: build a healthy worker at time t0 (LastHB 30s before now).
func healthyWorkerAt(workerID string, t time.Time) *workersreg.WorkerInfo {
	return &workersreg.WorkerInfo{
		WorkerID:         workerID,
		ConnectionStatus: "CONNECTED",
		SessionActive:    true,
		LastHB:           t.Add(-30 * time.Second).Format(time.RFC3339Nano),
		DeploymentState:  "CURRENT",
		Capabilities: map[string]interface{}{
			"supported_executors": []string{"scene.composite.v1"},
		},
	}
}

// ── Level A tests ─────────────────────────────────────────────────────

func TestProbeLevelA_HappyPath(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		switch {
		case cmd == "true":
			return "", nil
		case strings.HasPrefix(cmd, "awk '{print $1}' /proc/loadavg"):
			return "0.42", nil
		case strings.HasPrefix(cmd, "free -m"):
			return "4096", nil
		case strings.HasPrefix(cmd, "df --output=pcent"):
			return "44", nil
		case strings.HasPrefix(cmd, "docker info"):
			return "24.0.7", nil
		case strings.HasPrefix(cmd, "timedatectl"):
			return "yes", nil
		}
		return "", nil
	}}
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", now)
	if !rep.Healthy {
		t.Errorf("happy-path Level A not healthy: %+v", rep.Checks)
	}
	for k, want := range map[string]string{
		"ssh_up":              "ok",
		"cpu_load_1m":         "0.42",
		"memory_available_mb": "4096",
		"disk_used_pct":       "44",
		"docker_active":       "24.0.7",
		"ntp_synced":          "yes",
	} {
		c, ok := rep.Checks[k]
		if !ok {
			t.Errorf("missing sub-check %q", k)
			continue
		}
		if !c.Passed {
			t.Errorf("Level A sub-check %q failed: %+v", k, c)
		}
		if c.Value != want {
			t.Errorf("Level A sub-check %q value = %q, want %q", k, c.Value, want)
		}
	}
	if rep.Level != HealthLevelA {
		t.Errorf("Level = %q, want A", rep.Level)
	}
}

func TestProbeLevelA_NilSSH(t *testing.T) {
	rep := ProbeLevelA(context.Background(), nil, "wkr-1", time.Now())
	if rep.Healthy {
		t.Errorf("nil-ssh Level A should not be healthy")
	}
	if !strings.Contains(rep.Checks["ssh_deps"].Detail, "Step 11+") {
		t.Errorf("ssh_deps detail must mention the Step 11+ dependency; got %q", rep.Checks["ssh_deps"].Detail)
	}
}

func TestProbeLevelA_SSHUnreachable(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if cmd == "true" {
			return "", errors.New("connection refused")
		}
		return "", errors.New("ssh unreachable")
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["ssh_up"].Passed {
		t.Errorf("ssh_up should be false when ssh unreachable")
	}
	if !strings.Contains(rep.Checks["ssh_up"].Detail, "connection refused") {
		t.Errorf("ssh_up detail must mention the underlying err; got %q", rep.Checks["ssh_up"].Detail)
	}
	if rep.Healthy {
		t.Errorf("rep.Healthy = true, want false when ssh_up fails")
	}
}

func TestProbeLevelA_LoadOverThreshold(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "awk '{print $1}' /proc/loadavg") {
			return "5.00", nil
		}
		return "ok", nil
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["cpu_load_1m"].Passed {
		t.Errorf("load=5.00 must fail (threshold <%.2f)", ThresholdLoad1Max)
	}
	if rep.Checks["cpu_load_1m"].Value != "5.00" {
		t.Errorf("cpu_load_1m value = %q, want 5.00", rep.Checks["cpu_load_1m"].Value)
	}
}

func TestProbeLevelA_MemoryBelowMin(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "free -m") {
			return "64", nil
		}
		return "ok", nil
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["memory_available_mb"].Passed {
		t.Errorf("memory=64MB must fail (threshold >%d)", ThresholdMemAvailMin)
	}
}

func TestProbeLevelA_DiskOverMax(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "df --output=pcent") {
			return "92", nil
		}
		return "ok", nil
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["disk_used_pct"].Passed {
		t.Errorf("disk=92%% must fail (threshold <%d)", ThresholdDiskUsedMax)
	}
}

func TestProbeLevelA_DockerEmpty(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "docker info") {
			return "", nil
		}
		return "ok", nil
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["docker_active"].Passed {
		t.Errorf("empty ServerVersion must fail docker_active")
	}
}

func TestProbeLevelA_NTPUnsynced(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.HasPrefix(cmd, "timedatectl") {
			return "no", nil
		}
		return "ok", nil
	}}
	rep := ProbeLevelA(context.Background(), ssh, "wkr-1", time.Now())
	if rep.Checks["ntp_synced"].Passed {
		t.Errorf("NTP=no must fail ntp_synced")
	}
}

// ── Level B tests ─────────────────────────────────────────────────────

func TestProbeLevelB_HappyPath(t *testing.T) {
	targetDigest := "sha256:abc123"
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		switch {
		case strings.Contains(cmd, "State.Running"):
			return "true", nil
		case strings.Contains(cmd, "curl"):
			return "ok", nil
		case strings.Contains(cmd, "{{.Image}}"):
			return targetDigest, nil
		case strings.Contains(cmd, "RestartCount"):
			return "1", nil
		}
		return "", nil
	}}
	dep := stubDeployments{rec: &store.DeploymentRecord{TargetDigest: targetDigest, Status: "SUCCEEDED"}}
	rep := ProbeLevelB(context.Background(), ssh, dep, "wkr-1", time.Now())
	if !rep.Healthy {
		t.Errorf("happy-path Level B not healthy: %+v", rep.Checks)
	}
	for _, k := range []string{"container_running", "health_ready", "image_digest_match", "no_restart_loop"} {
		if !rep.Checks[k].Passed {
			t.Errorf("missing/passing %q: %+v", k, rep.Checks[k])
		}
	}
}

func TestProbeLevelB_NilSSH(t *testing.T) {
	rep := ProbeLevelB(context.Background(), nil, stubDeployments{}, "wkr-1", time.Now())
	if rep.Healthy {
		t.Errorf("nil-ssh Level B should not be healthy")
	}
	if !strings.Contains(rep.Checks["ssh_deps"].Detail, "Step 11+") {
		t.Errorf("ssh_deps detail must mention the Step 11+ dependency")
	}
}

func TestProbeLevelB_ContainerNotRunning(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.Contains(cmd, "State.Running") {
			return "false", nil
		}
		return "", nil
	}}
	rep := ProbeLevelB(context.Background(), ssh, stubDeployments{}, "wkr-1", time.Now())
	if rep.Checks["container_running"].Passed {
		t.Errorf("container_not_running must fail container_running sub-check")
	}
}

func TestProbeLevelB_HealthReadyFail(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.Contains(cmd, "curl") {
			return "", errors.New("exit 7")
		}
		return "", nil
	}}
	rep := ProbeLevelB(context.Background(), ssh, stubDeployments{}, "wkr-1", time.Now())
	if rep.Checks["health_ready"].Passed {
		t.Errorf("curl exit 7 must fail health_ready")
	}
}

func TestProbeLevelB_ImageDigestNoRow(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.Contains(cmd, "{{.Image}}") {
			return "sha256:runningdigest", nil
		}
		return "", nil
	}}
	dep := stubDeployments{err: store.ErrDeploymentNotFound}
	rep := ProbeLevelB(context.Background(), ssh, dep, "wkr-1", time.Now())
	if rep.Checks["image_digest_match"].Passed {
		t.Errorf("no ledger row must fail image_digest_match")
	}
	if !strings.Contains(rep.Checks["image_digest_match"].Detail, "ledger row") {
		t.Errorf("detail must mention ledger row absence; got %q", rep.Checks["image_digest_match"].Detail)
	}
}

func TestProbeLevelB_PendingAccepted(t *testing.T) {
	targetDigest := "sha256:in-flight-target"
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.Contains(cmd, "{{.Image}}") {
			return targetDigest, nil
		}
		return "", nil
	}}
	dep := stubDeployments{rec: &store.DeploymentRecord{TargetDigest: targetDigest, Status: "PENDING"}}
	rep := ProbeLevelB(context.Background(), ssh, dep, "wkr-1", time.Now())
	if !rep.Checks["image_digest_match"].Passed {
		t.Errorf("PENDING in-flight deploy should pass image_digest_match; got %+v", rep.Checks["image_digest_match"])
	}
}

func TestProbeLevelB_RestartLoop(t *testing.T) {
	ssh := stubSSH{runFn: func(_ context.Context, _, cmd string) (string, error) {
		if strings.Contains(cmd, "RestartCount") {
			return "10", nil
		}
		return "", nil
	}}
	rep := ProbeLevelB(context.Background(), ssh, stubDeployments{}, "wkr-1", time.Now())
	if rep.Checks["no_restart_loop"].Passed {
		t.Errorf("RestartCount=10 must fail no_restart_loop")
	}
}

// ── Level C tests ─────────────────────────────────────────────────────

func TestProbeLevelC_HappyPath(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	gater := stubLevelCGater{info: healthyWorkerAt("wkr-1", now)}
	rep := ProbeLevelC(context.Background(), gater, "wkr-1", now)
	if !rep.Healthy {
		t.Errorf("happy-path Level C not healthy: %+v", rep.Checks)
	}
	for _, k := range []string{
		"worker_present", "status_connected", "session_active",
		"executor_advertised", "heartbeat_fresh", "deployment_state",
	} {
		if !rep.Checks[k].Passed {
			t.Errorf("Level C sub-check %q failed: %+v", k, rep.Checks[k])
		}
	}
}

func TestProbeLevelC_NilRegistry(t *testing.T) {
	rep := ProbeLevelC(context.Background(), nil, "wkr-1", time.Now())
	if rep.Healthy {
		t.Errorf("nil-registry Level C should not be healthy")
	}
	if !strings.Contains(rep.Checks["registry_deps"].Detail, "registry gater not wired") {
		t.Errorf("registry_deps detail must mention the missing dep")
	}
}

func TestProbeLevelC_WorkerNotInReg(t *testing.T) {
	gater := stubLevelCGater{info: nil}
	rep := ProbeLevelC(context.Background(), gater, "wkr-ghost", time.Now())
	if rep.Healthy {
		t.Errorf("worker-not-in-reg must not be healthy")
	}
	if rep.Checks["worker_present"].Passed {
		t.Errorf("worker_present must be false for unregistered worker")
	}
}

func TestProbeLevelC_StatusNotConnected(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	info := healthyWorkerAt("wkr-1", now)
	info.ConnectionStatus = "DISCONNECTED"
	gater := stubLevelCGater{info: info}
	rep := ProbeLevelC(context.Background(), gater, "wkr-1", now)
	if rep.Checks["status_connected"].Passed {
		t.Errorf("status_connected must be false for DISCONNECTED state")
	}
}

func TestProbeLevelC_SessionInactive(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	info := healthyWorkerAt("wkr-1", now)
	info.SessionActive = false
	gater := stubLevelCGater{info: info}
	rep := ProbeLevelC(context.Background(), gater, "wkr-1", now)
	if rep.Checks["session_active"].Passed {
		t.Errorf("session_active must be false for inactive session")
	}
}

func TestProbeLevelC_NoExecutorAds(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	info := healthyWorkerAt("wkr-1", now)
	info.Capabilities = map[string]interface{}{} // empty
	gater := stubLevelCGater{info: info}
	rep := ProbeLevelC(context.Background(), gater, "wkr-1", now)
	if rep.Checks["executor_advertised"].Passed {
		t.Errorf("executor_advertised must be false for empty Capabilities")
	}
}

func TestProbeLevelC_HeartbeatStale(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	info := healthyWorkerAt("wkr-1", now)
	// LastHB 200s ago — older than HeartbeatFreshnessWin (150s)
	info.LastHB = now.Add(-200 * time.Second).Format(time.RFC3339Nano)
	gater := stubLevelCGater{info: info}
	rep := ProbeLevelC(context.Background(), gater, "wkr-1", now)
	if rep.Checks["heartbeat_fresh"].Passed {
		t.Errorf("heartbeat age 200s must fail heartbeat_fresh")
	}
}

// ── Level D tests ─────────────────────────────────────────────────────

func TestProbeLevelD_HappyPath(t *testing.T) {
	smoke := stubSmoke{artifactID: "smoke-artifact-x"}
	rep := ProbeLevelD(context.Background(), smoke, "wkr-1", time.Now())
	if !rep.Healthy {
		t.Errorf("happy-path Level D not healthy: %+v", rep.Checks)
	}
	if rep.Checks["smoke_ok"].Value != "smoke-artifact-x" {
		t.Errorf("smoke_ok value = %q, want smoke-artifact-x", rep.Checks["smoke_ok"].Value)
	}
}

func TestProbeLevelD_NilSmoke(t *testing.T) {
	rep := ProbeLevelD(context.Background(), nil, "wkr-1", time.Now())
	if rep.Healthy {
		t.Errorf("nil-smoke Level D should not be healthy")
	}
	if !strings.Contains(rep.Checks["smoke_deps"].Detail, "Step 12+") {
		t.Errorf("smoke_deps detail must mention the Step 12+ dependency")
	}
}

func TestProbeLevelD_RunnerError(t *testing.T) {
	smoke := stubSmoke{err: errors.New("ffmpeg rc=1")}
	rep := ProbeLevelD(context.Background(), smoke, "wkr-1", time.Now())
	if rep.Checks["smoke_ok"].Passed {
		t.Errorf("runner error must fail smoke_ok: %+v", rep.Checks["smoke_ok"])
	}
	if !strings.Contains(rep.Checks["smoke_ok"].Detail, "ffmpeg rc=1") {
		t.Errorf("smoke_ok detail must include underlying err; got %q", rep.Checks["smoke_ok"].Detail)
	}
}

// ── ProbeAll utility ──────────────────────────────────────────────────

func TestProbeAll_AggregatesFourLevels(t *testing.T) {
	now := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
	gater := stubLevelCGater{info: healthyWorkerAt("wkr-1", now)}
	smoke := stubSmoke{artifactID: "x"}
	reports := ProbeAll(context.Background(), nil, stubDeployments{}, gater, smoke, "wkr-1", now)
	if len(reports) != 4 {
		t.Fatalf("ProbeAll returned %d reports; want 4", len(reports))
	}
	expected := []HealthLevel{HealthLevelA, HealthLevelB, HealthLevelC, HealthLevelD}
	for i, rep := range reports {
		if rep.Level != expected[i] {
			t.Errorf("ProbeAll[%d].Level = %q, want %q", i, rep.Level, expected[i])
		}
	}
}
