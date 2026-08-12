package fleet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"velox-server/internal/workers"
)

// rolloutTrace decorates the existing UpdateExecutor stubs with an ordered
// trace. It keeps this test at the consumer contract boundary: no Docker,
// SSH, systemd, registry, or network is used.
type rolloutTrace struct {
	*stubBackendsState
	events                 []string
	cosignRefs             []string
	activatedRefs          []string
	sshCommands            []string
	activeChecks           int
	reconnectChecks        int
	reconnectAfterActivate bool
}

func (t *rolloutTrace) Verify(_ context.Context, ref string) error {
	t.events = append(t.events, "cosign")
	t.cosignRefs = append(t.cosignRefs, ref)
	return t.stubBackendsState.cosignErr
}

func (t *rolloutTrace) ActivateImage(ctx context.Context, workerID, imageRef string) (string, error) {
	t.events = append(t.events, "docker.activate")
	t.activatedRefs = append(t.activatedRefs, imageRef)
	output, err := t.stubBackendsState.ActivateImage(ctx, workerID, imageRef)
	if err == nil && t.reconnectAfterActivate {
		t.sessionActive = true
		t.lastHB = time.Now().UTC().Format(time.RFC3339)
		t.events = append(t.events, "worker.reconnected")
	}
	return output, err
}

func (t *rolloutTrace) ContainerRunning(ctx context.Context, workerID string) (bool, error) {
	t.events = append(t.events, "docker.running")
	return t.stubBackendsState.ContainerRunning(ctx, workerID)
}

func (t *rolloutTrace) Run(ctx context.Context, workerID, command string) (string, error) {
	t.events = append(t.events, "ssh")
	t.sshCommands = append(t.sshCommands, command)
	return t.stubBackendsState.Run(ctx, workerID, command)
}

func (t *rolloutTrace) GetWorker(ctx context.Context, workerID string) (*workers.Worker, error) {
	info, err := t.stubBackendsState.GetWorker(ctx, workerID)
	if info != nil && info.SessionActive && info.LastHB != "" {
		t.reconnectChecks++
		t.events = append(t.events, "master.connected")
	}
	return info, err
}

func (t *rolloutTrace) IsActiveJobsZero(ctx context.Context, workerID string) bool {
	t.activeChecks++
	t.events = append(t.events, "active_tasks=0")
	return t.stubBackendsState.IsActiveJobsZero(ctx, workerID)
}

func (t *rolloutTrace) IsDrained(ctx context.Context, workerID string) bool {
	t.events = append(t.events, "drained=true")
	return t.stubBackendsState.IsDrained(ctx, workerID)
}

func (t *rolloutTrace) SetDrainMode(ctx context.Context, workerID string, drain bool) error {
	t.events = append(t.events, fmt.Sprintf("drain=%t", drain))
	return t.stubBackendsState.SetDrainMode(ctx, workerID, drain)
}

func (t *rolloutTrace) RunLevelD(ctx context.Context, workerID string) (string, error) {
	t.events = append(t.events, "smoke")
	return t.stubBackendsState.RunLevelD(ctx, workerID)
}

func (t *rolloutTrace) VerifyDelivery(ctx context.Context, driveFileID string, expectedBytes int64) error {
	t.events = append(t.events, "drive.verify")
	return t.stubBackendsState.VerifyDelivery(ctx, driveFileID, expectedBytes)
}

func TestOfflineRollout_CompleteSequenceUsesExpectedDigestAndReconnects(t *testing.T) {
	backend, state := stubBackends(t)
	// Start disconnected, then model the worker's first fresh heartbeat after
	// the restart. This keeps reconnect a tested transition rather than merely
	// asserting a pre-populated connected fixture.
	state.sessionActive = false
	state.lastHB = ""
	trace := &rolloutTrace{stubBackendsState: state, reconnectAfterActivate: true}
	backend.SSHCmd = trace
	backend.Docker = trace
	backend.Cosign = trace
	backend.Smoke = trace
	backend.Drive = trace
	backend.Registry = trace
	backend.Deployments = trace

	const workerID = "offline-rollout-worker"
	targetDigest := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("d", 64)
	// The fresh Hello after the restart advertises the target digest (the
	// reconnect also flips the session to a NEW one, satisfying WAITING_READY).
	state.runtimeDigest = targetDigest
	e := NewUpdateExecutor(backend)
	if err := e.Execute(context.Background(), mkOp(workerID, targetDigest, "")); err != nil {
		t.Fatalf("offline rollout returned error: %v", err)
	}

	if !state.activeTasksZero {
		t.Fatal("fixture did not model active_tasks=0")
	}
	if trace.activeChecks == 0 {
		t.Fatal("UpdateExecutor never checked active_tasks=0")
	}
	if got, want := state.drainCalls, []bool{true, false}; !equalBools(got, want) {
		t.Fatalf("drain calls = %v, want %v", got, want)
	}
	if !containsEvent(trace.events, "docker.activate") || len(trace.activatedRefs) != 1 || trace.activatedRefs[0] != targetDigest {
		t.Fatalf("activation refs = %v, want [%s]", trace.activatedRefs, targetDigest)
	}
	if len(trace.cosignRefs) != 1 || trace.cosignRefs[0] != targetDigest {
		t.Fatalf("Cosign refs = %v, want [%s]", trace.cosignRefs, targetDigest)
	}
	if !containsEvent(trace.events, "ssh") || len(trace.sshCommands) != 1 || trace.sshCommands[0] != "curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready" {
		t.Fatalf("SSH health commands = %v, want canonical health/ready command", trace.sshCommands)
	}
	if !containsEvent(trace.events, "worker.reconnected") || trace.reconnectChecks == 0 || !containsEvent(trace.events, "master.connected") {
		t.Fatalf("rollout did not reconnect and verify an active master connection: %v", trace.events)
	}
	for _, required := range []string{"drain=true", "drained=true", "active_tasks=0", "cosign", "docker.activate", "docker.running", "ssh", "worker.reconnected", "master.connected", "smoke", "drive.verify", "drain=false"} {
		if !containsEvent(trace.events, required) {
			t.Fatalf("rollout trace missing %q: %v", required, trace.events)
		}
	}
}

func TestOfflineRollout_FailClosedWhenRequiredBackendIsMissing(t *testing.T) {
	backend, _ := stubBackends(t)
	backend.Docker = nil
	if err := NewUpdateExecutor(backend).ValidateProductionBackends(); err == nil || !strings.Contains(err.Error(), "docker") {
		t.Fatalf("missing Docker backend was not rejected: %v", err)
	}

	backend, _ = stubBackends(t)
	backend.SSHCmd = nil
	if err := NewUpdateExecutor(backend).ValidateProductionBackends(); err == nil || !strings.Contains(err.Error(), "ssh") {
		t.Fatalf("missing SSH backend was not rejected: %v", err)
	}
}

func TestOfflineRollout_FailClosedWhenActiveTasksNeverReachZero(t *testing.T) {
	backend, state := stubBackends(t)
	state.activeTasksZero = false
	e := NewUpdateExecutor(backend)
	e.drainTimeout = time.Millisecond
	if err := e.Execute(context.Background(), mkOp("offline-busy-worker", validImageRef(), "")); err == nil || !strings.Contains(err.Error(), "did not reach DRAINING") {
		t.Fatal("rollout did not fail closed for non-zero active_tasks")
	}
	if got, want := state.drainCalls, []bool{true, false}; !equalBools(got, want) {
		t.Fatalf("drain calls after fail-closed idle check = %v, want %v", got, want)
	}
}

func TestOfflineRollout_FailClosedWhenHealthReadyFails(t *testing.T) {
	backend, state := stubBackends(t)
	state.healthErr = errors.New("readiness unavailable")
	if err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("offline-unhealthy-worker", validImageRef(), "")); err == nil || !errors.Is(err, ErrRollbackFailed) {
		t.Fatalf("health failure did not fail closed through rollback: %v", err)
	}
}

func equalBools(got, want []bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func containsEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
