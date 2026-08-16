package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type recordingRuntimePreflight struct {
	err   error
	calls int
}

func (p *recordingRuntimePreflight) Check(context.Context, string) error {
	p.calls++
	return p.err
}

func TestCanonicalWorkerRuntimePreflight_UsesCanonicalContract(t *testing.T) {
	var commands []string
	ssh := backendSSHFunc(func(_ context.Context, _ string, command string) (string, error) {
		commands = append(commands, command)
		return "", nil
	})
	p := &CanonicalWorkerRuntimePreflight{SSH: ssh}
	if err := p.Check(context.Background(), "worker-1"); err != nil {
		t.Fatalf("Check returned error: %v", err)
	}
	got := strings.Join(commands, "\n")
	for _, fragment := range []string{
		"/etc/velox-worker/worker.env",
		"/opt/velox-worker/compose.yml",
		"velox-worker-activate-image",
		"velox-worker.service",
		"--project-name velox-worker",
		"docker inspect",
		"127.0.0.1:8081/health/ready",
	} {
		if !strings.Contains(got, fragment) {
			t.Errorf("preflight command missing %q: %s", fragment, got)
		}
	}
	if len(commands) != 7 {
		t.Fatalf("preflight command count = %d, want 7: %v", len(commands), commands)
	}
}

func TestUpdate_RuntimePreflightFailsBeforeDrain(t *testing.T) {
	backend, state := stubBackends(t)
	preflight := &recordingRuntimePreflight{err: errors.New("compose contract missing")}
	backend.Preflight = preflight

	err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !strings.Contains(err.Error(), "runtime preflight") {
		t.Fatalf("preflight failure = %v, want runtime preflight error", err)
	}
	if preflight.calls != 1 {
		t.Fatalf("preflight calls = %d, want 1", preflight.calls)
	}
	if len(state.drainCalls) != 0 {
		t.Fatalf("preflight failure mutated drain state: %v", state.drainCalls)
	}
}

type backendSSHFunc func(context.Context, string, string) (string, error)

func (f backendSSHFunc) Run(ctx context.Context, workerID, command string) (string, error) {
	return f(ctx, workerID, command)
}
