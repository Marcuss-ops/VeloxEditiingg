package fleet

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"velox-server/internal/store"
)

type workerConfigSSHStub struct{ command string }

func (s *workerConfigSSHStub) Run(_ context.Context, _ string, command string) (string, error) {
	s.command = command
	return "ok", nil
}

func TestWorkerConfigExecutorUsesOnlyAllowlistedHelperArguments(t *testing.T) {
	profile := 1
	payload, err := json.Marshal(WorkerConfigPayload{AudioMixStrategy: "optimized", AudioMixProfile: &profile})
	if err != nil {
		t.Fatal(err)
	}
	ssh := &workerConfigSSHStub{}
	exec := NewWorkerConfigExecutor(ssh)
	if err := exec.Execute(context.Background(), &store.Operation{WorkerID: "worker-1", Payload: payload}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(ssh.command, "sudo -n /usr/local/sbin/velox-worker-set-config") ||
		!strings.Contains(ssh.command, "--audio-mix-strategy optimized") ||
		!strings.Contains(ssh.command, "--audio-mix-profile 1") {
		t.Fatalf("unexpected helper command: %q", ssh.command)
	}
}

// TestWorkerConfigExecutorFMP4StreamProfileArgument pins the canonical
// rollout knob for the fMP4 admission gate: it must reach the root-owned
// helper as a boolean toggle and nothing else.
func TestWorkerConfigExecutorFMP4StreamProfileArgument(t *testing.T) {
	for _, want := range []int{0, 1} {
		toggle := want
		payload, err := json.Marshal(WorkerConfigPayload{FMP4StreamProfile: &toggle})
		if err != nil {
			t.Fatal(err)
		}
		ssh := &workerConfigSSHStub{}
		if err := NewWorkerConfigExecutor(ssh).Execute(context.Background(), &store.Operation{WorkerID: "worker-1", Payload: payload}); err != nil {
			t.Fatalf("Execute(fmp4=%d): %v", want, err)
		}
		if !strings.Contains(ssh.command, "sudo -n /usr/local/sbin/velox-worker-set-config --fmp4-stream-profile "+strconv.Itoa(want)) {
			t.Fatalf("fmp4=%d helper command = %q", want, ssh.command)
		}
	}
}

// TestWorkerConfigExecutorRejectsNonBooleanFMP4Toggle keeps the knob from
// becoming an arbitrary environment write (the helper only accepts 0|1).
func TestWorkerConfigExecutorRejectsNonBooleanFMP4Toggle(t *testing.T) {
	bad := 2
	payload, err := json.Marshal(WorkerConfigPayload{FMP4StreamProfile: &bad})
	if err != nil {
		t.Fatal(err)
	}
	ssh := &workerConfigSSHStub{}
	if err := NewWorkerConfigExecutor(ssh).Execute(context.Background(), &store.Operation{WorkerID: "worker-1", Payload: payload}); err == nil {
		t.Fatal("Execute accepted fmp4_stream_profile=2")
	}
	if ssh.command != "" {
		t.Fatalf("SSH invoked for rejected payload: %q", ssh.command)
	}
}

func TestWorkerConfigExecutorRejectsUnsupportedValueBeforeSSH(t *testing.T) {
	payload, err := json.Marshal(WorkerConfigPayload{AudioMixStrategy: "$(touch /tmp/should-not-run)"})
	if err != nil {
		t.Fatal(err)
	}
	ssh := &workerConfigSSHStub{}
	if err := NewWorkerConfigExecutor(ssh).Execute(context.Background(), &store.Operation{WorkerID: "worker-1", Payload: payload}); err == nil {
		t.Fatal("Execute accepted unsupported strategy")
	}
	if ssh.command != "" {
		t.Fatalf("SSH invoked for rejected payload: %q", ssh.command)
	}
}
