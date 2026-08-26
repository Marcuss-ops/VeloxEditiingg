// Package fleet — AZIONE 2 fail-closed capability verdict tests.
//
// The UpdateExecutor exposes a structured Capability() verdict used
// by three fail-closed surfaces:
//   - the /ready "update-capability" probe (boot signal),
//   - the POST /update 503 gate (refuse before publish),
//   - the operator boot log (names the missing backends).
//
// These tests pin the verdict semantics: Ready requires ALL critical
// backends (SSH, Docker, Deployments, Cosign, Image, Registry, Runtime,
// Preflight, Smoke, Drive), the Missing list uses the same grep-friendly
// vocabulary as
// ValidateProductionBackends, and AttachRuntimeBackends is the wiring
// that flips a partially-wired executor to READY.
package fleet

import (
	"context"
	"strings"
	"testing"
)

func TestUpdateCapability_FullyWired_Ready(t *testing.T) {
	backend, _ := stubBackends(t)
	e := NewUpdateExecutor(backend)

	capability := e.Capability()
	if !capability.Ready {
		t.Fatalf("fully-wired executor capability = %+v, want Ready", capability)
	}
	if len(capability.Missing) != 0 {
		t.Fatalf("fully-wired executor reports missing %v, want none", capability.Missing)
	}
	if !e.Ready() {
		t.Fatal("Ready() = false for fully-wired executor")
	}
	if err := e.ValidateProductionBackends(); err != nil {
		t.Fatalf("ValidateProductionBackends on fully-wired executor: %v", err)
	}
}

func TestUpdateCapability_MissingBackend_NotReady(t *testing.T) {
	tests := []struct {
		name    string
		unwire  func(b *UpdateBackend)
		wantKey string
	}{
		{name: "ssh", unwire: func(b *UpdateBackend) { b.SSHCmd = nil }, wantKey: "ssh"},
		{name: "docker", unwire: func(b *UpdateBackend) { b.Docker = nil }, wantKey: "docker"},
		{name: "deployments", unwire: func(b *UpdateBackend) { b.Deployments = nil }, wantKey: "deployments"},
		{name: "cosign", unwire: func(b *UpdateBackend) { b.Cosign = nil }, wantKey: "cosign"},
		{name: "image", unwire: func(b *UpdateBackend) { b.Image = nil }, wantKey: "image"},
		{name: "registry", unwire: func(b *UpdateBackend) { b.Registry = nil }, wantKey: "registry"},
		{name: "runtime_preflight", unwire: func(b *UpdateBackend) { b.Preflight = nil }, wantKey: "runtime_preflight"},
		{name: "smoke", unwire: func(b *UpdateBackend) { b.Smoke = nil }, wantKey: "smoke"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, _ := stubBackends(t)
			tt.unwire(&backend)
			e := NewUpdateExecutor(backend)

			capability := e.Capability()
			if capability.Ready {
				t.Fatalf("capability Ready=true despite missing %s", tt.wantKey)
			}
			if !containsString(capability.Missing, tt.wantKey) {
				t.Fatalf("Missing = %v, want it to contain %q", capability.Missing, tt.wantKey)
			}
			if e.Ready() {
				t.Fatal("Ready() = true despite missing backend")
			}
			if err := e.ValidateProductionBackends(); err == nil || !strings.Contains(err.Error(), tt.wantKey) {
				t.Fatalf("ValidateProductionBackends err = %v, want mention of %q", err, tt.wantKey)
			}
		})
	}
}

func TestUpdateCapability_NilExecutor_NotReady(t *testing.T) {
	var e *UpdateExecutor

	capability := e.Capability()
	if capability.Ready {
		t.Fatal("nil executor reported Ready")
	}
	if !containsString(capability.Missing, "executor") {
		t.Fatalf("nil executor Missing = %v, want [executor]", capability.Missing)
	}
	if err := e.ValidateProductionBackends(); err == nil {
		t.Fatal("nil executor ValidateProductionBackends must error")
	}
}

// TestUpdateCapability_FlipsReadyAfterAttachRuntimeBackends locks the
// boot wiring path: buildFleet constructs the executor with SSH/Docker/
// Cosign/Registry/Deployments/Image, then AttachRuntimeBackends attaches
// the worker-local smoke runner, and only then does Capability flip to READY.
func TestUpdateCapability_FlipsReadyAfterAttachRuntimeBackends(t *testing.T) {
	backend, _ := stubBackends(t)
	backend.Smoke = nil
	backend.Drive = nil
	e := NewUpdateExecutor(backend)

	if e.Ready() {
		t.Fatal("executor without smoke reported Ready")
	}
	if err := e.AttachRuntimeBackends(stubSmokeRunner{}, stubDriveVerifier{}); err != nil {
		t.Fatalf("AttachRuntimeBackends: %v", err)
	}
	if !e.Ready() {
		t.Fatalf("executor after AttachRuntimeBackends not Ready: %+v", e.Capability())
	}
	if err := e.ValidateProductionBackends(); err != nil {
		t.Fatalf("ValidateProductionBackends after attach: %v", err)
	}
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// stubSmokeRunner is a minimal BackendSmokeRunner for capability tests
// that need a non-nil smoke backend without standing up a full executor.
type stubSmokeRunner struct{}

func (stubSmokeRunner) RunLevelD(_ context.Context, _ string) (string, error) {
	return "cap-test-artifact", nil
}

// stubDriveVerifier is a minimal BackendDriveVerifier for capability
// tests that need a non-nil Drive backend.
type stubDriveVerifier struct{}

func (stubDriveVerifier) VerifyDelivery(_ context.Context, _ string, _ int64) error { return nil }
