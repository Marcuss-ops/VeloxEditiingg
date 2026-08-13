package fleet

import "testing"

func TestUpdateCapabilityStatusDisabledIsExplicitAndReady(t *testing.T) {
	status := DisabledUpdateCapability("update executor is not wired")
	if status.State != UpdateCapabilityDisabled {
		t.Fatalf("state = %q, want DISABLED", status.State)
	}
	if err := status.ReadinessError(); err != nil {
		t.Fatalf("DISABLED must not fail readiness: %v", err)
	}
}

func TestUpdateCapabilityStatusReady(t *testing.T) {
	status := ReadyUpdateCapability()
	if status.State != UpdateCapabilityReady {
		t.Fatalf("state = %q, want READY", status.State)
	}
	if err := status.ReadinessError(); err != nil {
		t.Fatalf("READY must not fail readiness: %v", err)
	}
}

func TestUpdateCapabilityStatusMisconfiguredFailsReadiness(t *testing.T) {
	status := MisconfiguredUpdateCapability("missing dependencies: docker, smoke")
	if status.State != UpdateCapabilityMisconfigured {
		t.Fatalf("state = %q, want MISCONFIGURED", status.State)
	}
	if err := status.ReadinessError(); err == nil {
		t.Fatal("MISCONFIGURED must fail readiness")
	}
}

func TestUpdateCapabilityStatusMisconfiguredRequiresReason(t *testing.T) {
	if err := MisconfiguredUpdateCapability("").Validate(); err == nil {
		t.Fatal("MISCONFIGURED without reason must be rejected")
	}
}

func TestUpdateExecutorCapabilityStatusDerivation(t *testing.T) {
	if status := (*UpdateExecutor)(nil).CapabilityStatus(); status.State != UpdateCapabilityDisabled {
		t.Fatalf("nil executor state = %q, want DISABLED", status.State)
	}

	executor := NewUpdateExecutor(UpdateBackend{})
	status := executor.CapabilityStatus()
	if status.State != UpdateCapabilityMisconfigured {
		t.Fatalf("empty-backend executor state = %q, want MISCONFIGURED", status.State)
	}
	if status.Reason == "" {
		t.Fatal("MISCONFIGURED status must carry the missing backends reason")
	}
	if status.ReadinessError() == nil {
		t.Fatal("MISCONFIGURED update executor must fail readiness")
	}
}
