package fleet

import "testing"

func TestSmokeCapabilityDisabledIsValidAndNonBlocking(t *testing.T) {
	status := DisabledSmokeCapability("real asset resolver is not wired")
	if status.State != SmokeCapabilityDisabled {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityDisabled)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("disabled status validation: %v", err)
	}
	if err := status.ReadinessError(); err != nil {
		t.Fatalf("disabled capability must not fail readiness: %v", err)
	}
}

func TestSmokeCapabilityReady(t *testing.T) {
	status := ReadySmokeCapability()
	if status.State != SmokeCapabilityReady {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityReady)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("ready status validation: %v", err)
	}
}

func TestSmokeCapabilityMisconfiguredFailsReadiness(t *testing.T) {
	status := MisconfiguredSmokeCapability("registration failed")
	if status.State != SmokeCapabilityMisconfigured {
		t.Fatalf("state = %q, want %q", status.State, SmokeCapabilityMisconfigured)
	}
	if err := status.ReadinessError(); err == nil {
		t.Fatal("misconfigured capability must fail readiness")
	}
}
