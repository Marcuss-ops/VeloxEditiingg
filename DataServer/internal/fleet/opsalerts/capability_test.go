package opsalerts

import "testing"

func TestCapabilityStatusDisabledIsExplicitAndReady(t *testing.T) {
	status := DisabledStatus("worker datasource is not wired")
	if status.State != CapabilityDisabled {
		t.Fatalf("state = %q, want %q", status.State, CapabilityDisabled)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("disabled status validation: %v", err)
	}
	if err := status.ReadinessError(); err != nil {
		t.Fatalf("disabled capability must not fail process readiness: %v", err)
	}
}

func TestCapabilityStatusReady(t *testing.T) {
	status := ReadyStatus()
	if status.State != CapabilityReady {
		t.Fatalf("state = %q, want %q", status.State, CapabilityReady)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("ready status validation: %v", err)
	}
	if err := status.ReadinessError(); err != nil {
		t.Fatalf("ready capability readiness: %v", err)
	}
}

func TestCapabilityStatusMisconfiguredFailsReadiness(t *testing.T) {
	status := MisconfiguredStatus("datasource is nil")
	if status.State != CapabilityMisconfigured {
		t.Fatalf("state = %q, want %q", status.State, CapabilityMisconfigured)
	}
	if err := status.Validate(); err == nil {
		t.Fatal("misconfigured status must validate as an error")
	}
	if err := status.ReadinessError(); err == nil {
		t.Fatal("misconfigured status must fail readiness")
	}
}

func TestCapabilityStatusMisconfiguredRequiresReason(t *testing.T) {
	if err := (CapabilityStatus{State: CapabilityMisconfigured}).Validate(); err == nil {
		t.Fatal("misconfigured status without reason must fail validation")
	}
}
