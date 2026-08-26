package workers

import (
	"testing"
	"time"
)

func TestConnectionStatusForInfo_HydratesIndependentStateDimensions(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
		LastHB:          freshHB(now, 30*time.Second),
		Drain:           true,
		Quarantined:     false,
		DeploymentState: DeploymentUpdating,
		Metrics: map[string]interface{}{
			"active_tasks": float64(4),
		},
	}

	ConnectionStatusForInfo(info, true, now)

	if info.ConnectionState != ConnectionConnected {
		t.Fatalf("ConnectionState = %q, want %q", info.ConnectionState, ConnectionConnected)
	}
	if info.SchedulingState != SchedulingDraining {
		t.Fatalf("SchedulingState = %q, want %q", info.SchedulingState, SchedulingDraining)
	}
	if info.DeploymentState != DeploymentUpdating {
		t.Fatalf("DeploymentState = %q, want %q", info.DeploymentState, DeploymentUpdating)
	}
	if info.HealthState != HealthDegraded {
		t.Fatalf("HealthState = %q, want %q", info.HealthState, HealthDegraded)
	}
	if info.ConnectionStatus != StatusDraining {
		t.Fatalf("legacy ConnectionStatus = %q, want %q", info.ConnectionStatus, StatusDraining)
	}
	if info.Health != WorkerHealthUpdating {
		t.Fatalf("legacy Health = %q, want %q", info.Health, WorkerHealthUpdating)
	}
}

func TestConnectionStatusForInfo_DoesNotUseLegacyProjectionAsInput(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
		LastHB:           freshHB(now, 30*time.Second),
		SessionActive:    true,
		ConnectionStatus: StatusDisconnected,
		Health:           WorkerHealthOffline,
		DeploymentState:  DeploymentCurrent,
	}

	ConnectionStatusForInfo(info, true, now)

	if info.ConnectionState != ConnectionConnected {
		t.Fatalf("ConnectionState = %q, want %q; stale legacy fields leaked into derivation", info.ConnectionState, ConnectionConnected)
	}
	if info.HealthState != HealthHealthy {
		t.Fatalf("HealthState = %q, want %q; stale legacy fields leaked into derivation", info.HealthState, HealthHealthy)
	}
	if info.ConnectionStatus != StatusConnected || info.Health != WorkerHealthHealthy {
		t.Fatalf("legacy projections were not refreshed: status=%q health=%q", info.ConnectionStatus, info.Health)
	}
}

func TestHealthForInfo_PreservesTypedDeploymentWhenLegacyInputIsEmpty(t *testing.T) {
	now := canonicalNow()
	info := &Worker{
		LastHB:          freshHB(now, 30*time.Second),
		SessionActive:   true,
		DeploymentState: DeploymentCurrent,
	}

	HealthForInfo(info, time.Time{}, "", now)

	if info.DeploymentState != DeploymentCurrent {
		t.Fatalf("DeploymentState = %q, want preserved %q", info.DeploymentState, DeploymentCurrent)
	}
	if info.HealthState != HealthHealthy {
		t.Fatalf("HealthState = %q, want %q", info.HealthState, HealthHealthy)
	}
}

func TestNormalizeDeploymentStateRejectsUnknownValues(t *testing.T) {
	if got := NormalizeDeploymentState("legacy-busy"); got != DeploymentNone {
		t.Fatalf("NormalizeDeploymentState unknown = %q, want %q", got, DeploymentNone)
	}
	if got := NormalizeDeploymentState(string(DeploymentRollback)); got != DeploymentRollback {
		t.Fatalf("NormalizeDeploymentState valid = %q, want %q", got, DeploymentRollback)
	}
}
