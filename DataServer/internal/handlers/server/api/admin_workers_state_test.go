package api

import (
	"testing"

	workersreg "velox-server/internal/workers"
)

func TestBuildWorkerCard_CanonicalStateDimensions(t *testing.T) {
	info := makeCardInfo("w-state", func(i *workersreg.Worker) {
		i.ConnectionState = workersreg.ConnectionStale
		i.SchedulingState = workersreg.SchedulingBusy
		i.DeploymentState = workersreg.DeploymentRollback
		i.HealthState = workersreg.HealthDegraded
		// Deliberately contradictory legacy projections: the DTO must use
		// the typed dimensions for the new fields.
		i.ConnectionStatus = workersreg.StatusConnected
		i.Health = workersreg.WorkerHealthHealthy
	})

	card := buildWorkerCard(&info)
	if card.ConnectionState != string(workersreg.ConnectionStale) {
		t.Fatalf("ConnectionState = %q, want %q", card.ConnectionState, workersreg.ConnectionStale)
	}
	if card.SchedulingState != string(workersreg.SchedulingBusy) {
		t.Fatalf("SchedulingState = %q, want %q", card.SchedulingState, workersreg.SchedulingBusy)
	}
	if card.DeploymentState != string(workersreg.DeploymentRollback) {
		t.Fatalf("DeploymentState = %q, want %q", card.DeploymentState, workersreg.DeploymentRollback)
	}
	if card.HealthState != string(workersreg.HealthDegraded) {
		t.Fatalf("HealthState = %q, want %q", card.HealthState, workersreg.HealthDegraded)
	}
}
