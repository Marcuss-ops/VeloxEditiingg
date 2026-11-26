package pipeline

import (
	"testing"

	"velox-server/internal/workers"
	"velox-shared/controltransport"
)

func TestHasStockWindowPreparationRequiresConnectedWorkerAndExecutor(t *testing.T) {
	registry, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "video.prepare.window.v1", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := workers.Worker{
		SessionActive:        true,
		ConnectionStatus:     workers.StatusConnected,
		ExecutorCapabilities: registry,
	}
	if !hasStockWindowPreparation(base) {
		t.Fatal("connected worker with registered preparation executor was not recognized")
	}
	base.SessionActive = false
	if hasStockWindowPreparation(base) {
		t.Fatal("disconnected session advertised stock preparation")
	}
	base.SessionActive = true
	base.ConnectionStatus = workers.StatusStale
	if hasStockWindowPreparation(base) {
		t.Fatal("stale worker advertised stock preparation")
	}
	base.ConnectionStatus = workers.StatusConnected
	base.ExecutorCapabilities = controltransport.EmptyExecutorRegistry()
	if hasStockWindowPreparation(base) {
		t.Fatal("worker without preparation executor advertised stock preparation")
	}
}

func TestHasVideoSourceCertificationRequiresConnectedWorkerAndExecutor(t *testing.T) {
	registry, err := controltransport.NewExecutorRegistry(controltransport.ExecutorCapability{ID: "video.source.certify.v1", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	worker := workers.Worker{SessionActive: true, ConnectionStatus: workers.StatusConnected, ExecutorCapabilities: registry}
	if !hasVideoSourceCertification(worker) {
		t.Fatal("connected worker with source certification executor was not recognized")
	}
	worker.ConnectionStatus = workers.StatusStale
	if hasVideoSourceCertification(worker) {
		t.Fatal("stale worker advertised source certification")
	}
	worker.ConnectionStatus = workers.StatusConnected
	worker.ExecutorCapabilities = controltransport.EmptyExecutorRegistry()
	if hasVideoSourceCertification(worker) {
		t.Fatal("worker without source certification executor advertised source certification")
	}
}
