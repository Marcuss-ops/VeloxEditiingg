package pipeline

import (
	"testing"

	"velox-server/internal/workers"
	"velox-shared/controltransport"
)

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
