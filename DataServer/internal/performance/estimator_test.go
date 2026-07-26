package performance

import "testing"

func TestConservativeEstimatorIncludesTransferAndUpload(t *testing.T) {
	e := ConservativeEstimator{}.Estimate(Workload{InputDurationMs: 1000, MissingAssetBytes: 1_000_000, UploadBytes: 2_000_000, BandwidthMbps: 100, CPUThreads: 4}, nil, WorkerEconomics{})
	if e.TransferMs <= 0 || e.UploadMs <= e.TransferMs/2 || e.FinishMs <= e.ComputeMs {
		t.Fatalf("estimate=%+v", e)
	}
}

func TestRegistryUsesExecutorVersion(t *testing.T) {
	r := NewRegistry()
	r.Register("scene.composite.v1", 1, fixedEstimator{ms: 42})
	e := r.Estimate(Workload{ExecutorID: "scene.composite.v1", ExecutorVersion: 1}, nil, WorkerEconomics{})
	if e.ComputeMs != 42 {
		t.Fatalf("estimate=%+v", e)
	}
}

type fixedEstimator struct{ ms float64 }

func (f fixedEstimator) Estimate(Workload, *Baseline, WorkerEconomics) Estimate {
	return Estimate{ComputeMs: f.ms}
}
