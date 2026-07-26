package performance

import "testing"

func TestChooseWorkerPenalizesUncertaintyAndRisk(t *testing.T) {
	d := ChooseWorker([]PlacementCandidate{
		{WorkerID: "fast-unknown", Estimate: Estimate{FinishMs: 100, Confidence: .1}},
		{WorkerID: "steady", Estimate: Estimate{FinishMs: 120, Confidence: .95}},
		{WorkerID: "rejected", Rejected: "memory_pressure"},
	})
	if d.SelectedWorker != "steady" {
		t.Fatalf("decision=%+v", d)
	}
}
