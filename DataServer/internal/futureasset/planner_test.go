package futureasset

import (
	"testing"
	"time"

	shared "velox-shared/futureasset"
)

func TestPlannerVersionsWorkerScopedSnapshots(t *testing.T) {
	p := NewPlanner()
	jobs := []shared.Job{{JobID: "job-1", TaskID: "task-1", ReservationID: "reservation-1", Assets: []shared.AssetManifest{{AssetKey: "asset-1", SHA256: "sha", SizeBytes: 10}}}}
	a, err := p.Build("worker-a", "current", "plan-a", jobs, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Build("worker-a", "current", "plan-b", jobs, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if a.Version != 1 || b.Version != 2 || a.WorkerID != "worker-a" || len(a.PrefetchJobs) != 1 {
		t.Fatalf("unexpected plans: %#v %#v", a, b)
	}
}
