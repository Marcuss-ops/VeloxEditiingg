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
	if b.Version <= a.Version || a.WorkerID != "worker-a" || len(a.PrefetchJobs) != 1 {
		t.Fatalf("unexpected plans: %#v %#v", a, b)
	}
}

func TestPlannerVersionSurvivesMasterRestartClockEpoch(t *testing.T) {
	first := NewPlanner()
	first.now = func() time.Time { return time.Unix(1_800_000_000, 0) }
	second := NewPlanner()
	second.now = func() time.Time { return time.Unix(1_800_000_001, 0) }
	jobs := []shared.Job{{JobID: "job-1", TaskID: "task-1", ReservationID: "reservation-1", Assets: []shared.AssetManifest{{AssetKey: "asset-1", SHA256: "sha", SizeBytes: 10}}}}
	a, err := first.Build("worker-a", "current", "plan-a", jobs, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Build("worker-a", "current", "plan-a", jobs, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if b.Version <= a.Version {
		t.Fatalf("restart produced stale version: first=%d second=%d", a.Version, b.Version)
	}
}
