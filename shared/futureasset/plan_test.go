package futureasset

import (
	"testing"
	"time"
)

func TestBuildSeparatesPrefetchAndProtectionHorizons(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	jobs := make([]Job, 10)
	for i := range jobs {
		jobs[i] = Job{JobID: "job-" + string(rune('a'+i)), TaskID: "task-" + string(rune('a'+i)), ReservationID: "res-" + string(rune('a'+i)), Assets: []AssetManifest{{AssetKey: "asset-" + string(rune('a'+i)), SHA256: "sha", SizeBytes: 10}}}
	}
	plan, err := Build(PlannerInput{Version: 1, PlanID: "plan-1", WorkerID: "worker-1", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), FutureJobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(plan.PrefetchJobs); got != 3 {
		t.Fatalf("prefetch jobs=%d, want 3", got)
	}
	if got := len(plan.Protect); got != 10 {
		t.Fatalf("protected assets=%d, want 10", got)
	}
	if plan.PrefetchJobs[0].Distance != 1 || plan.PrefetchJobs[2].Distance != 3 {
		t.Fatalf("unexpected distances: %#v", plan.PrefetchJobs)
	}
}

func TestBuildDeduplicatesProtectedAssetsAndCanonicalizesJobAssets(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	plan, err := Build(PlannerInput{Version: 2, PlanID: "plan-2", WorkerID: "worker-1", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), FutureJobs: []Job{
		{JobID: "job-1", TaskID: "task-1", ReservationID: "res-1", Assets: []AssetManifest{{AssetKey: "b", SHA256: "b", SizeBytes: 2}, {AssetKey: "a", SHA256: "a", SizeBytes: 1}, {AssetKey: "a", SHA256: "a", SizeBytes: 1}}},
		{JobID: "job-2", TaskID: "task-2", ReservationID: "res-2", Assets: []AssetManifest{{AssetKey: "b", SHA256: "b", SizeBytes: 2}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.PrefetchJobs[0].Assets) != 2 || plan.PrefetchJobs[0].Assets[0].AssetKey != "a" {
		t.Fatalf("assets were not canonicalized: %#v", plan.PrefetchJobs[0].Assets)
	}
	if len(plan.Protect) != 2 || plan.Protect[1].AssetKey != "b" || plan.Protect[1].FutureRefCount != 2 || plan.Protect[1].NextUseDistance != 1 {
		t.Fatalf("unexpected protection aggregate: %#v", plan.Protect)
	}
}

func TestBuildRequiresIntegrityManifestForPrefetch(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	_, err := Build(PlannerInput{Version: 1, PlanID: "plan", WorkerID: "worker", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), FutureJobs: []Job{{JobID: "job", TaskID: "task", ReservationID: "res", Assets: []AssetManifest{{AssetKey: "asset", SizeBytes: 1}}}}})
	if err == nil {
		t.Fatal("expected incomplete integrity manifest to be rejected")
	}
}

func TestProtoRoundTrip(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	want, err := Build(PlannerInput{Version: 7, PlanID: "plan", WorkerID: "worker", GeneratedAt: now, ExpiresAt: now.Add(time.Minute), CurrentJob: "current", FutureJobs: []Job{{JobID: "job", TaskID: "task", ReservationID: "res", Assets: []AssetManifest{{AssetKey: "asset", SHA256: "sha", SizeBytes: 4}}}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := FromProto(want.ToProto())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || got.WorkerID != want.WorkerID || got.PrefetchJobs[0].Assets[0].SHA256 != "sha" {
		t.Fatalf("round trip lost fields: %#v", got)
	}
}
