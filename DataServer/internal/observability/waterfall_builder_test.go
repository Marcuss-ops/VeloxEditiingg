package observability

import (
	"testing"

	sharedtelemetry "velox-shared/telemetry"
)

func TestBuildAttemptWaterfallPartitionsAttemptWall(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		{Name: sharedtelemetry.MilestoneAllAssetsReady, ElapsedMS: 125},
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	got := BuildAttemptWaterfall("attempt-1", samples, 330)
	if got.AccountedMS != got.WallMS || got.UnaccountedMS != 0 || got.CoveragePct != 100 {
		t.Fatalf("waterfall = %+v, want complete wall-clock coverage", got)
	}
	if len(got.Buckets) != 12 {
		t.Fatalf("bucket count = %d, want 12", len(got.Buckets))
	}
	for i := 1; i < len(got.Buckets); i++ {
		if got.Buckets[i].StartMS != got.Buckets[i-1].EndMS {
			t.Fatalf("bucket %d starts at %d after bucket %d ends at %d", i, got.Buckets[i].StartMS, i-1, got.Buckets[i-1].EndMS)
		}
	}
}

// TestBuildAttemptWaterfall_MissingMilestoneNeverFabricated locks the STEP C
// "unknown instead of inventing" rule: when a mid-timeline boundary milestone
// (here assets.all_ready) is absent, the spanning bucket must be SKIPPED and
// reported as missing — never silently attributed to a neighbouring bucket.
func TestBuildAttemptWaterfall_MissingMilestoneNeverFabricated(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		// assets.all_ready deliberately absent.
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	got := BuildAttemptWaterfall("attempt-missing", samples, 330)

	// The asset_preparation bucket must not exist: it spans the missing
	// milestone and would require inventing assets.all_ready.
	for _, bucket := range got.Buckets {
		if bucket.Name == "asset_preparation" {
			t.Fatalf("asset_preparation bucket was fabricated despite missing assets.all_ready: %+v", bucket)
		}
	}

	// The truly unknown stretch must surface as unaccounted ms and a missing
	// milestone, not be hidden or misattributed.
	if got.UnaccountedMS == 0 {
		t.Fatal("missing milestone produced no unaccounted_ms")
	}
	if !containsStr(got.MissingMilestones, string(sharedtelemetry.MilestoneAllAssetsReady)) {
		t.Fatalf("missing_milestones = %v, want assets.all_ready present", got.MissingMilestones)
	}
	if got.CoveragePct >= 100 {
		t.Fatalf("coverage_pct = %f, want < 100 when a milestone is missing", got.CoveragePct)
	}

	// assets.all_ready is the boundary for BOTH asset_preparation and
	// pre_plan_wait, so neither bucket may exist; the timeline resumes at
	// plan_compile (the first bucket after the missing boundary).
	for _, name := range []string{"asset_preparation", "pre_plan_wait"} {
		for _, bucket := range got.Buckets {
			if bucket.Name == name {
				t.Fatalf("%s bucket fabricated despite missing assets.all_ready: %+v", name, bucket)
			}
		}
	}
	hasPlanCompile := false
	for _, bucket := range got.Buckets {
		if bucket.Name == "plan_compile" {
			hasPlanCompile = true
			break
		}
	}
	if !hasPlanCompile {
		t.Fatal("plan_compile bucket missing after a mid-timeline gap")
	}
}

func containsStr(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func TestDecodeAttemptWaterfallAcceptsProtoJSONMilestones(t *testing.T) {
	raw := `{"milestones":[{"name":"attempt.accepted","sequence":"1","elapsedMs":"0"},{"name":"execution.started","sequence":"2","elapsedMs":"5"},{"name":"attempt.completed","sequence":"3","elapsedMs":"10"}]}`
	got := decodeAttemptWaterfall(raw, "attempt-2", 10)
	if got == nil || got.WallMS != 10 {
		t.Fatalf("decoded waterfall = %+v, want non-nil wall 10", got)
	}
	if len(got.MissingMilestones) == 0 {
		t.Fatal("expected missing milestone diagnostics for incomplete timeline")
	}
}
