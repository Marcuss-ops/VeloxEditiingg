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
