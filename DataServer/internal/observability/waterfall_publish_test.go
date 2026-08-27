package observability

import (
	"testing"
	sharedtelemetry "velox-shared/telemetry"
)

func TestBuildAttemptWaterfallIncludesPublishBreakdown(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestonePublishSlotWaitStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestonePublishSlotWaitCompleted, ElapsedMS: 12},
		{Name: sharedtelemetry.MilestonePublishDeclareStarted, ElapsedMS: 12},
		{Name: sharedtelemetry.MilestonePublishDeclareCompleted, ElapsedMS: 20},
		{Name: sharedtelemetry.MilestonePublishUploadStarted, ElapsedMS: 20},
		{Name: sharedtelemetry.MilestonePublishUploadCompleted, ElapsedMS: 120},
		{Name: sharedtelemetry.MilestonePublishRemoteFinalizeStarted, ElapsedMS: 120},
		{Name: sharedtelemetry.MilestonePublishRemoteFinalizeCompleted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePublishCommitWaitStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePublishCommitWaitCompleted, ElapsedMS: 135},
		{Name: sharedtelemetry.MilestonePublishSpoolCommitStarted, ElapsedMS: 135},
		{Name: sharedtelemetry.MilestonePublishSpoolCommitCompleted, ElapsedMS: 140},
	}
	got := BuildAttemptWaterfall("a", samples, 140)
	if got.Publish == nil {
		t.Fatal("publish breakdown is nil")
	}
	if got.Publish.UploadMS != 100 || got.Publish.DeclareMS != 8 || got.Publish.SpoolCommitMS != 5 {
		t.Fatalf("publish breakdown = %+v", got.Publish)
	}
}
