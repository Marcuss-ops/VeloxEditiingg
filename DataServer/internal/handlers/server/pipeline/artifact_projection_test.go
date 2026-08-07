package pipeline

import (
	"testing"

	"velox-server/internal/store"
)

func TestSelectPrimaryReadyArtifact(t *testing.T) {
	artifacts := []store.Artifact{
		{ID: "sidecar", JobID: "job", Type: "engine_progress_sidecar", Status: "READY", CreatedAt: "2026-08-07T12:02:00Z"},
		{ID: "staging-video", JobID: "job", Type: "final_video", Status: "STAGING", CreatedAt: "2026-08-07T12:03:00Z"},
		{ID: "final", JobID: "job", Type: "final_video", Status: "READY", CreatedAt: "2026-08-07T12:01:00Z"},
	}
	got := selectPrimaryReadyArtifact(artifacts)
	if got == nil || got.ID != "final" {
		t.Fatalf("primary artifact = %#v, want READY final_video", got)
	}
}

func TestSelectPrimaryReadyArtifactIgnoresFailedAndQuarantined(t *testing.T) {
	artifacts := []store.Artifact{
		{ID: "failed", Type: "final_video", Status: "FAILED"},
		{ID: "quarantined", Type: "video/mp4", Status: "QUARANTINED"},
	}
	if got := selectPrimaryReadyArtifact(artifacts); got != nil {
		t.Fatalf("primary artifact = %#v, want nil", got)
	}
}
