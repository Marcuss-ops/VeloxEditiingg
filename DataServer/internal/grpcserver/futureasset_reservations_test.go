package grpcserver

import (
	"context"
	"testing"

	"velox-server/internal/placement"
)

func TestPrioritizeCurrentJobCandidates(t *testing.T) {
	candidates := []placement.TaskCandidate{
		{TaskID: "old-1", JobID: "job-old-1"},
		{TaskID: "current", JobID: "job-current"},
		{TaskID: "old-2", JobID: "job-old-2"},
	}

	got := prioritizeCurrentJobCandidates(candidates, "job-current")
	if got[0].TaskID != "current" {
		t.Fatalf("current job candidate first = %q, want current", got[0].TaskID)
	}
	if got[1].TaskID != "old-1" || got[2].TaskID != "old-2" {
		t.Fatalf("non-current candidate order changed: %+v", got)
	}
}

func TestPrioritizeCurrentJobCandidatesWithoutCurrentJobPreservesInput(t *testing.T) {
	candidates := []placement.TaskCandidate{{TaskID: "old-1", JobID: "job-old-1"}}
	got := prioritizeCurrentJobCandidates(candidates, "job-missing")
	if len(got) != 1 || got[0].TaskID != "old-1" {
		t.Fatalf("candidate list changed without current job: %+v", got)
	}
}

func TestLatestFutureTaskPayloadUsesFinalizedRuntimeAssets(t *testing.T) {
	prepared := []byte(`{"runtime_assets_pending":true}`)
	finalized := []byte(`{"runtime_assets":[{"asset_id":"fragment-1","role":"stock","sha256":"abc","size_bytes":42,"source_uri":"https://objects.example/fragment?sig=opaque"}]}`)
	store := &mockFutureReservationStore{payload: finalized}

	assets := futureAssetManifests(latestFutureTaskPayload(context.Background(), store, "task-1", prepared))
	if len(assets) != 1 {
		t.Fatalf("finalized asset manifests=%d, want 1", len(assets))
	}
	if assets[0].AssetID != "fragment-1" || assets[0].SourceURI != "https://objects.example/fragment?sig=opaque" {
		t.Fatalf("reservation refresh did not use FINALIZE payload: %+v", assets[0])
	}
}
