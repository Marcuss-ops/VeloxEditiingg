package assembly

import (
	"strings"
	"testing"
)

func finalArtifact(jobID, assetID, producer string) PublishedArtifact {
	return PublishedArtifact{
		JobID: jobID, TimelineRevision: 7, AssetID: assetID,
		StorageURL: "https://example.test/" + assetID, SHA256: strings.Repeat("a", 64),
		SizeBytes: 42, ProfileID: CanonicalAssemblyProfileLegacyV1, Producer: AssetProducer(producer),
	}
}

func finalBase() FinalAssemblyManifest {
	return FinalAssemblyManifest{
		ContractVersion: ContractVersion, JobID: "job-final", Revision: 1,
		PreparationHash: "sha256:" + strings.Repeat("0", 64), TimelineRevision: 7,
		TimelineHash: "sha256:" + strings.Repeat("t", 64), ExpectedProfile: CanonicalAssemblyProfileLegacyV1,
		Artifacts: []PublishedArtifact{},
	}
}

func finalDelta(revision uint64, artifacts ...PublishedArtifact) FinalManifestDelta {
	return FinalManifestDelta{
		ContractVersion: ContractVersion, JobID: "job-final", BaseRevision: revision - 1,
		Revision: revision, PreparationHash: finalBase().PreparationHash, TimelineRevision: 7,
		TimelineHash: finalBase().TimelineHash, ExpectedProfile: CanonicalAssemblyProfileLegacyV1,
		UpsertedArtifacts: artifacts,
	}
}

func TestFinalManifestDeltaAppliesChrononArtifactAndIsReplayIdempotent(t *testing.T) {
	base := finalBase()
	artifact := finalArtifact(base.JobID, "scene-003", string(ProducerChronon))
	delta := finalDelta(2, artifact)
	if err := base.Validate(); err != nil {
		t.Fatalf("base Validate() error = %v", err)
	}
	if err := delta.Validate(); err != nil {
		t.Fatalf("delta Validate() error = %v", err)
	}
	first, disposition, err := base.ApplyDelta(delta)
	if err != nil {
		t.Fatalf("ApplyDelta() error = %v", err)
	}
	if disposition != RevisionApplied || first.Revision != 2 || len(first.Artifacts) != 1 {
		t.Fatalf("applied result = %#v disposition=%q", first, disposition)
	}
	replay, disposition, err := first.ApplyDelta(delta)
	if err != nil {
		t.Fatalf("replay ApplyDelta() error = %v", err)
	}
	if disposition != RevisionReplay || replay.LastDeltaHash != first.LastDeltaHash {
		t.Fatalf("replay = %#v disposition=%q", replay, disposition)
	}
}

func TestFinalManifestDeltaMergesAndInvalidatesWithoutTouchingOtherArtifacts(t *testing.T) {
	base := finalBase()
	a := finalArtifact(base.JobID, "scene-a", string(ProducerChronon))
	b := finalArtifact(base.JobID, "scene-b", string(ProducerRenderingGen))
	first, _, err := base.ApplyDelta(finalDelta(2, a, b))
	if err != nil {
		t.Fatal(err)
	}
	bReplacement := b
	bReplacement.SHA256 = strings.Repeat("b", 64)
	secondDelta := finalDelta(3, bReplacement)
	secondDelta.InvalidatedArtifactIDs = []string{"scene-a"}
	second, disposition, err := first.ApplyDelta(secondDelta)
	if err != nil {
		t.Fatalf("merge/invalidate error = %v", err)
	}
	if disposition != RevisionApplied || len(second.Artifacts) != 1 || second.Artifacts[0].AssetID != "scene-b" || second.Artifacts[0].SHA256 != bReplacement.SHA256 {
		t.Fatalf("merged manifest = %#v disposition=%q", second, disposition)
	}
}

func TestFinalManifestDeltaRejectsRevisionAndPreparationConflicts(t *testing.T) {
	base := finalBase()
	artifact := finalArtifact(base.JobID, "scene-003", string(ProducerChronon))
	applied, _, err := base.ApplyDelta(finalDelta(2, artifact))
	if err != nil {
		t.Fatal(err)
	}
	stale := finalDelta(2, artifact)
	stale.BaseRevision = 0
	if _, _, err := applied.ApplyDelta(stale); err == nil {
		t.Fatal("expected same-revision delta with different base to be rejected")
	}
	conflict := finalDelta(3, artifact)
	conflict.PreparationHash = "sha256:" + strings.Repeat("f", 64)
	if _, _, err := applied.ApplyDelta(conflict); err == nil || !strings.Contains(err.Error(), "preparation_hash") {
		t.Fatalf("preparation conflict = %v", err)
	}
}

func TestPublishedArtifactRequiresIntegrityAndProducerIdentity(t *testing.T) {
	artifact := finalArtifact("job", "scene", string(ProducerChronon))
	artifact.SizeBytes = 0
	if err := artifact.Validate(); err == nil {
		t.Fatal("zero-sized artifact must be rejected")
	}
	artifact = finalArtifact("job", "scene", "unknown")
	if err := artifact.Validate(); err == nil {
		t.Fatal("unknown producer must be rejected")
	}
}
