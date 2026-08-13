package preparedassets

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/repository"
	"velox-shared/contract"
)

type artifactReaderStub struct{ artifact *repository.Artifact }

func (s artifactReaderStub) GetByID(context.Context, string) (*repository.Artifact, error) {
	return s.artifact, nil
}

func validManifest() contract.PreparedVideoFragmentManifestV1 {
	return contract.PreparedVideoFragmentManifestV1{
		SchemaVersion: 1, ArtifactID: "artifact-1", AssetKey: "sha256:asset-1",
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 10,
		ProfileID: contract.CanonicalVideoProfileIDV1, DurationUS: 1_000_000, FrameCount: 30,
		TimelineRevision: 4, TimelineSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		TimelineStartFrame: 0, FirstFrameKeyframe: true, ClosedGOP: true,
	}
}

func TestResolverAdmitsOnlyReadyMatchingArtifact(t *testing.T) {
	manifest := validManifest()
	reader := artifactReaderStub{artifact: &repository.Artifact{ID: manifest.ArtifactID, Type: "video", Status: "READY", SHA256: manifest.SHA256, SizeBytes: manifest.SizeBytes}}
	got, err := NewResolver(reader).Admit(context.Background(), manifest)
	if err != nil || got.State != StateReady {
		t.Fatalf("Admit = %+v, %v; want READY", got, err)
	}
}

func TestResolverRegisterPreparedArtifactUsesExistingAuthority(t *testing.T) {
	manifest := validManifest()
	reader := artifactReaderStub{artifact: &repository.Artifact{ID: manifest.ArtifactID, Type: "video", Status: "READY", SHA256: manifest.SHA256, SizeBytes: manifest.SizeBytes}}
	got, err := NewPreparedAssetAdmissionResolver(reader).RegisterPreparedArtifact(context.Background(), manifest)
	if err != nil || got.State != StateReady {
		t.Fatalf("RegisterPreparedArtifact = %+v, %v; want READY", got, err)
	}
}

func TestResolverBlocksUnreadyArtifact(t *testing.T) {
	manifest := validManifest()
	reader := artifactReaderStub{artifact: &repository.Artifact{ID: manifest.ArtifactID, Type: "video", Status: "VERIFYING", SHA256: manifest.SHA256, SizeBytes: manifest.SizeBytes}}
	got, err := NewResolver(reader).Admit(context.Background(), manifest)
	if got.State != StateWaiting || err != ErrArtifactNotReady {
		t.Fatalf("Admit = %+v, %v; want WAITING/ErrArtifactNotReady", got, err)
	}
}

func TestResolverRejectsIntegrityMismatch(t *testing.T) {
	manifest := validManifest()
	reader := artifactReaderStub{artifact: &repository.Artifact{ID: manifest.ArtifactID, Type: "video", Status: "READY", SHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", SizeBytes: manifest.SizeBytes}}
	if _, err := NewResolver(reader).Admit(context.Background(), manifest); !errors.Is(err, ErrArtifactIntegrity) {
		t.Fatalf("Admit error = %v; want ErrArtifactIntegrity", err)
	}
}
