package assets

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/platform/clock"
)

type preflightBlobStore struct {
	root string
}

func (b preflightBlobStore) StagingPath(string, string, string) (string, error) { return "", nil }
func (b preflightBlobStore) PromoteToFinal(string, string) (string, error)      { return "", nil }
func (b preflightBlobStore) RemoveStaging(string) error                         { return nil }
func (b preflightBlobStore) FinalPath(_, _, _ string) string                    { return "" }
func (b preflightBlobStore) FinalDir() string                                   { return b.root }
func (b preflightBlobStore) ReadFinal(storageKey string) (*os.File, error) {
	return os.Open(filepath.Join(b.root, storageKey))
}

func TestAssetServicePreflightVerifiesMetadataAndFinalBlob(t *testing.T) {
	content := []byte("verified asset bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "asset.bin"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	service := NewAssetService(&rewriteAssetRepository{assets: map[string]*AssetRecord{
		"asset-1": {
			AssetID: "asset-1", Status: AssetStatusReady, SHA256: digest,
			SizeBytes: int64(len(content)), StorageKey: "asset.bin",
		},
	}}, preflightBlobStore{root: root}, nil, nil)

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{
		AssetID: "asset-1", SHA256: digest, SizeBytes: int64(len(content)),
	}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.Requested != 1 || report.MetadataAvailable != 1 || report.BlobResolvable != 1 || report.SHA256Valid != 1 || report.SizeValid != 1 {
		t.Fatalf("report = %#v, want one fully verified asset", report)
	}
	if report.Items[0].Issue != "" {
		t.Fatalf("item issue = %q, want empty", report.Items[0].Issue)
	}
}

// writePreflightBlob writes a deterministic blob into the preflight blob
// store root and returns its content + SHA-256 digest for asset fixtures.
func writePreflightBlob(t *testing.T, root, name string, content []byte) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), content, 0o644); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(content))
}

// TestAssetServicePreflight_MediaAssetWithVerifiedMetadataPasses pins the
// Fase C2 registry-hit path: a media asset with a verified asset_media_metadata
// row passes the gate WITHOUT spawning ffprobe.
func TestAssetServicePreflight_MediaAssetWithVerifiedMetadataPasses(t *testing.T) {
	root := t.TempDir()
	content := []byte("media asset bytes")
	digest := writePreflightBlob(t, root, "asset.mp4", content)
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"media-1": {AssetID: "media-1", Status: AssetStatusReady, MimeType: "video/mp4", SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "asset.mp4"},
	})
	repo.metadata["media-1"] = MediaMetadataRecord{
		AssetID: "media-1", Container: "mp4", DurationMs: 5000, VideoCodec: "h264",
		MetadataVerifiedAt: "2026-08-11T00:00:00Z", MetadataSchemaVersion: MediaMetadataSchemaVersion,
	}
	runner := &recordingVideoRunner{}
	service := &AssetService{
		repo: repo, blobStore: preflightBlobStore{root: root},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "media-1"}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.MediaMetadataAvailable != 1 || !report.Items[0].MediaMetadata {
		t.Fatalf("report = %#v, want MediaMetadata=true for verified media asset", report)
	}
	if report.Items[0].Issue != "" {
		t.Fatalf("issue = %q, want empty", report.Items[0].Issue)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("verified registry row must NOT spawn ffprobe, got %d commands", len(runner.commands))
	}
}

// TestAssetServicePreflight_MediaAssetWithoutRowProbesOnceAndPersists pins the
// Fase C2 canonical verifier path: a media asset without a verified row is
// probed ONCE through the single MediaMetadataResolver, the result persisted,
// and the gate passes.
func TestAssetServicePreflight_MediaAssetWithoutRowProbesOnceAndPersists(t *testing.T) {
	root := t.TempDir()
	content := []byte("media asset bytes")
	digest := writePreflightBlob(t, root, "asset.mp4", content)
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"media-1": {AssetID: "media-1", Status: AssetStatusReady, MimeType: "video/mp4", SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "asset.mp4"},
	})
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080, FrameRate: "30/1", TimeBase: "1/90000", PixelFormat: "yuv420p", Duration: 10}},
		Format:  mediaProbeFormat{FormatName: "mp4", Duration: 10},
	})}
	service := &AssetService{
		repo: repo, blobStore: preflightBlobStore{root: root},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "media-1"}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.MediaMetadataAvailable != 1 || !report.Items[0].MediaMetadata {
		t.Fatalf("report = %#v, want MediaMetadata=true after one-time probe", report)
	}
	if persisted := repo.metadata["media-1"]; !persisted.Verified() {
		t.Fatalf("one-time probe must persist a verified row, got %+v", persisted)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("probe commands = %d, want exactly 1", len(runner.commands))
	}
}

// TestAssetServicePreflight_UnverifiableMediaAssetFailsClosed pins the Fase C2
// fail-closed semantics: a media asset whose canonical probe fails is flagged
// media_metadata_unavailable and NEVER invents metadata.
func TestAssetServicePreflight_UnverifiableMediaAssetFailsClosed(t *testing.T) {
	root := t.TempDir()
	content := []byte("media asset bytes")
	digest := writePreflightBlob(t, root, "asset.mp4", content)
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"media-1": {AssetID: "media-1", Status: AssetStatusReady, MimeType: "video/mp4", SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "asset.mp4"},
	})
	service := &AssetService{
		repo: repo, blobStore: preflightBlobStore{root: root},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(&failingMediaRunner{err: errors.New("ffprobe boom")}),
	}

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "media-1"}})
	if err != nil {
		t.Fatalf("Preflight must report, not error: %v", err)
	}
	if report.MediaMetadataAvailable != 0 || report.Items[0].MediaMetadata {
		t.Fatalf("report = %#v, want MediaMetadata=false for unverifiable media asset", report)
	}
	if report.Items[0].Issue != "media_metadata_unavailable" {
		t.Fatalf("issue = %q, want media_metadata_unavailable", report.Items[0].Issue)
	}
	if _, ok := repo.metadata["media-1"]; ok {
		t.Fatal("failed probe must not invent a metadata row")
	}
}

// TestAssetServicePreflight_NonMediaAssetPassesWithoutProbe pins the N/A
// semantics: non-media assets never trigger the media gate and never probe.
func TestAssetServicePreflight_NonMediaAssetPassesWithoutProbe(t *testing.T) {
	root := t.TempDir()
	content := []byte("font bytes")
	digest := writePreflightBlob(t, root, "font.ttf", content)
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"font-1": {AssetID: "font-1", Status: AssetStatusReady, MimeType: "font/ttf", SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "font.ttf"},
	})
	runner := &recordingVideoRunner{}
	service := &AssetService{
		repo: repo, blobStore: preflightBlobStore{root: root},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "font-1"}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.MediaMetadataAvailable != 1 || !report.Items[0].MediaMetadata {
		t.Fatalf("report = %#v, want MediaMetadata N/A true for non-media asset", report)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("non-media asset must not probe, got %d commands", len(runner.commands))
	}
}

func TestAssetServicePreflightRejectsMissingAndCorruptBlobs(t *testing.T) {
	content := []byte("verified asset bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(content))
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "corrupt.bin"), []byte("wrong"), 0o644); err != nil {
		t.Fatal(err)
	}
	service := NewAssetService(&rewriteAssetRepository{assets: map[string]*AssetRecord{
		"corrupt": {AssetID: "corrupt", Status: AssetStatusReady, SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "corrupt.bin"},
		"missing": {AssetID: "missing", Status: AssetStatusReady, SHA256: digest, SizeBytes: int64(len(content)), StorageKey: "missing.bin"},
	}}, preflightBlobStore{root: root}, nil, nil)

	report, err := service.Preflight(context.Background(), []AssetPreflightRequirement{{AssetID: "corrupt"}, {AssetID: "missing"}})
	if err != nil {
		t.Fatalf("Preflight: %v", err)
	}
	if report.BlobResolvable != 0 {
		t.Fatalf("BlobResolvable = %d, want 0", report.BlobResolvable)
	}
	if report.Items[0].Issue != "blob_size_mismatch" && report.Items[0].Issue != "blob_sha256_mismatch" {
		t.Fatalf("corrupt issue = %q", report.Items[0].Issue)
	}
	if report.Items[1].Issue != "blob_unresolvable" {
		t.Fatalf("missing issue = %q, want blob_unresolvable", report.Items[1].Issue)
	}
}
