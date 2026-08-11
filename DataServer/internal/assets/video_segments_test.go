package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/platform/clock"
)

type segmentTestRepository struct {
	inserted []AssetRecord
	sources  []AssetSourceRecord
	bySHA    map[string]*AssetRecord
}

func (r *segmentTestRepository) Insert(_ context.Context, asset AssetRecord) error {
	if r.bySHA == nil {
		r.bySHA = make(map[string]*AssetRecord)
	}
	copy := asset
	r.inserted = append(r.inserted, asset)
	r.bySHA[asset.SHA256] = &copy
	return nil
}
func (r *segmentTestRepository) GetByID(context.Context, string) (*AssetRecord, error) {
	return nil, nil
}
func (r *segmentTestRepository) GetBySHA256(_ context.Context, hash string) (*AssetRecord, error) {
	return r.bySHA[hash], nil
}
func (r *segmentTestRepository) UpdateStatus(context.Context, string, string, string) error {
	return nil
}
func (r *segmentTestRepository) InsertSource(_ context.Context, source AssetSourceRecord) error {
	r.sources = append(r.sources, source)
	return nil
}
func (r *segmentTestRepository) LinkToJob(context.Context, string, string, string, int, bool) error {
	return nil
}
func (r *segmentTestRepository) UpsertMediaMetadata(context.Context, string, MediaMetadataRecord) error {
	return nil
}
func (r *segmentTestRepository) GetMediaMetadata(context.Context, string) (*MediaMetadataRecord, error) {
	return nil, nil
}

type segmentTestBlobStore struct {
	root string
}

func (b *segmentTestBlobStore) StagingPath(_, artifactID, extension string) (string, error) {
	path := filepath.Join(b.root, "staging", artifactID+extension)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	return path, nil
}
func (b *segmentTestBlobStore) PromoteToFinal(stagingPath, finalPath string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return "", err
	}
	return finalPath, nil
}
func (b *segmentTestBlobStore) RemoveStaging(path string) error { return os.Remove(path) }
func (b *segmentTestBlobStore) FinalPath(_, artifactID, extension string) string {
	return filepath.Join(b.root, "final", artifactID+extension)
}
func (b *segmentTestBlobStore) FinalDir() string { return filepath.Join(b.root, "final") }

func TestRewriteVideoClipSegmentsRegistersCanonicalSegment(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	if err := os.WriteFile(sourcePath, []byte("source video"), 0o644); err != nil {
		t.Fatal(err)
	}
	body := []byte("trimmed segment")
	runner := &recordingVideoRunner{probeOutput: probeJSON(t, normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10}))}
	trimmer := newVideoTrimmerForTest(runner, defaultVideoNormalization)
	repo := &segmentTestRepository{}
	service := &AssetService{
		repo:         repo,
		blobStore:    &segmentTestBlobStore{root: filepath.Join(root, "assets")},
		clock:        clock.System{},
		videoTrimmer: trimmer,
	}
	// The command runner writes a deterministic payload; use it to assert the
	// registered hash and size are derived from the produced segment bytes.
	_ = body
	payload := map[string]interface{}{
		"clip_segments": []interface{}{
			map[string]interface{}{
				"source_path":   sourcePath,
				"start_seconds": 2.0,
				"end_seconds":   6.0,
			},
		},
	}
	if err := service.RewriteVideoClipSegments(context.Background(), payload); err != nil {
		t.Fatalf("RewriteVideoClipSegments: %v", err)
	}
	segment := payload["clip_segments"].([]interface{})[0].(map[string]interface{})
	uri, _ := segment["uri"].(string)
	if !strings.HasPrefix(uri, "velox-asset://") {
		t.Fatalf("uri = %q, want canonical velox-asset://", uri)
	}
	sha, _ := segment["sha256"].(string)
	if len(sha) != sha256.Size*2 {
		t.Fatalf("sha256 = %q, want complete SHA-256", sha)
	}
	if got, ok := segment["size_bytes"].(int64); !ok || got <= 0 {
		t.Fatalf("size_bytes = %#v, want positive int64", segment["size_bytes"])
	}
	if len(repo.inserted) != 1 || repo.inserted[0].Kind != "video_segment" {
		t.Fatalf("inserted assets = %+v, want one video_segment", repo.inserted)
	}
	if len(repo.sources) != 1 || repo.sources[0].SourceType != "video_segment" {
		t.Fatalf("sources = %+v, want one video_segment source", repo.sources)
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(repo.inserted[0].MetadataJSON), &metadata); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	if metadata["trim_mode"] != string(TrimModeStreamCopy) || metadata["start_seconds"] != float64(2) {
		t.Fatalf("metadata = %#v, want stream-copy and start 2", metadata)
	}
	wantHash := sha256.Sum256([]byte("trimmed video"))
	if sha != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("sha256 = %q, want hash of prepared segment", sha)
	}
}

func TestTrimAndRegisterVideoSegment_ProbesSourceOnceAcrossSegments(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	if err := os.WriteFile(sourcePath, []byte("source video"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &recordingVideoRunner{probeOutput: probeJSON(t, normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10}))}
	service := &AssetService{
		repo:         &segmentTestRepository{},
		blobStore:    &segmentTestBlobStore{root: filepath.Join(root, "assets")},
		clock:        clock.System{},
		videoTrimmer: newVideoTrimmerForTest(runner, defaultVideoNormalization),
	}
	memo := make(map[string]*VideoProbe)

	if _, _, err := service.TrimAndRegisterVideoSegment(context.Background(), sourcePath, VideoSegment{StartSeconds: 2, EndSeconds: 6}, memo); err != nil {
		t.Fatalf("first segment: %v", err)
	}
	if _, _, err := service.TrimAndRegisterVideoSegment(context.Background(), sourcePath, VideoSegment{StartSeconds: 4, EndSeconds: 8}, memo); err != nil {
		t.Fatalf("second segment: %v", err)
	}

	if probes := countFFProbeCommands(runner); probes != 1 {
		t.Fatalf("ffprobe invocations = %d, want exactly 1 for two segments of the same source", probes)
	}
}

func TestRewriteVideoClipSegments_RegisteredSourceWithVerifiedMetadataPassesGate(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"clip-reg": {AssetID: "clip-reg", Status: AssetStatusReady, MimeType: "video/mp4", StorageKey: "assets/clip-reg.mp4"},
	})
	repo.metadata["clip-reg"] = MediaMetadataRecord{
		AssetID: "clip-reg", Container: "mp4", DurationMs: 5000, VideoCodec: "h264",
		MetadataVerifiedAt: "2026-08-11T00:00:00Z", MetadataSchemaVersion: MediaMetadataSchemaVersion,
	}
	service := &AssetService{
		repo:          repo,
		blobStore:     &segmentTestBlobStore{root: t.TempDir()},
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(&recordingVideoRunner{}),
		videoTrimmer:  newVideoTrimmerForTest(&recordingVideoRunner{}, defaultVideoNormalization),
		// registry intentionally nil: after the C2 metadata gate PASSES, the
		// rewrite proceeds to materialization which fails on the missing
		// resolver — proving the verified registered asset was NOT rejected
		// by the metadata gate.
	}
	payload := map[string]interface{}{
		"clip_segments": []interface{}{
			map[string]interface{}{
				"source_path":   "velox-asset://clip-reg",
				"start_seconds": 2.0,
				"end_seconds":   6.0,
			},
		},
	}
	err := service.RewriteVideoClipSegments(context.Background(), payload)
	if err == nil {
		t.Fatal("expected materialization failure after the metadata gate passed")
	}
	if strings.Contains(err.Error(), "no verified media metadata") {
		t.Fatalf("metadata gate rejected a registered asset WITH verified metadata: %v", err)
	}
	if !strings.Contains(err.Error(), "no resolver is configured") {
		t.Fatalf("expected the no-resolver materialization error, got %v", err)
	}
}

func TestRewriteVideoClipSegments_FailsClosedOnUnverifiedRegisteredSource(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"clip-reg": {AssetID: "clip-reg", Status: AssetStatusReady, MimeType: "video/mp4", StorageKey: "assets/clip-reg.mp4"},
	})
	service := &AssetService{
		repo:          repo,
		blobStore:     &segmentTestBlobStore{root: t.TempDir()},
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(&failingMediaRunner{err: errors.New("ffprobe boom")}),
		videoTrimmer:  newVideoTrimmerForTest(&recordingVideoRunner{}, defaultVideoNormalization),
	}
	payload := map[string]interface{}{
		"clip_segments": []interface{}{
			map[string]interface{}{
				"source_path":   "velox-asset://clip-reg",
				"start_seconds": 2.0,
				"end_seconds":   6.0,
			},
		},
	}
	err := service.RewriteVideoClipSegments(context.Background(), payload)
	if err == nil || !strings.Contains(err.Error(), "no verified media metadata") {
		t.Fatalf("want fail-closed registered-asset rejection, got %v", err)
	}
}

func countFFProbeCommands(runner *recordingVideoRunner) int {
	count := 0
	for _, cmd := range runner.commands {
		if cmd.name == "ffprobe" {
			count++
		}
	}
	return count
}

// TestRegisterPreparedVideoFile_PersistsMediaMetadata pins the Fase C2
// closure for derived assets: a registered video segment gets its canonical
// one-time media metadata persisted (best-effort), so later consumers read
// the registry instead of re-probing.
func TestRegisterPreparedVideoFile_PersistsMediaMetadata(t *testing.T) {
	root := t.TempDir()
	preparedPath := filepath.Join(root, "prepared.mp4")
	if err := os.WriteFile(preparedPath, []byte("trimmed segment"), 0o644); err != nil {
		t.Fatal(err)
	}
	repo := &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{
			{CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080, FrameRate: "30/1", TimeBase: "1/90000", PixelFormat: "yuv420p", Duration: 4},
		},
		Format: mediaProbeFormat{FormatName: "mp4", Duration: 4},
	})}
	service := &AssetService{
		repo:          repo,
		blobStore:     &segmentTestBlobStore{root: filepath.Join(root, "assets")},
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	asset, err := service.registerPreparedVideoFile(context.Background(), preparedPath, "source.mp4", TrimPlan{
		Segment:         VideoSegment{StartSeconds: 2, EndSeconds: 6},
		DurationSeconds: 4,
		Mode:            TrimModeStreamCopy,
	})
	if err != nil {
		t.Fatalf("registerPreparedVideoFile: %v", err)
	}
	if asset == nil {
		t.Fatal("registerPreparedVideoFile returned nil asset")
	}
	if len(repo.upserts) != 1 {
		t.Fatalf("metadata upserts = %d, want 1 for the registered segment", len(repo.upserts))
	}
	record := repo.upserts[0]
	if record.AssetID != asset.AssetID {
		t.Errorf("metadata asset_id = %q, want %q", record.AssetID, asset.AssetID)
	}
	if !record.Verified() {
		t.Errorf("segment metadata not verified: %+v", record)
	}
	if record.Container != "mp4" || record.DurationMs != 4000 || record.VideoCodec != "h264" || record.Width != 1920 {
		t.Errorf("segment metadata = %+v, want mp4/4000ms/h264/1920", record)
	}
	if ffprobes := countFFProbeCommands(runner); ffprobes != 1 {
		t.Errorf("ffprobe invocations = %d, want exactly 1 for the segment", ffprobes)
	}
}

func TestRewriteVideoClipSegmentsSupportsJSONAndFailsClosedWithoutLocalSource(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	if err := os.WriteFile(sourcePath, []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &recordingVideoRunner{probeOutput: probeJSON(t, normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10}))}
	service := &AssetService{
		repo:         &segmentTestRepository{},
		blobStore:    &segmentTestBlobStore{root: filepath.Join(root, "assets")},
		clock:        clock.System{},
		videoTrimmer: newVideoTrimmerForTest(runner, defaultVideoNormalization),
	}
	encoded, err := json.Marshal([]map[string]interface{}{{
		"source_path": sourcePath, "start_ms": float64(2000), "end_ms": float64(4000),
	}})
	if err != nil {
		t.Fatal(err)
	}
	payload := map[string]interface{}{"clip_segments_json": string(encoded)}
	if err := service.RewriteVideoClipSegments(context.Background(), payload); err != nil {
		t.Fatalf("JSON rewrite: %v", err)
	}
	if !strings.Contains(payload["clip_segments_json"].(string), "velox-asset://") {
		t.Fatalf("rewritten JSON = %s, want canonical URI", payload["clip_segments_json"])
	}

	missingSource := &AssetService{}
	err = missingSource.RewriteVideoClipSegments(context.Background(), map[string]interface{}{
		"clip_segments": []interface{}{map[string]interface{}{"start_ms": float64(1000), "end_ms": float64(2000), "clip_link": "https://example.test/full.mp4"}},
	})
	if err == nil || !strings.Contains(err.Error(), "master-local source_path") {
		t.Fatalf("missing source error = %v, want fail-closed master-local source error", err)
	}
}
