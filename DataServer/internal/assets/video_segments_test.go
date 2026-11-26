package assets

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/inputsecurity"
	"velox-server/internal/platform/clock"

	videoContract "velox-shared/contract"
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
func (r *segmentTestRepository) GetBySourceReference(_ context.Context, reference string) (*AssetRecord, error) {
	for i := len(r.sources) - 1; i >= 0; i-- {
		source := r.sources[i]
		if source.SourceReference == reference {
			record := r.bySHA[source.AssetID]
			if record == nil {
				return nil, nil
			}
			copy := *record
			copy.MetadataJSON = source.MetadataJSON
			return &copy, nil
		}
	}
	return nil, nil
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

func (b *segmentTestBlobStore) OpenStagedWrite(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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
func (b *segmentTestBlobStore) RemoveFinal(path string) error   { return os.Remove(path) }
func (b *segmentTestBlobStore) FinalPath(_, artifactID, extension string) string {
	return filepath.Join(b.root, "final", artifactID+extension)
}
func (b *segmentTestBlobStore) FinalDir() string { return filepath.Join(b.root, "final") }

func TestRegisterPreparedWindowRejectsNonCanonicalAVCCAndDeduplicatesCanonical(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is required for the prepared-window registry test")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is required for the prepared-window registry test")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.mp4")
	generate := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc=size=1920x1080:rate=24", "-frames:v", "48", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-profile:v", "high", "-level:v", "4.0", "-g", "48", "-bf", "0", "-sc_threshold", "0", "-x264-params", "scenecut=0:open-gop=0:keyint=48:min-keyint=48", "-video_track_timescale", "90000", sourcePath)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Fatalf("generate source fixture: %v: %s", err, output)
	}
	profile := videoContract.CanonicalVideoProfileV1Default
	quality := videoContract.CanonicalPreparationQualityPolicyDefault
	revision, err := videoContract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	preparedPath := filepath.Join(root, "prepared.mp4")
	args := videoContract.CanonicalVideoWindowArgs(profile, quality, sourcePath, preparedPath, 0, 48, 24, 1, 48)
	encode := exec.Command("ffmpeg", args...)
	if output, err := encode.CombinedOutput(); err != nil {
		t.Fatalf("generate canonical window: %v: %s", err, output)
	}
	preparedProbe, err := newVideoWindowVerifier().probe(context.Background(), preparedPath)
	if err != nil {
		t.Fatalf("probe generated window fixture: %v", err)
	}
	outputFile, err := os.Open(preparedPath)
	if err != nil {
		t.Fatal(err)
	}
	outputSHA, err := ComputeSHA256(outputFile)
	_ = outputFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	preparedInfo, err := os.Stat(preparedPath)
	if err != nil {
		t.Fatal(err)
	}
	request := videoContract.VideoPrepareWindowV1{
		Version:          videoContract.VideoPrepareWindowVersion,
		Source:           videoContract.AssetRefV2{AssetID: "source-asset", SHA256: strings.Repeat("a", 64), SizeBytes: 12_000, Kind: "video"},
		SourceStartFrame: 0, SourceFrameCount: 48, SourceFPSNum: 24, SourceFPSDen: 1,
		OutputFrameCount: 48, TimelineRevision: 1, TimelineSHA256: strings.Repeat("b", 64),
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	certificate := videoContract.VideoPrepareWindowCertificateV1{
		Version: videoContract.VideoPrepareWindowVersion, SourceSHA256: request.Source.SHA256,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		OutputFrameCount: request.OutputFrameCount, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision, EncoderVersion: "ffmpeg test",
		EncoderBuildSHA256: strings.Repeat("c", 64), OutputSHA256: outputSHA, OutputSizeBytes: preparedInfo.Size(),
		OutputEvidence: videoContract.VideoWindowOutputEvidenceV1{
			Codec: preparedProbe.VideoCodec, Profile: preparedProbe.VideoProfile, Level: preparedProbe.VideoLevel,
			Width: preparedProbe.Width, Height: preparedProbe.Height, PixelFormat: preparedProbe.PixelFormat,
			FPSNum: preparedProbe.FPSNum, FPSDen: preparedProbe.FPSDen,
			TimeBaseNum: preparedProbe.TimebaseNum, TimeBaseDen: preparedProbe.TimebaseDen,
			BFrames: preparedProbe.BFrames, FrameCount: preparedProbe.FrameCount,
			FirstFrameKeyframe: true, ExtradataSHA256: preparedProbe.ExtradataSHA256,
		},
		ClosedGOPAttested: true, PacketCopyUsed: true, SourceAssetID: request.Source.AssetID, SourceSizeBytes: request.Source.SizeBytes,
		TimelineRevision: request.TimelineRevision, TimelineSHA256: request.TimelineSHA256,
	}
	repo := &segmentTestRepository{}
	service := &AssetService{
		repo: repo, blobStore: &segmentTestBlobStore{root: filepath.Join(root, "assets")},
		clock: clock.System{}, videoWindowVerifier: newVideoWindowVerifier(),
		security: inputsecurity.NewFetcher(inputsecurity.DefaultPolicy()), mediaMetadata: NewMediaMetadataResolver(),
	}
	if preparedProbe.ExtradataSHA256 != videoContract.CanonicalVideoExtradataSHA256 {
		_, err := service.RegisterPreparedWindow(context.Background(), preparedPath, request, certificate)
		if err == nil || !strings.Contains(err.Error(), "does not match canonical identity") {
			t.Fatalf("non-canonical avcC was not rejected at the Master registration boundary: %v", err)
		}
		return
	}
	sourceEvidence := certificate.OutputEvidence
	sourceEvidence.FrameCount = 96
	certificate.SourceCertificate = &videoContract.VideoSourceCertificateV1{Version: 1, SHA256: request.Source.SHA256, SizeBytes: request.Source.SizeBytes, ProfileID: profile.ProfileID, PreparationRevision: revision, Evidence: sourceEvidence, DurationUS: 4000000, KeyframesUS: []int64{0, 1000000, 2000000, 3000000}}
	asset, err := service.RegisterPreparedWindow(context.Background(), preparedPath, request, certificate)
	if err != nil {
		t.Fatalf("register prepared window: %v", err)
	}
	proof, err := repo.GetBySourceReference(context.Background(), sourceCertificateReference(request.Source.SHA256, revision))
	if err != nil || proof != nil {
		t.Fatalf("source certificate must wait for the fenced task completion: %+v %v", proof, err)
	}
	if asset.AssetID != outputSHA || asset.SHA256 != outputSHA || len(repo.inserted) != 1 {
		t.Fatalf("registered asset=%+v inserts=%d", asset, len(repo.inserted))
	}
	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(repo.inserted[0].MetadataJSON), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["source_start_frame"] != float64(0) || metadata["window_certificate"] == nil {
		t.Fatalf("prepared window certification missing from registry metadata: %#v", metadata)
	}
	cacheProbeRunner := &recordingVideoRunner{probeOutput: []byte("cache hit must not probe")}
	service.videoWindowVerifier = newVideoWindowVerifierForTest(cacheProbeRunner)
	second, err := service.RegisterPreparedWindow(context.Background(), preparedPath, request, certificate)
	if err != nil {
		t.Fatalf("reuse prepared window: %v", err)
	}
	if second.AssetID != asset.AssetID || len(repo.inserted) != 1 || countFFProbeCommands(cacheProbeRunner) != 0 {
		t.Fatalf("second registration=%+v inserts=%d probes=%d, want certified reuse without another probe", second, len(repo.inserted), countFFProbeCommands(cacheProbeRunner))
	}

	// Another window can map to the same bytes. Its source record stores the
	// certificate directly, unlike the envelope used by the first registration.
	service.videoWindowVerifier = newVideoWindowVerifier()
	request.SourceStartFrame = 24
	certificate.SourceStartFrame = 24
	if _, err := service.RegisterPreparedWindow(context.Background(), preparedPath, request, certificate); err != nil {
		t.Fatal(err)
	}
	identity, err := videoContract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := service.LookupPreparedWindowAsset(context.Background(), identity)
	if err != nil || binding == nil || binding.AssetID != asset.AssetID {
		t.Fatalf("deduplicated source evidence was not reusable: binding=%+v err=%v", binding, err)
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

	asset, err := service.registerPreparedVideoFile(context.Background(), preparedPath, "source.mp4", preparedVideoFilePlan{StartSeconds: 2, EndSeconds: 6, DurationSeconds: 4})
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
