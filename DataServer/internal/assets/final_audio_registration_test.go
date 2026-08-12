package assets

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/platform/clock"
	"velox-shared/contract"
)

func TestFinalAudioRegistration_MetadataBindsCanonicalTimeline(t *testing.T) {
	timeline := &contract.CanonicalTimeline{
		Revision:   9,
		DurationUS: 10_000_000,
		Segments: []contract.TimelineSegment{{
			SegmentID:          "segment-1",
			AssetID:            "video-1",
			TimelineStartUS:    0,
			TimelineDurationUS: 10_000_000,
			SourceInUS:         0,
			SourceDurationUS:   10_000_000,
		}},
	}
	registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 10_000_000}
	metadataJSON, err := registration.MetadataJSON(timeline)
	if err != nil {
		t.Fatalf("MetadataJSON: %v", err)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	wantSHA, err := timeline.TimelineSHA256()
	if err != nil {
		t.Fatalf("TimelineSHA256: %v", err)
	}
	if metadata["producer"] != "audio_compiler" || metadata["mode"] != contract.AudioModeFinalAudioCopy {
		t.Fatalf("producer/mode = %#v/%#v", metadata["producer"], metadata["mode"])
	}
	if metadata["timeline_revision"] != float64(9) || metadata["timeline_sha256"] != wantSHA {
		t.Fatalf("timeline binding = %#v, want revision=9 sha=%s", metadata, wantSHA)
	}
	if metadata["codec"] != "aac" || metadata["sample_rate_hz"] != float64(48_000) || metadata["channels"] != float64(2) {
		t.Fatalf("audio contract metadata = %#v", metadata)
	}
	if strings.Contains(metadataJSON, "local_path") || strings.Contains(metadataJSON, "/tmp/") {
		t.Fatalf("final audio metadata must not contain local paths: %s", metadataJSON)
	}
}

func TestFinalAudioRegistration_RejectsWrongMediaContract(t *testing.T) {
	timeline := &contract.CanonicalTimeline{
		Revision:   1,
		DurationUS: 5_000_000,
		Segments: []contract.TimelineSegment{{
			SegmentID: "segment-1", AssetID: "video-1", TimelineDurationUS: 5_000_000, SourceDurationUS: 5_000_000,
		}},
	}
	cases := []struct {
		name string
		edit func(*FinalAudioRegistration)
		want string
	}{
		{"codec", func(r *FinalAudioRegistration) { r.Codec = "opus" }, "codec must be aac"},
		{"sample rate", func(r *FinalAudioRegistration) { r.SampleRateHz = 44_100 }, "sample rate must be 48000"},
		{"channels", func(r *FinalAudioRegistration) { r.Channels = 1 }, "channels must be 2"},
		{"duration", func(r *FinalAudioRegistration) { r.DurationUS = 1_000_000 }, "differs from timeline"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: timeline.DurationUS}
			test.edit(&registration)
			if _, err := registration.MetadataJSON(timeline); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("MetadataJSON() = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestRegisterFinalAudio_RejectsInvalidContractBeforeServiceAccess(t *testing.T) {
	registration := FinalAudioRegistration{Codec: "opus", SampleRateHz: 44_100, Channels: 1, DurationUS: 1}
	if _, err := (&AssetService{}).RegisterFinalAudio(nil, "/tmp/audio.m4a", nil, registration); err == nil {
		t.Fatal("RegisterFinalAudio() = nil error, want fail-closed validation")
	}
}

func TestFinalAudioRegistration_RejectsWrongVerifiedMediaMetadata(t *testing.T) {
	registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 10_000_000}
	cases := []struct {
		name string
		meta *MediaMetadata
		want string
	}{
		{"codec", &MediaMetadata{AudioCodec: "opus", AudioSampleRate: 48_000, AudioChannels: 2, DurationMs: 10_000}, "verified codec"},
		{"sample rate", &MediaMetadata{AudioCodec: "aac", AudioSampleRate: 44_100, AudioChannels: 2, DurationMs: 10_000}, "verified sample rate"},
		{"channels", &MediaMetadata{AudioCodec: "aac", AudioSampleRate: 48_000, AudioChannels: 1, DurationMs: 10_000}, "verified channels"},
		{"duration", &MediaMetadata{AudioCodec: "aac", AudioSampleRate: 48_000, AudioChannels: 2, DurationMs: 8_000}, "verified duration"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := validateFinalAudioMediaMetadata(test.meta, registration); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateFinalAudioMediaMetadata() = %v, want error containing %q", err, test.want)
			}
		})
	}
}

type failingFinalAudioMetadataRepo struct {
	*rewriteAssetRepository
}

func (r *failingFinalAudioMetadataRepo) UpsertMediaMetadata(context.Context, string, MediaMetadataRecord) error {
	return errors.New("metadata persistence failed")
}

func TestFinalAudioRegistration_RejectsMetadataPersistenceFailure(t *testing.T) {
	root := t.TempDir()
	repo := &failingFinalAudioMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{
		"audio": {AssetID: "audio", Status: AssetStatusReady, MimeType: "audio/mp4", StorageKey: "audio.m4a"},
	}}}
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "audio", CodecName: "aac", SampleRate: 48_000, Channels: 2}},
		Format:  mediaProbeFormat{FormatName: "m4a", Duration: 10},
	})}
	service := &AssetService{
		repo:          repo,
		blobStore:     preflightBlobStore{root: root},
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(runner),
	}
	_, err := service.EnsureMediaMetadata(context.Background(), "audio")
	if err == nil || !strings.Contains(err.Error(), "could not be persisted") {
		t.Fatalf("EnsureMediaMetadata() = %v, want persistence failure", err)
	}
}

func TestFinalAudioRegistration_RejectsMismatchedDedupProvenance(t *testing.T) {
	timeline := &contract.CanonicalTimeline{
		Revision:   1,
		DurationUS: 1_000_000,
		Segments:   []contract.TimelineSegment{{SegmentID: "s", AssetID: "v", TimelineDurationUS: 1_000_000, SourceDurationUS: 1_000_000}},
	}
	registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 1_000_000}
	metadataJSON, err := registration.MetadataJSON(timeline)
	if err != nil {
		t.Fatalf("MetadataJSON: %v", err)
	}
	asset := &Asset{AssetID: "audio", Kind: KindFinalAudio, Status: AssetStatusReady, SHA256: strings.Repeat("a", 64), SizeBytes: 1, VerifiedAt: "now", MetadataJSON: metadataJSON}
	otherTimeline := *timeline
	otherTimeline.Revision = 2
	if err := validateFinalAudioProvenance(asset, &otherTimeline, registration); err == nil || !strings.Contains(err.Error(), "provenance mismatch") {
		t.Fatalf("validateFinalAudioProvenance() = %v, want mismatch for dedup asset", err)
	}
}

type finalAudioTestRepository struct {
	assets          map[string]*AssetRecord
	metadata        map[string]MediaMetadataRecord
	sources         []AssetSourceRecord
	atomicInsertErr error
}

func (r *finalAudioTestRepository) Insert(_ context.Context, asset AssetRecord) error {
	if r.assets == nil {
		r.assets = map[string]*AssetRecord{}
	}
	copy := asset
	r.assets[asset.AssetID] = &copy
	return nil
}
func (r *finalAudioTestRepository) InsertWithMediaMetadataAndSource(_ context.Context, asset AssetRecord, metadata MediaMetadataRecord, source AssetSourceRecord) error {
	if r.atomicInsertErr != nil {
		return r.atomicInsertErr
	}
	if err := r.Insert(context.Background(), asset); err != nil {
		return err
	}
	if r.metadata == nil {
		r.metadata = map[string]MediaMetadataRecord{}
	}
	r.metadata[metadata.AssetID] = metadata
	r.sources = append(r.sources, source)
	return nil
}

func (r *finalAudioTestRepository) GetByID(_ context.Context, assetID string) (*AssetRecord, error) {
	asset := r.assets[assetID]
	if asset == nil {
		return nil, nil
	}
	copy := *asset
	return &copy, nil
}
func (r *finalAudioTestRepository) GetBySHA256(_ context.Context, hash string) (*AssetRecord, error) {
	for _, asset := range r.assets {
		if asset.SHA256 == hash {
			copy := *asset
			return &copy, nil
		}
	}
	return nil, nil
}
func (r *finalAudioTestRepository) UpdateStatus(context.Context, string, string, string) error {
	return nil
}
func (r *finalAudioTestRepository) InsertSource(_ context.Context, source AssetSourceRecord) error {
	r.sources = append(r.sources, source)
	return nil
}
func (r *finalAudioTestRepository) LinkToJob(context.Context, string, string, string, int, bool) error {
	return nil
}
func (r *finalAudioTestRepository) UpsertMediaMetadata(_ context.Context, assetID string, metadata MediaMetadataRecord) error {
	if r.metadata == nil {
		r.metadata = map[string]MediaMetadataRecord{}
	}
	metadata.AssetID = assetID
	r.metadata[assetID] = metadata
	return nil
}
func (r *finalAudioTestRepository) GetMediaMetadata(_ context.Context, assetID string) (*MediaMetadataRecord, error) {
	metadata, ok := r.metadata[assetID]
	if !ok {
		return nil, nil
	}
	copy := metadata
	return &copy, nil
}

func TestRegisterFinalAudio_AtomicMetadataFailureDoesNotCreateAsset(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is required for the atomic registration certification")
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "compiled-final-audio.m4a")
	command := exec.Command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1", "-c:a", "aac", "-ar", "48000", "-ac", "2", "-vn", sourcePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create AAC fixture: %v: %s", err, output)
	}
	timeline := &contract.CanonicalTimeline{
		Revision: 1, DurationUS: 1_000_000,
		Segments: []contract.TimelineSegment{{SegmentID: "s", AssetID: "v", TimelineDurationUS: 1_000_000, SourceDurationUS: 1_000_000}},
	}
	registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 1_000_000}
	repo := &finalAudioTestRepository{atomicInsertErr: errors.New("metadata transaction failed")}
	assetStore := NewStore(root, 16*1024*1024, []string{root})
	registry := NewResolverRegistry(NewTypedResolversFromStore(assetStore, nil, nil)...)
	service := NewAssetService(repo, &segmentTestBlobStore{root: filepath.Join(root, "registered")}, registry, clock.System{})
	if _, err := service.RegisterFinalAudio(context.Background(), "file://"+sourcePath, timeline, registration); err == nil || !strings.Contains(err.Error(), "metadata transaction failed") {
		t.Fatalf("RegisterFinalAudio() = %v, want atomic metadata failure", err)
	}
	if len(repo.assets) != 0 {
		t.Fatalf("asset registry after atomic failure = %d rows, want 0", len(repo.assets))
	}
	if len(repo.metadata) != 0 {
		t.Fatalf("metadata registry after atomic failure = %d rows, want 0", len(repo.metadata))
	}
	finalBlobs, globErr := filepath.Glob(filepath.Join(root, "registered", "final", "*"))
	if globErr != nil {
		t.Fatalf("glob promoted blobs: %v", globErr)
	}
	if len(finalBlobs) != 0 {
		t.Fatalf("promoted blobs after atomic failure = %v, want cleanup", finalBlobs)
	}
}

func TestRegisterFinalAudio_RealVerifiedAssetPipeline(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is required for the real final-audio registration certification")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is required for the real final-audio registration certification")
	}

	root := t.TempDir()
	sourcePath := filepath.Join(root, "compiled-final-audio.m4a")
	command := exec.Command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1", "-c:a", "aac", "-ar", "48000", "-ac", "2", "-vn", sourcePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create AAC fixture: %v: %s", err, output)
	}

	timeline := &contract.CanonicalTimeline{
		Revision:   11,
		DurationUS: 1_000_000,
		Segments: []contract.TimelineSegment{{
			SegmentID:          "segment-1",
			AssetID:            "video-1",
			TimelineStartUS:    0,
			TimelineDurationUS: 1_000_000,
			SourceInUS:         0,
			SourceDurationUS:   1_000_000,
		}},
	}
	registration := FinalAudioRegistration{Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 1_000_000}

	assetStore := NewStore(root, 16*1024*1024, []string{root})
	registry := NewResolverRegistry(NewTypedResolversFromStore(assetStore, nil, nil)...)
	repo := &finalAudioTestRepository{}
	blobStore := &segmentTestBlobStore{root: filepath.Join(root, "registered")}
	service := NewAssetService(repo, blobStore, registry, clock.System{})

	compiled, err := contract.CompileAudioTimeline(timeline)
	if err != nil {
		t.Fatalf("CompileAudioTimeline: %v", err)
	}
	asset, err := service.RegisterCompiledFinalAudio(context.Background(), "file://"+sourcePath, timeline, compiled, registration)
	if err != nil {
		t.Fatalf("RegisterCompiledFinalAudio: %v", err)
	}
	if asset.Status != AssetStatusReady || asset.Kind != KindFinalAudio || asset.SizeBytes <= 0 || asset.SHA256 == "" || asset.VerifiedAt == "" {
		t.Fatalf("registered asset = %+v, want READY verified final_audio", asset)
	}
	content, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(content))
	if asset.SHA256 != wantSHA || asset.SizeBytes != int64(len(content)) {
		t.Fatalf("asset identity = %s/%d, want %s/%d", asset.SHA256, asset.SizeBytes, wantSHA, len(content))
	}
	if len(repo.sources) != 1 || repo.sources[0].SourceType != "audio_compiler" {
		t.Fatalf("asset sources = %+v, want audio_compiler provenance", repo.sources)
	}
	metadata := repo.metadata[asset.AssetID]
	if metadata.AudioCodec != "aac" || metadata.AudioSampleRate != 48_000 || metadata.AudioChannels != 2 || !metadata.Verified() {
		t.Fatalf("media metadata = %+v, want verified AAC/48k/stereo", metadata)
	}

	otherTimeline := *timeline
	otherTimeline.Revision++
	if _, err := service.RegisterFinalAudio(context.Background(), "file://"+sourcePath, &otherTimeline, registration); err == nil || !strings.Contains(err.Error(), "provenance mismatch") {
		t.Fatalf("dedup with changed timeline = %v, want provenance mismatch", err)
	}

	wrongFormatPath := filepath.Join(root, "wrong-format.m4a")
	wrongCommand := exec.Command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=1000:duration=1", "-c:a", "aac", "-ar", "44100", "-ac", "2", "-vn", wrongFormatPath)
	if output, err := wrongCommand.CombinedOutput(); err != nil {
		t.Fatalf("create wrong-format AAC fixture: %v: %s", err, output)
	}
	if _, err := service.RegisterFinalAudio(context.Background(), "file://"+wrongFormatPath, timeline, registration); err == nil || !strings.Contains(err.Error(), "verified sample rate") {
		t.Fatalf("wrong-format final audio = %v, want sample-rate rejection before insert", err)
	}
	if len(repo.assets) != 1 {
		t.Fatalf("wrong-format final audio inserted an asset: registry size = %d, want 1", len(repo.assets))
	}
}
