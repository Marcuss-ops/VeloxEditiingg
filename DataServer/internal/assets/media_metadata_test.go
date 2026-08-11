package assets

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"velox-server/internal/platform/clock"
)

// mediaProbeDocument is the ffprobe JSON shape emitted by the fake runner.
type mediaProbeDocument struct {
	Streams []mediaProbeStream `json:"streams"`
	Format  mediaProbeFormat   `json:"format"`
}

type mediaProbeStream struct {
	CodecType   string  `json:"codec_type"`
	CodecName   string  `json:"codec_name"`
	Width       int     `json:"width"`
	Height      int     `json:"height"`
	FrameRate   string  `json:"r_frame_rate"`
	TimeBase    string  `json:"time_base"`
	PixelFormat string  `json:"pix_fmt"`
	Duration    float64 `json:"duration"`
	SampleRate  int     `json:"sample_rate"`
	Channels    int     `json:"channels"`
}

type mediaProbeFormat struct {
	FormatName string  `json:"format_name"`
	Duration   float64 `json:"duration"`
}

func mediaProbeJSON(t *testing.T, document mediaProbeDocument) []byte {
	t.Helper()
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestMediaMetadataResolver_ExtractsVideoAndAudio(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{
			{CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080, FrameRate: "30/1", TimeBase: "1/90000", PixelFormat: "yuv420p", Duration: 12.5},
			{CodecType: "audio", CodecName: "AAC", SampleRate: 48000, Channels: 2},
		},
		Format: mediaProbeFormat{FormatName: "mov,mp4,m4a,3gp,3g2,mj2", Duration: 12.5},
	})}
	resolver := newMediaMetadataResolverForTest(runner)

	meta, err := resolver.Resolve(context.Background(), "/assets/source.mp4")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if meta.Container != "mov" {
		t.Errorf("container = %q, want mov", meta.Container)
	}
	if meta.DurationMs != 12500 {
		t.Errorf("duration_ms = %d, want 12500", meta.DurationMs)
	}
	if meta.VideoCodec != "h264" || meta.PixelFormat != "yuv420p" {
		t.Errorf("video codec/pix_fmt = %q/%q, want h264/yuv420p", meta.VideoCodec, meta.PixelFormat)
	}
	if meta.Width != 1920 || meta.Height != 1080 {
		t.Errorf("resolution = %dx%d, want 1920x1080", meta.Width, meta.Height)
	}
	if meta.FPSNum != 30 || meta.FPSDen != 1 {
		t.Errorf("fps = %d/%d, want 30/1", meta.FPSNum, meta.FPSDen)
	}
	if meta.TimeBaseNum != 1 || meta.TimeBaseDen != 90000 {
		t.Errorf("time_base = %d/%d, want 1/90000", meta.TimeBaseNum, meta.TimeBaseDen)
	}
	if meta.AudioCodec != "aac" || meta.AudioSampleRate != 48000 || meta.AudioChannels != 2 {
		t.Errorf("audio = %q %dHz %dch, want aac 48000 2ch", meta.AudioCodec, meta.AudioSampleRate, meta.AudioChannels)
	}
	if len(runner.commands) != 1 || runner.commands[0].name != "ffprobe" {
		t.Fatalf("expected exactly one ffprobe invocation, got %#v", runner.commands)
	}
}

func TestMediaMetadataResolver_ExtractsAudioOnly(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{
			{CodecType: "audio", CodecName: "mp3", SampleRate: 44100, Channels: 1},
		},
		Format: mediaProbeFormat{FormatName: "mp3", Duration: 3.75},
	})}
	meta, err := newMediaMetadataResolverForTest(runner).Resolve(context.Background(), "/assets/voiceover.mp3")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if meta.Container != "mp3" {
		t.Errorf("container = %q, want mp3", meta.Container)
	}
	if meta.DurationMs != 3750 {
		t.Errorf("duration_ms = %d, want 3750", meta.DurationMs)
	}
	if meta.AudioCodec != "mp3" || meta.AudioSampleRate != 44100 || meta.AudioChannels != 1 {
		t.Errorf("audio = %q %dHz %dch", meta.AudioCodec, meta.AudioSampleRate, meta.AudioChannels)
	}
	if meta.VideoCodec != "" || meta.Width != 0 || meta.Height != 0 {
		t.Errorf("video fields must be empty for audio-only asset: %+v", meta)
	}
}

func TestMediaMetadataResolver_RejectsNoMediaStreams(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "subtitle", CodecName: "subrip"}},
		Format:  mediaProbeFormat{FormatName: "ass"},
	})}
	_, err := newMediaMetadataResolverForTest(runner).Resolve(context.Background(), "/assets/captions.srt")
	if err == nil || !strings.Contains(err.Error(), "no media streams") {
		t.Fatalf("want no-media-streams error, got %v", err)
	}
}

func TestMediaMetadataResolver_RejectsEmptyContainer(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "audio", CodecName: "aac"}},
		Format:  mediaProbeFormat{FormatName: ""},
	})}
	_, err := newMediaMetadataResolverForTest(runner).Resolve(context.Background(), "/assets/x.m4a")
	if err == nil || !strings.Contains(err.Error(), "empty container") {
		t.Fatalf("want empty-container error, got %v", err)
	}
}

func TestMediaMetadataResolver_ProbeFailure(t *testing.T) {
	failing := &failingMediaRunner{err: errors.New("ffprobe not found")}
	_, err := newMediaMetadataResolverForTest(failing).Resolve(context.Background(), "/assets/x.mp4")
	if err == nil || !strings.Contains(err.Error(), "ffprobe") {
		t.Fatalf("want ffprobe error, got %v", err)
	}
}

func TestMediaMetadataResolver_RejectsEmptyInputPath(t *testing.T) {
	_, err := NewMediaMetadataResolver().Resolve(context.Background(), "  ")
	if err == nil || !strings.Contains(err.Error(), "input path is required") {
		t.Fatalf("want input-path error, got %v", err)
	}
}

type failingMediaRunner struct {
	err error
}

func (r *failingMediaRunner) Run(_ context.Context, _ string, _ ...string) ([]byte, error) {
	return nil, r.err
}

func TestIsMediaMIME(t *testing.T) {
	cases := []struct {
		mime string
		want bool
	}{
		{"video/mp4", true},
		{"audio/mpeg", true},
		{"Video/QuickTime", true},
		{"  audio/x-wav ", true},
		{"font/ttf", false},
		{"text/vtt", false},
		{"application/octet-stream", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isMediaMIME(tc.mime); got != tc.want {
			t.Errorf("isMediaMIME(%q) = %v, want %v", tc.mime, got, tc.want)
		}
	}
}

// recordingMetadataRepo records media-metadata upserts on top of the
// rewriteAssetRepository fake.
type recordingMetadataRepo struct {
	*rewriteAssetRepository
	upserts []MediaMetadataRecord
}

// metadataProbeRepo stores assets + media-metadata rows (EnsureMediaMetadata
// tests).
type metadataProbeRepo struct {
	*rewriteAssetRepository
	metadata map[string]MediaMetadataRecord
}

func newMetadataProbeRepo(assets map[string]*AssetRecord) *metadataProbeRepo {
	return &metadataProbeRepo{
		rewriteAssetRepository: &rewriteAssetRepository{assets: assets},
		metadata:               map[string]MediaMetadataRecord{},
	}
}

func (r *metadataProbeRepo) GetMediaMetadata(_ context.Context, assetID string) (*MediaMetadataRecord, error) {
	if rec, ok := r.metadata[assetID]; ok {
		copied := rec
		return &copied, nil
	}
	return nil, nil
}

func (r *metadataProbeRepo) UpsertMediaMetadata(_ context.Context, assetID string, rec MediaMetadataRecord) error {
	if r.metadata == nil {
		r.metadata = map[string]MediaMetadataRecord{}
	}
	rec.AssetID = assetID
	r.metadata[assetID] = rec
	return nil
}

func TestEnsureMediaMetadata_RegistryHitSkipsProbe(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"clip-1": {AssetID: "clip-1", Status: AssetStatusReady, MimeType: "video/mp4", StorageKey: "assets/clip-1.mp4"},
	})
	repo.metadata["clip-1"] = MediaMetadataRecord{
		AssetID: "clip-1", Container: "mp4", DurationMs: 5000, VideoCodec: "h264",
		Width: 1920, Height: 1080, MetadataVerifiedAt: "2026-08-11T00:00:00Z",
		MetadataSchemaVersion: MediaMetadataSchemaVersion,
	}
	runner := &recordingVideoRunner{}
	service := &AssetService{
		repo: repo, blobStore: &segmentTestBlobStore{root: t.TempDir()},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}
	meta, err := service.EnsureMediaMetadata(context.Background(), "clip-1")
	if err != nil {
		t.Fatalf("EnsureMediaMetadata: %v", err)
	}
	if meta == nil || meta.Container != "mp4" || meta.DurationMs != 5000 || meta.Width != 1920 {
		t.Fatalf("meta = %+v, want registry row surfaced", meta)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("registered asset with verified row must NOT spawn ffprobe, got %d commands", len(runner.commands))
	}
}

func TestEnsureMediaMetadata_ProbesOncePersistsThenRegistryHit(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"clip-1": {AssetID: "clip-1", Status: AssetStatusReady, MimeType: "video/mp4", StorageKey: "assets/clip-1.mp4"},
	})
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080, FrameRate: "30/1", TimeBase: "1/90000", PixelFormat: "yuv420p", Duration: 10}},
		Format:  mediaProbeFormat{FormatName: "mp4", Duration: 10},
	})}
	service := &AssetService{
		repo: repo, blobStore: &segmentTestBlobStore{root: t.TempDir()},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	first, err := service.EnsureMediaMetadata(context.Background(), "clip-1")
	if err != nil {
		t.Fatalf("first EnsureMediaMetadata: %v", err)
	}
	if first.DurationMs != 10000 {
		t.Errorf("duration_ms = %d, want 10000", first.DurationMs)
	}
	persisted := repo.metadata["clip-1"]
	if !persisted.Verified() {
		t.Fatalf("one-time probe must persist a verified row: %+v", persisted)
	}
	if got := len(runner.commands); got != 1 {
		t.Fatalf("probe commands = %d, want exactly 1", got)
	}

	second, err := service.EnsureMediaMetadata(context.Background(), "clip-1")
	if err != nil {
		t.Fatalf("second EnsureMediaMetadata: %v", err)
	}
	if second.DurationMs != 10000 {
		t.Errorf("second duration_ms = %d", second.DurationMs)
	}
	if got := len(runner.commands); got != 1 {
		t.Fatalf("registry-hit must skip the probe: commands = %d, want still 1", got)
	}
}

func TestEnsureMediaMetadata_ProbeFailureFailsClosed(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"clip-1": {AssetID: "clip-1", Status: AssetStatusReady, MimeType: "video/mp4", StorageKey: "assets/clip-1.mp4"},
	})
	service := &AssetService{
		repo: repo, blobStore: &segmentTestBlobStore{root: t.TempDir()},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(&failingMediaRunner{err: errors.New("boom")}),
	}
	_, err := service.EnsureMediaMetadata(context.Background(), "clip-1")
	if err == nil || !strings.Contains(err.Error(), "no verified media metadata") {
		t.Fatalf("want fail-closed error, got %v", err)
	}
	if _, ok := repo.metadata["clip-1"]; ok {
		t.Fatal("failed probe must not invent a metadata row")
	}
}

func TestEnsureMediaMetadata_NonMediaReturnsNil(t *testing.T) {
	repo := newMetadataProbeRepo(map[string]*AssetRecord{
		"font-1": {AssetID: "font-1", Status: AssetStatusReady, MimeType: "font/ttf", StorageKey: "assets/font-1.ttf"},
	})
	runner := &recordingVideoRunner{}
	service := &AssetService{
		repo: repo, blobStore: &segmentTestBlobStore{root: t.TempDir()},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(runner),
	}
	meta, err := service.EnsureMediaMetadata(context.Background(), "font-1")
	if err != nil {
		t.Fatalf("EnsureMediaMetadata non-media: %v", err)
	}
	if meta != nil {
		t.Fatalf("non-media asset must yield (nil, nil), got %+v", meta)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("non-media must not probe, got %d commands", len(runner.commands))
	}
}

func TestEnsureMediaMetadata_MissingAssetFailsClosed(t *testing.T) {
	service := &AssetService{
		repo: newMetadataProbeRepo(map[string]*AssetRecord{}), blobStore: &segmentTestBlobStore{root: t.TempDir()},
		clock: clock.System{}, mediaMetadata: newMediaMetadataResolverForTest(&recordingVideoRunner{}),
	}
	_, err := service.EnsureMediaMetadata(context.Background(), "ghost")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want not-found error, got %v", err)
	}
}

func (r *recordingMetadataRepo) UpsertMediaMetadata(_ context.Context, assetID string, rec MediaMetadataRecord) error {
	rec.AssetID = assetID
	r.upserts = append(r.upserts, rec)
	return nil
}

func TestPersistMediaMetadata_WiresVerifiedRowForMediaAsset(t *testing.T) {
	repo := &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	runner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{
			{CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080, FrameRate: "30/1", TimeBase: "1/90000", PixelFormat: "yuv420p", Duration: 10},
			{CodecType: "audio", CodecName: "aac", SampleRate: 48000, Channels: 2},
		},
		Format: mediaProbeFormat{FormatName: "mp4", Duration: 10},
	})}
	service := &AssetService{
		repo:          repo,
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(runner),
	}

	service.persistMediaMetadata(context.Background(), "asset-1", "/final/assets/asset-1.mp4", "video/mp4")

	if len(repo.upserts) != 1 {
		t.Fatalf("upserts = %d, want 1", len(repo.upserts))
	}
	record := repo.upserts[0]
	if record.AssetID != "asset-1" {
		t.Errorf("asset_id = %q, want asset-1", record.AssetID)
	}
	if !record.Verified() {
		t.Errorf("record not verified: %+v", record)
	}
	if record.MetadataSchemaVersion != MediaMetadataSchemaVersion {
		t.Errorf("schema version = %d, want %d", record.MetadataSchemaVersion, MediaMetadataSchemaVersion)
	}
	if record.Container != "mp4" || record.DurationMs != 10000 {
		t.Errorf("container/duration = %q/%d, want mp4/10000", record.Container, record.DurationMs)
	}
	if record.VideoCodec != "h264" || record.Width != 1920 || record.Height != 1080 {
		t.Errorf("video = %q %dx%d, want h264 1920x1080", record.VideoCodec, record.Width, record.Height)
	}
	if len(runner.commands) != 1 {
		t.Errorf("ffprobe invocations = %d, want exactly 1", len(runner.commands))
	}
}

func TestPersistMediaMetadata_SkipsNonMediaMIME(t *testing.T) {
	repo := &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	service := &AssetService{
		repo:          repo,
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(&recordingVideoRunner{}),
	}
	service.persistMediaMetadata(context.Background(), "font-1", "/final/assets/font-1.ttf", "font/ttf")
	if len(repo.upserts) != 0 {
		t.Fatalf("non-media asset must not create a metadata row, got %d upserts", len(repo.upserts))
	}
}

func TestMediaMetadataMetrics_ObserverAndSnapshot(t *testing.T) {
	metrics := NewMediaMetadataMetrics()
	var observed []MediaMetadataOutcome
	metrics.AddObserver(func(outcome MediaMetadataOutcome) {
		observed = append(observed, outcome)
	})
	metrics.Observe(MetadataOutcomeVerified)
	metrics.Observe(MetadataOutcomeProbeFailed)
	metrics.Observe(MetadataOutcomeVerified)

	if len(observed) != 3 || observed[0] != MetadataOutcomeVerified || observed[1] != MetadataOutcomeProbeFailed || observed[2] != MetadataOutcomeVerified {
		t.Fatalf("observed = %v", observed)
	}
	snapshot := metrics.Snapshot()
	if snapshot[MetadataOutcomeVerified] != 2 || snapshot[MetadataOutcomeProbeFailed] != 1 {
		t.Fatalf("snapshot = %v", snapshot)
	}
}

func TestPersistMediaMetadata_ObservesOutcomes(t *testing.T) {
	metrics := NewMediaMetadataMetrics()
	verifiedRunner := &recordingVideoRunner{probeOutput: mediaProbeJSON(t, mediaProbeDocument{
		Streams: []mediaProbeStream{{CodecType: "audio", CodecName: "aac", SampleRate: 48000, Channels: 2}},
		Format:  mediaProbeFormat{FormatName: "m4a", Duration: 2},
	})}
	verifiedService := &AssetService{
		repo:                 &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}},
		clock:                clock.System{},
		mediaMetadata:        newMediaMetadataResolverForTest(verifiedRunner),
		mediaMetadataMetrics: metrics,
	}
	verifiedService.persistMediaMetadata(context.Background(), "a-1", "/final/a-1.m4a", "audio/mp4")
	if got := metrics.Snapshot()[MetadataOutcomeVerified]; got != 1 {
		t.Errorf("verified = %d, want 1", got)
	}

	failingService := &AssetService{
		repo:                 &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}},
		clock:                clock.System{},
		mediaMetadata:        newMediaMetadataResolverForTest(&failingMediaRunner{err: errors.New("boom")}),
		mediaMetadataMetrics: metrics,
	}
	failingService.persistMediaMetadata(context.Background(), "a-2", "/final/a-2.mp4", "video/mp4")
	if got := metrics.Snapshot()[MetadataOutcomeProbeFailed]; got != 1 {
		t.Errorf("probe_failed = %d, want 1", got)
	}
}

func TestPersistMediaMetadata_SkipsOnProbeFailure(t *testing.T) {
	repo := &recordingMetadataRepo{rewriteAssetRepository: &rewriteAssetRepository{assets: map[string]*AssetRecord{}}}
	service := &AssetService{
		repo:          repo,
		clock:         clock.System{},
		mediaMetadata: newMediaMetadataResolverForTest(&failingMediaRunner{err: errors.New("boom")}),
	}
	service.persistMediaMetadata(context.Background(), "clip-1", "/final/assets/clip-1.mp4", "video/mp4")
	if len(repo.upserts) != 0 {
		t.Fatalf("failed probe must not invent a metadata row, got %d upserts", len(repo.upserts))
	}
}
