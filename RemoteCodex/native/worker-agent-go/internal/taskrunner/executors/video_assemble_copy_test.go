package executors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/chunkfactory"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

type copyOnlyRenderClient struct {
	planJSON []byte
}

// keyframeUnsafeRenderClient always fails the native render with the exact
// keyframe-safety rejection emitted by the C++ packet mux. It records whether
// RenderCompiledPlanV2 was invoked so the test can prove the executor fails
// BEFORE publishing any output and never falls back to a transcode.
type keyframeUnsafeRenderClient struct {
	renderAttempted bool
}

func (c *keyframeUnsafeRenderClient) Render(context.Context, *plan.RenderPlan) error { return nil }

func (c *keyframeUnsafeRenderClient) RenderWithMetrics(context.Context, *plan.RenderPlan) (pipeline.RenderMetrics, error) {
	return pipeline.RenderMetrics{}, nil
}

func (c *keyframeUnsafeRenderClient) RenderCompiledPlanV2(_ context.Context, _ []byte, _ string) (pipeline.RenderMetrics, error) {
	c.renderAttempted = true
	return pipeline.RenderMetrics{}, errors.New("engine failed: exit status 1 (stderr=errore rendering: copy-only packet mux failed: copy-only source window must start on an exact video keyframe: /cache/base.mp4 source_in_us=60000000)")
}

func (c *copyOnlyRenderClient) Render(context.Context, *plan.RenderPlan) error { return nil }

func (c *copyOnlyRenderClient) RenderWithMetrics(context.Context, *plan.RenderPlan) (pipeline.RenderMetrics, error) {
	return pipeline.RenderMetrics{}, nil
}

func (c *copyOnlyRenderClient) RenderCompiledPlanV2(_ context.Context, data []byte, outputPath string) (pipeline.RenderMetrics, error) {
	c.planJSON = append([]byte(nil), data...)
	if err := os.WriteFile(outputPath, []byte("assembled"), 0o640); err != nil {
		return pipeline.RenderMetrics{}, err
	}
	return pipeline.RenderMetrics{TotalMs: 3}, nil
}

func copyOnlySHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func copyOnlyPlan(videoSHA, audioSHA string, videoSize, audioSize int64) *contract.CompiledRenderPlanV2 {
	const timelineSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	return &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 7, TimelineSHA256: timelineSHA, DurationUS: 1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p", ProfileID: contract.CanonicalVideoProfileIDV1,
			CodecProfile: "high", CodecLevel: "4.0", GOPSize: 48, BFrames: 0,
			ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA, SizeBytes: audioSize, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 7, TimelineSHA256: timelineSHA},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "fragment-1", AssetID: "video", SHA256: videoSHA, TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video", SHA256: videoSHA, SizeBytes: videoSize, Kind: "prepared_video_fragment", MIME: "video/mp4", DurationUS: 1_000_000, Width: 1920, Height: 1080, ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24, TimelineRevision: 7, TimelineSHA256: timelineSHA, TimelineStartFrame: 0, FirstFrameKeyframe: true, ClosedGOP: true},
			{AssetID: "audio", SHA256: audioSHA, SizeBytes: audioSize, Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
		},
	}
}

func TestVideoAssembleCopy_UsesNativeV2WithoutFFmpegFallback(t *testing.T) {
	video := []byte("prepared-video")
	audio := []byte("final-audio")
	videoSHA, audioSHA := copyOnlySHA(video), copyOnlySHA(audio)
	plan := copyOnlyPlan(videoSHA, audioSHA, int64(len(video)), int64(len(audio)))
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	client := &copyOnlyRenderClient{}
	runner := pipeline.NewRunner(nil, client, logger.New(logger.WarnLevel, os.Stderr))
	exec := NewVideoAssembleCopy(runner, t.TempDir())
	videoPath, audioPath := t.TempDir()+"/video.mp4", t.TempDir()+"/audio.m4a"
	if err := os.WriteFile(videoPath, video, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, audio, 0o640); err != nil {
		t.Fatal(err)
	}
	spec := executorTaskSpec(VideoAssembleCopyID, "job-copy", canonical, plan)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"video": {AssetID: "video", Path: videoPath, SHA256: videoSHA, Size: int64(len(video))},
		"audio": {AssetID: "audio", Path: audioPath, SHA256: audioSHA, Size: int64(len(audio))},
	}), nil, spec)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("Execute = %+v, %v", result, err)
	}
	if strings.Contains(string(client.planJSON), "ffmpeg") || !strings.Contains(string(client.planJSON), "bindings") {
		t.Fatalf("native wire document does not reflect copy-only boundary: %s", client.planJSON)
	}
	if result.RawMetrics == nil || result.RawMetrics.FramesEncoded != 0 {
		t.Fatalf("raw frames_encoded = %#v, want 0", result.RawMetrics)
	}
}

func TestVideoAssembleCopy_EmitsReusableChunkManifestWhenEnabled(t *testing.T) {
	video := []byte("prepared-video")
	audio := []byte("final-audio")
	videoSHA, audioSHA := copyOnlySHA(video), copyOnlySHA(audio)
	plan := copyOnlyPlan(videoSHA, audioSHA, int64(len(video)), int64(len(audio)))
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	store, err := chunkfactory.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client := &copyOnlyRenderClient{}
	runner := pipeline.NewRunner(nil, client, logger.New(logger.WarnLevel, os.Stderr))
	exec := NewVideoAssembleCopyWithChunkStore(runner, t.TempDir(), store)
	videoPath, audioPath := t.TempDir()+"/video.mp4", t.TempDir()+"/audio.m4a"
	if err := os.WriteFile(videoPath, video, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, audio, 0o640); err != nil {
		t.Fatal(err)
	}
	const jobID = "job-chunk-manifest"
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"video": {AssetID: "video", Path: videoPath, SHA256: videoSHA, Size: int64(len(video))},
		"audio": {AssetID: "audio", Path: audioPath, SHA256: audioSHA, Size: int64(len(audio))},
	}), nil, executorTaskSpec(VideoAssembleCopyID, jobID, canonical, plan))
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("Execute = %+v, %v", result, err)
	}
	data, err := os.ReadFile(store.ManifestPath(jobID))
	if err != nil {
		t.Fatal(err)
	}
	var manifest chunkfactory.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != chunkfactory.ManifestVersion || len(manifest.Chunks) != 1 {
		t.Fatalf("manifest = %+v; want v%d with one chunk", manifest, chunkfactory.ManifestVersion)
	}
	chunk, err := store.GetChunk(manifest.Chunks[0].ChunkID)
	if err != nil || chunk == nil {
		t.Fatalf("stored chunk = %+v, %v", chunk, err)
	}
	if chunk.PayloadSHA256 != videoSHA || chunk.SizeBytes != int64(len(video)) {
		t.Fatalf("stored chunk identity = %+v", chunk)
	}
}

func TestVideoAssembleCopy_UsesArtifactStagingResolver(t *testing.T) {
	video := []byte("prepared-video")
	audio := []byte("final-audio")
	videoSHA, audioSHA := copyOnlySHA(video), copyOnlySHA(audio)
	plan := copyOnlyPlan(videoSHA, audioSHA, int64(len(video)), int64(len(audio)))
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	resolver, err := storage.New(storage.Config{
		CacheDir: filepath.Join(root, "cache"), TempDir: filepath.Join(root, "temp"), ArtifactDir: filepath.Join(root, "artifact"),
		ArtifactStaging: storage.ArtifactStagingConfig{Enabled: true, Dir: filepath.Join(root, "shm"), MaxPercent: 99, ReserveBytes: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	client := &copyOnlyRenderClient{}
	runner := pipeline.NewRunner(nil, client, logger.New(logger.WarnLevel, os.Stderr))
	exec := NewVideoAssembleCopy(runner, filepath.Join(root, "legacy-output"))
	videoPath, audioPath := filepath.Join(root, "video.mp4"), filepath.Join(root, "audio.m4a")
	if err := os.WriteFile(videoPath, video, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, audio, 0o640); err != nil {
		t.Fatal(err)
	}
	spec := executorTaskSpec(VideoAssembleCopyID, "job-copy-staging", canonical, plan)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"video": {AssetID: "video", Path: videoPath, SHA256: videoSHA, Size: int64(len(video))},
		"audio": {AssetID: "audio", Path: audioPath, SHA256: audioSHA, Size: int64(len(audio))},
	}), &storageExecutionContext{resolver: resolver}, spec)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("Execute = %+v, %v", result, err)
	}
	if len(result.Outputs) != 1 || !strings.HasPrefix(result.Outputs[0].URI, resolver.Config().ArtifactStaging.Dir) {
		t.Fatalf("output URI = %+v; want ARTIFACT_STAGING root %q", result.Outputs, resolver.Config().ArtifactStaging.Dir)
	}
}

// TestVideoAssembleCopy_RejectsNonKeyframeSafeCut pins the fail-closed
// keyframe-safety gate (plan §9): a source window that does not start on an
// exact video keyframe is rejected with COPY_ONLY_NOT_KEYFRAME_SAFE, produces
// NO output artifact, and — crucially — is never silently repaired by a
// decode→encode fallback. The executor only ever calls the native packet mux;
// there is no transcode path to fall back to.
func TestVideoAssembleCopy_RejectsNonKeyframeSafeCut(t *testing.T) {
	video := []byte("prepared-video")
	audio := []byte("final-audio")
	videoSHA, audioSHA := copyOnlySHA(video), copyOnlySHA(audio)
	plan := copyOnlyPlan(videoSHA, audioSHA, int64(len(video)), int64(len(audio)))
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}

	client := &keyframeUnsafeRenderClient{}
	runner := pipeline.NewRunner(nil, client, logger.New(logger.WarnLevel, os.Stderr))
	outputRoot := t.TempDir()
	exec := NewVideoAssembleCopy(runner, outputRoot)

	videoPath, audioPath := filepath.Join(outputRoot, "video.mp4"), filepath.Join(outputRoot, "audio.m4a")
	if err := os.WriteFile(videoPath, video, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, audio, 0o640); err != nil {
		t.Fatal(err)
	}
	spec := executorTaskSpec(VideoAssembleCopyID, "job-keyframe-unsafe", canonical, plan)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"video": {AssetID: "video", Path: videoPath, SHA256: videoSHA, Size: int64(len(video))},
		"audio": {AssetID: "audio", Path: audioPath, SHA256: audioSHA, Size: int64(len(audio))},
	}), nil, spec)
	if err != nil {
		t.Fatalf("Execute returned error (executors surface failures in the result): %v", err)
	}
	if result.Status != "failed" || result.ErrorCode != "COPY_ONLY_NOT_KEYFRAME_SAFE" {
		t.Fatalf("result = %+v; want failed/COPY_ONLY_NOT_KEYFRAME_SAFE", result)
	}
	if !client.renderAttempted {
		t.Fatalf("native render was never attempted; the keyframe gate must run after plan validation")
	}
	// No output must be published: the executor failed before the artifact
	// step, and there is no decode→encode fallback that could emit one.
	if len(result.Outputs) != 0 {
		t.Fatalf("rejected render published outputs: %+v", result.Outputs)
	}
	if _, statErr := os.Stat(filepath.Join(outputRoot, "job-keyframe-unsafe.mp4")); !os.IsNotExist(statErr) {
		t.Fatalf("rejected render left an output artifact: %v", statErr)
	}
}

// TestVideoAssembleCopy_AcceptsFragmentedProfileWhenGateOpen pins the
// completed hand-off: fMP4 changes only the final mux layout, so prepared
// source assets retain the canonical stream profile identity.
func TestVideoAssembleCopy_AcceptsFragmentedProfileWhenGateOpen(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	video := []byte("prepared-video")
	audio := []byte("final-audio")
	videoSHA, audioSHA := copyOnlySHA(video), copyOnlySHA(audio)
	plan := copyOnlyPlan(videoSHA, audioSHA, int64(len(video)), int64(len(audio)))
	plan.Output.ProfileID = contract.CanonicalVideoProfileFMP4StreamV1
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	client := &copyOnlyRenderClient{}
	runner := pipeline.NewRunner(nil, client, logger.New(logger.WarnLevel, os.Stderr))
	exec := NewVideoAssembleCopy(runner, t.TempDir())
	spec := executorTaskSpec(VideoAssembleCopyID, "job-fmp4", canonical, plan)
	if err := exec.Validate(spec); err != nil {
		t.Fatalf("Validate error = %v; want fragmented profile admission", err)
	}
}

func TestValidateFragmentedMP4OutputRequiresNativeEvidenceAndMoof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.mp4")
	boxBytes := func(kind string, payload []byte) []byte {
		var header [8]byte
		binary.BigEndian.PutUint32(header[:4], uint32(8+len(payload)))
		copy(header[4:], kind)
		return append(header[:], payload...)
	}
	if err := os.WriteFile(path, bytes.Join([][]byte{
		boxBytes("ftyp", []byte("isom")),
		boxBytes("moov", []byte("init")),
		boxBytes("moof", []byte("fragment")),
	}, nil), 0o640); err != nil {
		t.Fatal(err)
	}
	native := pipeline.RenderMetrics{ConcatMode: "packet_copy", OutputDurable: true}
	if err := validateFragmentedMP4Output(path, native); err != nil {
		t.Fatalf("valid fMP4 rejected: %v", err)
	}

	if err := os.WriteFile(path, boxBytes("moov", []byte("progressive")), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := validateFragmentedMP4Output(path, native); err == nil || !strings.Contains(err.Error(), "no moof") {
		t.Fatalf("missing moof error = %v; want explicit moof rejection", err)
	}

	native.BackwardSeekSeen = true
	if err := validateFragmentedMP4Output(path, native); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("backward seek error = %v; want append-only rejection", err)
	}
}

func TestCopyOnlyRawMetricsProjectsNativePacketCopyFacts(t *testing.T) {
	plan := &contract.CompiledRenderPlanV2{VideoTracks: []contract.VideoTrackV2{{Segments: []contract.VideoSegmentV2{{}, {}}}}}
	metrics := copyOnlyRawMetrics(plan, pipeline.RenderMetrics{
		TotalBytesRead: 1234, StorageBytesRead: 1000, StorageBytesWritten: 2000,
		CPUUserMs: 7, CPUSystemMs: 3, TotalMs: 250, DurationSec: 2.5,
		SpeedX: 9, EncodePasses: 0, SegmentsTotal: 2, SegmentsPacketCopy: 2,
		PacketCopyRatio: 100, EngineSpawnCount: 1, EngineExternalSpawnCount: 0,
		FirstOutputWriteMS: 4, TrailerToPublishUS: 6000,
	}, executor.ArtifactRef{SizeBytes: 4096, Hash: "sha"})
	if metrics.InputBytes != 1234 || metrics.OutputFileSize != 4096 || metrics.CpuTimeMs != 10 ||
		metrics.DiskReadBytes != 1000 || metrics.DiskWriteBytes != 2000 || metrics.VideoConcatMs != 250 ||
		metrics.SegmentsPacketCopy != 2 || metrics.PacketCopyRatio != 100 || !metrics.FinalConcatStreamCopy ||
		metrics.ProcessSpawnCount != 1 || metrics.OutputFinalizeMs != 6 {
		t.Fatalf("native packet-copy metrics projection = %+v", metrics)
	}
}

func TestCopyOnlyRenderErrorCode(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil error defaults to generic", nil, "PACKET_COPY_FAILED"},
		{"exact keyframe rejection", errors.New("engine failed: copy-only source window must start on an exact video keyframe: /c.mp4 source_in_us=60000000"), "COPY_ONLY_NOT_KEYFRAME_SAFE"},
		{"mixed-path keyframe rejection", errors.New("segment_execution_rejected: source window is not keyframe-safe for packet copy (segment 0)"), "COPY_ONLY_NOT_KEYFRAME_SAFE"},
		{"unrelated mux failure stays generic", errors.New("engine failed: copy-only packet mux failed: avformat_alloc_output_context2"), "PACKET_COPY_FAILED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := copyOnlyRenderErrorCode(tc.err); got != tc.want {
				t.Fatalf("copyOnlyRenderErrorCode(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func executorTaskSpec(id, jobID string, canonical []byte, plan *contract.CompiledRenderPlanV2) executor.TaskSpec {
	sha := contract.HashCompiledPlanV2(canonical)
	return executor.TaskSpec{Version: 1, JobID: jobID, ExecutorID: id, Payload: map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical), contract.PayloadKeyCompiledRenderPlanSHA: sha,
	}}
}
