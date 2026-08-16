package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/contract"
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
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p", ProfileID: contract.CanonicalVideoProfileIDV1,
			CodecProfile: "high", CodecLevel: "4.0", GOPSize: 60, BFrames: 0,
			ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: audioSHA, SizeBytes: audioSize, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 7, TimelineSHA256: timelineSHA},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "fragment-1", AssetID: "video", SHA256: videoSHA, TimelineStartFrame: 0, FrameCount: 30, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video", SHA256: videoSHA, SizeBytes: videoSize, Kind: "prepared_video_fragment", MIME: "video/mp4", DurationUS: 1_000_000, Width: 1920, Height: 1080, ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 30, TimelineRevision: 7, TimelineSHA256: timelineSHA, TimelineStartFrame: 0, FirstFrameKeyframe: true, ClosedGOP: true},
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
	if got := result.Metrics["frames_encoded"]; got != int64(0) {
		t.Fatalf("frames_encoded = %v, want 0", got)
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

func executorTaskSpec(id, jobID string, canonical []byte, plan *contract.CompiledRenderPlanV2) executor.TaskSpec {
	sha := contract.HashCompiledPlanV2(canonical)
	return executor.TaskSpec{Version: 1, JobID: jobID, ExecutorID: id, Payload: map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: string(canonical), contract.PayloadKeyCompiledRenderPlanSHA: sha,
	}}
}
