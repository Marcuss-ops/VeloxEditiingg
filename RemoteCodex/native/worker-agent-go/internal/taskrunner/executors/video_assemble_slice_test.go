package executors

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

type sliceFFmpegRunner struct{ args []string }

func (r *sliceFFmpegRunner) Run(_ context.Context, req ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.args = append([]string(nil), req.Args...)
	if err := os.WriteFile(req.Args[len(req.Args)-1], []byte("remote-render"), 0o640); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{ProcessWallMS: 12}, nil
}

type sliceMediaProbeStub struct{}

func (sliceMediaProbeStub) ProbeMediaDetails(_ context.Context, path string) (publisher.MediaProbe, error) {
	if strings.HasSuffix(path, "stock.mp4") {
		return publisher.MediaProbe{HasVideo: true, DurationSec: 20}, nil
	}
	return publisher.MediaProbe{HasVideo: true, HasAudio: true, VideoCodec: "h264", AudioCodec: "aac", DurationSec: 1, Width: 1920, Height: 1080}, nil
}

func driveSlicePlan() *contract.CompiledRenderPlanV2 {
	const timelineHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	return &contract.CompiledRenderPlanV2{
		PlanVersion: 2, TimelineRevision: 7, TimelineSHA256: timelineHash, DurationUS: 1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080,
			FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p", ProfileID: contract.CanonicalVideoProfileIDV1,
			CodecProfile: "high", CodecLevel: "4.0", GOPSize: 48, BFrames: 0,
			ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
		},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: copyOnlySHA([]byte("audio")), SizeBytes: 5, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 7, TimelineSHA256: timelineHash},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "segment-0003", AssetID: "drive:stock-01", TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 14_000_000, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "drive:stock-01", AssetKey: "drive:stock-01", SourceKind: "drive", DriveFileID: "stock-01", Kind: "video", DurationUS: 20_000_000},
			{AssetID: "audio", SHA256: copyOnlySHA([]byte("audio")), SizeBytes: 5, Kind: "final_audio", DurationUS: 1_000_000},
		},
	}
}

func TestVideoAssembleSlice_RendersDriveSourceWindowsWithoutIntermediateFragments(t *testing.T) {
	plan := driveSlicePlan()
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	videoPath := filepath.Join(t.TempDir(), "stock.mp4")
	audioPath := filepath.Join(t.TempDir(), "audio.m4a")
	if err := os.WriteFile(videoPath, []byte("stock"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, []byte("audio"), 0o640); err != nil {
		t.Fatal(err)
	}
	runner := &sliceFFmpegRunner{}
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceMediaProbeStub{}, outputRoot: t.TempDir()}
	spec := executorTaskSpec(VideoAssembleSliceID, "job-drive-slice", canonical, plan)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"drive:stock-01": {AssetID: "drive:stock-01", Path: videoPath, SHA256: strings.Repeat("b", 64), Size: 5, Verified: true},
		"audio":          {AssetID: "audio", Path: audioPath, SHA256: copyOnlySHA([]byte("audio")), Size: 5, Verified: true},
	}), nil, spec)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("slice execution result=%+v err=%v", result, err)
	}
	if len(result.Outputs) != 1 || result.RawMetrics == nil || result.RawMetrics.ConcatMode != "reencode" {
		t.Fatalf("slice result did not report remote final render: %+v", result)
	}
	joined := strings.Join(runner.args, " ")
	for _, want := range []string{"trim=start=14.000000:duration=1.000000", "concat=n=1:v=1:a=0", "libx264", "-c:a copy"} {
		if !strings.Contains(joined, want) {
			t.Errorf("FFmpeg args missing %q: %s", want, joined)
		}
	}
}
