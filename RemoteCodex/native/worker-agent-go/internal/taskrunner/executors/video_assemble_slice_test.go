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

type sliceFFmpegRunner struct {
	args        [][]string
	concatInput string
}

func (r *sliceFFmpegRunner) Run(_ context.Context, req ffmpegrunner.FFmpegRequest) (ffmpegrunner.FFmpegResult, error) {
	r.args = append(r.args, append([]string(nil), req.Args...))
	for i := 0; i+1 < len(req.Args); i++ {
		if req.Args[i] == "-i" && strings.HasSuffix(req.Args[i+1], "segments.ffconcat") {
			data, err := os.ReadFile(req.Args[i+1])
			if err != nil {
				return ffmpegrunner.FFmpegResult{}, err
			}
			r.concatInput = string(data)
		}
	}
	if err := os.WriteFile(req.Args[len(req.Args)-1], []byte("remote-render"), 0o640); err != nil {
		return ffmpegrunner.FFmpegResult{}, err
	}
	return ffmpegrunner.FFmpegResult{ProcessWallMS: 12}, nil
}

type sliceMediaProbeStub struct{}

func (sliceMediaProbeStub) ProbeMediaDetails(_ context.Context, path string) (publisher.MediaProbe, error) {
	if strings.HasSuffix(path, "stock.mp4") {
		return publisher.MediaProbe{HasVideo: true, VideoCodec: "h264", VideoCodecTag: "avc1", VideoProfile: "High", VideoLevel: 40, VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1, VideoTimeBaseNum: 1, VideoTimeBaseDen: 90000, VideoExtradataHash: "same-sps", VideoFrameCount: 480, DurationSec: 20, Width: 1920, Height: 1080}, nil
	}
	return publisher.MediaProbe{HasVideo: true, HasAudio: true, VideoCodec: "h264", AudioCodec: "aac", VideoProfile: "High", VideoLevel: 40, VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1, VideoFrameCount: 24, DurationSec: 1, Width: 1920, Height: 1080}, nil
}

type sliceAVC3RemuxProbeStub struct{}

func (sliceAVC3RemuxProbeStub) ProbeMediaDetails(_ context.Context, path string) (publisher.MediaProbe, error) {
	base := publisher.MediaProbe{HasVideo: true, VideoCodec: "h264", VideoProfile: "High", VideoLevel: 40,
		VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1, VideoFrameCount: 24,
		DurationSec: 1, Width: 1920, Height: 1080, VideoBFrames: 0}
	switch {
	case strings.HasSuffix(path, "stock-avc1.mp4"):
		base.VideoCodecTag, base.VideoTimeBaseNum, base.VideoTimeBaseDen, base.VideoExtradataHash = "avc1", 1, 12288, "stock-sps"
	case strings.HasSuffix(path, "overlay-avc3.mp4"):
		base.VideoCodecTag, base.VideoTimeBaseNum, base.VideoTimeBaseDen, base.VideoExtradataHash = "avc3", 1, 90000, "overlay-sps"
	case strings.HasSuffix(path, "normalized-000.mp4"):
		base.VideoCodecTag, base.VideoTimeBaseNum, base.VideoTimeBaseDen, base.VideoExtradataHash = "avc3", 1, 12288, "overlay-sps"
	default:
		return publisher.MediaProbe{}, os.ErrNotExist
	}
	return base, nil
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
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "segment-0003", AssetID: "drive:stock-01", TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "drive:stock-01", AssetKey: "drive:stock-01", SourceKind: "drive", DriveFileID: "stock-01", SHA256: strings.Repeat("b", 64), SizeBytes: 5, Kind: "video", DurationUS: 20_000_000, Width: 1920, Height: 1080, ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 480, FirstFrameKeyframe: true, ClosedGOP: true},
			{AssetID: "audio", SHA256: copyOnlySHA([]byte("audio")), SizeBytes: 5, Kind: "final_audio", DurationUS: 1_000_000},
		},
	}
}

func TestVideoAssembleSlice_CopiesDriveSourceWindowsWithoutReencoding(t *testing.T) {
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
	if len(result.Outputs) != 1 || result.RawMetrics == nil || result.RawMetrics.ConcatMode != "stream_copy" || result.RawMetrics.EncodePasses != 0 || !result.RawMetrics.FinalConcatStreamCopy {
		t.Fatalf("slice result did not report copy-only remote assembly: %+v", result)
	}
	if len(runner.args) != 1 {
		t.Fatalf("FFmpeg call count = %d, want one stream-copy concat/mux", len(runner.args))
	}
	copyArgs := strings.Join(runner.args[0], " ")
	for _, want := range []string{"-f concat", "-c:v copy", "-tag:v avc1", "-c:a copy", "-frames:v 24"} {
		if !strings.Contains(copyArgs, want) {
			t.Errorf("FFmpeg stream-copy args missing %q: %s", want, copyArgs)
		}
	}
	if strings.Contains(copyArgs, "libx264") || strings.Contains(copyArgs, "filter_complex") {
		t.Fatalf("copy path must not contain an encoder or filter graph: %s", copyArgs)
	}
	if !strings.Contains(runner.concatInput, "inpoint 0\noutpoint 1.000000") {
		t.Fatalf("concat manifest does not contain the expected source window: %q", runner.concatInput)
	}
	if strings.Contains(copyArgs, "14.000000") {
		t.Fatalf("copy path unexpectedly trims a non-keyframe source offset: %s", copyArgs)
	}
}

func TestVideoAssembleSlice_RejectsNonKeyframeOffsetBeforeFFmpeg(t *testing.T) {
	plan := driveSlicePlan()
	plan.VideoTracks[0].Segments[0].SourceInUS = 14_000_000
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	runner := &sliceFFmpegRunner{}
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceMediaProbeStub{}, outputRoot: t.TempDir()}
	spec := executorTaskSpec(VideoAssembleSliceID, "job-drive-slice-invalid-cut", canonical, plan)
	result, err := exec.Execute(context.Background(), nil, spec)
	if err != nil || result.Status != "failed" {
		t.Fatalf("invalid copy plan result=%+v err=%v, want immediate failure", result, err)
	}
	if result.ErrorCode != "DRIVE_SLICE_PLAN_INVALID" || len(runner.args) != 0 {
		t.Fatalf("invalid cut was not rejected before FFmpeg: result=%+v ffmpeg_calls=%d", result, len(runner.args))
	}
}

func TestSameSliceCopySignatureRejectsDivergentTimebaseAndCodecData(t *testing.T) {
	base := publisher.MediaProbe{
		VideoCodec: "h264", VideoCodecTag: "avc1", VideoProfile: "High", VideoLevel: 40,
		VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1,
		VideoTimeBaseNum: 1, VideoTimeBaseDen: 90000, VideoExtradataHash: "sps-1",
	}
	other := base
	other.VideoTimeBaseDen = 12288
	if sameSliceCopySignature(base, other) {
		t.Fatal("different source timebases were accepted for packet copy")
	}
	other = base
	other.VideoExtradataHash = "sps-2"
	if sameSliceCopySignature(base, other) {
		t.Fatal("different H.264 codec data were accepted for packet copy")
	}
}

func TestCompatibleAVC1AVC3CopyPairAllowsContainerVariantWithoutTranscoding(t *testing.T) {
	stock := publisher.MediaProbe{
		VideoCodec: "h264", VideoCodecTag: "avc1", VideoProfile: "High", VideoLevel: 40,
		VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1,
		VideoTimeBaseNum: 1, VideoTimeBaseDen: 12288, VideoExtradataHash: "stock-sps-pps",
	}
	overlay := stock
	overlay.VideoCodecTag = "avc3"
	overlay.VideoTimeBaseDen = 90000
	overlay.VideoExtradataHash = "overlay-inband-sps-pps"
	if !compatibleAVC1AVC3CopyPair(stock, overlay) {
		t.Fatal("compatible avc1/avc3 H.264 streams with in-band parameter sets were rejected")
	}
	if sameSliceCopySignature(stock, overlay) {
		t.Fatal("container variants should remain distinct signatures")
	}
	args, err := sliceConcatMuxArgs("segments.ffconcat", "audio.m4a", "out.mp4", driveSlicePlan(), "avc3")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-tag:v avc3") || strings.Contains(joined, "libx264") {
		t.Fatalf("mixed container stream-copy must signal avc3 without encoding: %s", joined)
	}

	divergent := overlay
	divergent.VideoLevel = 41
	if compatibleAVC1AVC3CopyPair(stock, divergent) {
		t.Fatal("divergent H.264 levels must still be rejected")
	}
	divergent = overlay
	divergent.VideoCodecTag = "avc1"
	if compatibleAVC1AVC3CopyPair(stock, divergent) {
		t.Fatal("avc1-to-avc1 parameter-set drift must still be rejected")
	}
}

func TestVideoAssembleSliceRemuxesOnlyAVC3TimebaseWithPacketCopy(t *testing.T) {
	root := t.TempDir()
	stockPath := filepath.Join(root, "stock-avc1.mp4")
	overlayPath := filepath.Join(root, "overlay-avc3.mp4")
	for _, path := range []string{stockPath, overlayPath} {
		if err := os.WriteFile(path, []byte("source"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	runner := &sliceFFmpegRunner{}
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceAVC3RemuxProbeStub{}, outputRoot: root}
	assets := []contract.AssetRefV2{
		{AssetID: "stock", Kind: "video", ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24},
		{AssetID: "overlay", Kind: "video", ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24},
	}
	bindings := runtimeassets.Bindings{
		"stock":   {AssetID: "stock", Path: stockPath, Verified: true},
		"overlay": {AssetID: "overlay", Path: overlayPath, Verified: true},
	}
	paths, err := exec.remuxAVC3Timebases(context.Background(), &contract.CompiledRenderPlanV2{Assets: assets}, bindings, sliceCopyCompatibility{
		OutputTag: "avc3", TimeBaseNum: 1, TimeBaseDen: 12288, RemuxAssetIDs: map[string]struct{}{"overlay": {}},
	}, root)
	if err != nil {
		t.Fatal(err)
	}
	if paths["overlay"] == "" || paths["stock"] != "" {
		t.Fatalf("remux paths = %#v, want only the avc3 overlay remuxed", paths)
	}
	if len(runner.args) != 1 {
		t.Fatalf("FFmpeg remux calls = %d, want one", len(runner.args))
	}
	args := strings.Join(runner.args[0], " ")
	for _, want := range []string{"-c:v copy", "-tag:v avc3", "-video_track_timescale 12288"} {
		if !strings.Contains(args, want) {
			t.Errorf("packet-copy normalization args missing %q: %s", want, args)
		}
	}
	if strings.Contains(args, "libx264") || strings.Contains(args, "libx265") {
		t.Fatalf("time-base normalization unexpectedly encodes video: %s", args)
	}
}
