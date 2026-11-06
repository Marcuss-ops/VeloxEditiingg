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
	"velox-worker-agent/pkg/video/pipeline"
	"velox-worker-agent/pkg/video/plan"
)

type sliceNativeRenderer struct {
	calls int
	wire  []byte
}

func (*sliceNativeRenderer) Render(context.Context, *plan.RenderPlan) error { return nil }

func (*sliceNativeRenderer) RenderWithMetrics(context.Context, *plan.RenderPlan) (pipeline.RenderMetrics, error) {
	return pipeline.RenderMetrics{}, nil
}

func (r *sliceNativeRenderer) RenderCompiledPlanV2(_ context.Context, wire []byte, outputPath string) (pipeline.RenderMetrics, error) {
	r.calls++
	r.wire = append([]byte(nil), wire...)
	if err := os.WriteFile(outputPath, []byte("native-packet-mux"), 0o640); err != nil {
		return pipeline.RenderMetrics{}, err
	}
	return pipeline.RenderMetrics{TotalMs: 12, DurationSec: 1, ConcatMode: "packet_copy"}, nil
}

type sliceMediaProbeStub struct{}

func (sliceMediaProbeStub) ProbeMediaDetails(_ context.Context, path string) (publisher.MediaProbe, error) {
	if strings.HasSuffix(path, "stock.mp4") {
		return publisher.MediaProbe{HasVideo: true, VideoCodec: "h264", VideoCodecTag: "avc1", VideoProfile: "High", VideoLevel: 40, VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1, VideoTimeBaseNum: 1, VideoTimeBaseDen: 90000, VideoExtradataHash: "same-sps", VideoFrameCount: 480, DurationSec: 20, Width: 1920, Height: 1080}, nil
	}
	return publisher.MediaProbe{HasVideo: true, HasAudio: true, VideoCodec: "h264", AudioCodec: "aac", VideoProfile: "High", VideoLevel: 40, VideoPixelFormat: "yuv420p", VideoFPSNum: 24, VideoFPSDen: 1, VideoFrameCount: 24, DurationSec: 1, Width: 1920, Height: 1080}, nil
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
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "segment-0003", AssetID: "drive:stock-01", SHA256: strings.Repeat("b", 64), TimelineStartFrame: 0, FrameCount: 24, SourceInUS: 0, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "drive:stock-01", AssetKey: "drive:stock-01", SourceKind: "drive", DriveFileID: "stock-01", SHA256: strings.Repeat("b", 64), SizeBytes: 5, Kind: "video", DurationUS: 1_000_000, Width: 1920, Height: 1080, ProfileID: contract.CanonicalVideoProfileIDV1, FrameCount: 24, TimelineRevision: 7, TimelineSHA256: timelineHash, FirstFrameKeyframe: true, ClosedGOP: true},
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
	renderer := &sliceNativeRenderer{}
	runner := pipeline.NewRunner(nil, renderer, nil)
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceMediaProbeStub{}, outputRoot: t.TempDir()}
	spec := executorTaskSpec(VideoAssembleSliceID, "job-drive-slice", canonical, plan)
	result, err := exec.Execute(runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		"drive:stock-01": {AssetID: "drive:stock-01", Path: videoPath, SHA256: strings.Repeat("b", 64), Size: 5, Verified: true},
		"audio":          {AssetID: "audio", Path: audioPath, SHA256: copyOnlySHA([]byte("audio")), Size: 5, Verified: true},
	}), nil, spec)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("slice execution result=%+v err=%v", result, err)
	}
	if len(result.Outputs) != 1 || result.RawMetrics == nil || result.RawMetrics.ConcatMode != "packet_copy" || result.RawMetrics.EncodePasses != 0 || !result.RawMetrics.FinalConcatStreamCopy {
		t.Fatalf("slice result did not report copy-only remote assembly: %+v", result)
	}
	if renderer.calls != 1 {
		t.Fatalf("native packet renderer call count = %d, want one", renderer.calls)
	}
	if !strings.Contains(string(renderer.wire), `"bindings"`) || !strings.Contains(string(renderer.wire), `"output_path"`) {
		t.Fatalf("native render did not receive the bound compiled plan: %s", renderer.wire)
	}
}

func TestVideoAssembleSlice_RejectsNonKeyframeOffsetBeforeFFmpeg(t *testing.T) {
	plan := driveSlicePlan()
	plan.VideoTracks[0].Segments[0].SourceInUS = 14_000_000
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	renderer := &sliceNativeRenderer{}
	runner := pipeline.NewRunner(nil, renderer, nil)
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceMediaProbeStub{}, outputRoot: t.TempDir()}
	spec := executorTaskSpec(VideoAssembleSliceID, "job-drive-slice-invalid-cut", canonical, plan)
	result, err := exec.Execute(context.Background(), nil, spec)
	if err != nil || result.Status != "failed" {
		t.Fatalf("invalid copy plan result=%+v err=%v, want immediate failure", result, err)
	}
	if result.ErrorCode != "DRIVE_SLICE_PLAN_INVALID" || renderer.calls != 0 {
		t.Fatalf("invalid cut was not rejected before native mux: result=%+v native_calls=%d", result, renderer.calls)
	}
}

func TestVideoAssembleSlice_RejectsLegacyStockWindowWithoutProbingOrCutting(t *testing.T) {
	plan := driveSlicePlan()
	plan.VideoTracks[0].Segments[0].FrameCount = 13
	plan.VideoTracks[0].Segments[0].SourceInUS = 4_475_000
	plan.VideoTracks[0].Segments[0].SourceDurationUS = 525_000
	plan.Assets[0].DurationUS = 5_000_000
	plan.Assets[0].FrameCount = 120
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	renderer := &sliceNativeRenderer{}
	runner := pipeline.NewRunner(nil, renderer, nil)
	exec := &videoAssembleSliceExecutor{runner: runner, prober: sliceMediaProbeStub{}, outputRoot: t.TempDir()}
	spec := executorTaskSpec(VideoAssembleSliceID, "job-stock-window", canonical, plan)
	spec.Payload["runtime_assets"] = []interface{}{map[string]interface{}{
		"asset_id": "drive:stock-01", "drive_file_id": "stock-01", "role": "stock",
	}}
	result, err := exec.Execute(context.Background(), nil, spec)
	if err != nil || result.Status != "failed" {
		t.Fatalf("legacy stock window result=%+v err=%v, want immediate failure", result, err)
	}
	if !strings.Contains(result.ErrorDetail, "whole-clip stock_pool slots only") || renderer.calls != 0 {
		t.Fatalf("unsafe stock window was not rejected before render: result=%+v native_calls=%d", result, renderer.calls)
	}
}
