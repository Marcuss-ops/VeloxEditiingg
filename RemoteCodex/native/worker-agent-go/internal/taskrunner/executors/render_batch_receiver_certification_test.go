package executors

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/ffmpegrunner"
)

func syntheticCertificationPlan(t *testing.T, assets map[string][]byte) *contract.CompiledRenderPlanV2 {
	t.Helper()
	const timelineSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	asset := func(id string, kind string, durationUS int64) contract.AssetRefV2 {
		data := assets[id]
		return contract.AssetRefV2{
			AssetID: id, SHA256: receiverOnlySHA(data), SizeBytes: int64(len(data)),
			Kind: kind, DurationUS: durationUS, Width: 640, Height: 360,
		}
	}
	return &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 9,
		TimelineSHA256:   timelineSHA,
		DurationUS:       53_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "libx264", Width: 640, Height: 360,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p",
		},
		FinalAudio: contract.FinalAudioV2{
			Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio-master",
			SHA256: receiverOnlySHA(assets["audio-master"]), SizeBytes: int64(len(assets["audio-master"])),
			Codec: "aac", SampleRateHz: 48_000, Channels: 2, DurationUS: 53_000_000,
			TimelineRevision: 9, TimelineSHA256: timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "synthetic-certification", Segments: []contract.VideoSegmentV2{
				// A: source 33.2–38.8, timeline 12.4–18.0.
				{SegmentID: "A", AssetID: "video-A", SHA256: receiverOnlySHA(assets["video-A"]), TimelineStartFrame: 372, FrameCount: 168, SourceInUS: 33_200_000, SourceDurationUS: 5_600_000},
				// B: source 7.1–13.6, timeline 22.0–28.5.
				{SegmentID: "B", AssetID: "video-B", SHA256: receiverOnlySHA(assets["video-B"]), TimelineStartFrame: 660, FrameCount: 195, SourceInUS: 7_100_000, SourceDurationUS: 6_500_000},
				// C: source 91–95, timeline 35–39.
				{SegmentID: "C", AssetID: "video-C", SHA256: receiverOnlySHA(assets["video-C"]), TimelineStartFrame: 1050, FrameCount: 120, SourceInUS: 91_000_000, SourceDurationUS: 4_000_000},
				// D: source 15.5–21.5, timeline 47–53.
				{SegmentID: "D", AssetID: "video-D", SHA256: receiverOnlySHA(assets["video-D"]), TimelineStartFrame: 1410, FrameCount: 180, SourceInUS: 15_500_000, SourceDurationUS: 6_000_000},
			},
		}},
		Assets: []contract.AssetRefV2{
			asset("video-A", "video", 120_000_000),
			asset("video-B", "video", 120_000_000),
			asset("video-C", "video", 120_000_000),
			asset("video-D", "video", 120_000_000),
			asset("audio-master", "final_audio", 53_000_000),
		},
	}
}

func TestRenderBatchReceiverCertification_SyntheticABCDPlacementDurationAudioCopyCleanup(t *testing.T) {
	assets := map[string][]byte{
		"video-A":      []byte("synthetic-red-A"),
		"video-B":      []byte("synthetic-green-B"),
		"video-C":      []byte("synthetic-blue-C"),
		"video-D":      []byte("synthetic-white-D"),
		"audio-master": []byte("synthetic-final-audio"),
	}
	plan := syntheticCertificationPlan(t, assets)
	spec := receiverOnlyTaskSpec(t, plan)
	outputRoot := t.TempDir()
	assetDir := t.TempDir()
	bindings := make(runtimeassets.Bindings, len(assets))
	for assetID, data := range assets {
		path := filepath.Join(assetDir, assetID+".asset")
		if err := os.WriteFile(path, data, 0o640); err != nil {
			t.Fatalf("write synthetic asset %s: %v", assetID, err)
		}
		bindings[assetID] = runtimeassets.Binding{
			AssetID: assetID, Path: path, SHA256: receiverOnlySHA(data), Size: int64(len(data)),
		}
	}

	runner := &receiverOnlyFFmpegRunner{}
	exec := NewRenderBatch(runner, outputRoot)
	exec.(*renderBatchExecutor).probe = func(_ context.Context, path string) (publisher.MediaProbe, error) {
		if strings.Contains(path, "audio-master") {
			return publisher.MediaProbe{HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 53}, nil
		}
		return publisher.MediaProbe{HasVideo: true, VideoTrackCount: 1, HasAudio: true, AudioTrackCount: 1, AudioCodec: "aac", AudioSampleRateHz: 48_000, AudioChannels: 2, DurationSec: 53}, nil
	}

	first, err := exec.Execute(runtimeassets.WithBindings(context.Background(), bindings), nil, spec)
	if err != nil {
		t.Fatalf("first certification execution: %v", err)
	}
	if first.Status != "succeeded" || len(first.Outputs) != 1 {
		t.Fatalf("first certification result = %+v", first)
	}
	if len(runner.requests) != 2 {
		t.Fatalf("first execution FFmpeg requests = %d, want visual + mux", len(runner.requests))
	}
	visual := strings.Join(runner.requests[0].Args, " ")
	mux := strings.Join(runner.requests[1].Args, " ")
	if runner.requests[0].Operation != ffmpegrunner.OperationCompose || runner.requests[1].Operation != ffmpegrunner.OperationEncode {
		t.Fatalf("operation order = %q then %q, want compose then encode", runner.requests[0].Operation, runner.requests[1].Operation)
	}
	placements := []struct {
		segment string
		assetID string
		input   int
		trim    string
		setpts  string
	}{
		{"A", "video-A", 1, "trim=start=33.200000:duration=5.600000", "setpts=PTS-STARTPTS+12.400000/TB"},
		{"B", "video-B", 2, "trim=start=7.100000:duration=6.500000", "setpts=PTS-STARTPTS+22.000000/TB"},
		{"C", "video-C", 3, "trim=start=91.000000:duration=4.000000", "setpts=PTS-STARTPTS+35.000000/TB"},
		{"D", "video-D", 4, "trim=start=15.500000:duration=6.000000", "setpts=PTS-STARTPTS+47.000000/TB"},
	}
	previousPathArg := -1
	for _, placement := range placements {
		path := bindings[placement.assetID].Path
		pathArg := -1
		for index, arg := range runner.requests[0].Args {
			if arg == path {
				pathArg = index
				break
			}
		}
		if pathArg < 0 || pathArg <= previousPathArg || pathArg == 0 || runner.requests[0].Args[pathArg-1] != "-i" {
			t.Errorf("segment %s asset %s is not mapped to ordered FFmpeg input %d: args=%v", placement.segment, placement.assetID, placement.input, runner.requests[0].Args)
		}
		previousPathArg = pathArg
		if !strings.Contains(visual, fmt.Sprintf("[%d:v]%s", placement.input, placement.trim)) || !strings.Contains(visual, placement.setpts) {
			t.Errorf("segment %s missing exact input/placement/source duration: %s", placement.segment, visual)
		}
	}
	for _, expected := range []string{"d=53.000000", "-t 53.000000", "-an"} {
		if !strings.Contains(visual, expected) {
			t.Errorf("visual command missing output duration/audio exclusion %q: %s", expected, visual)
		}
	}
	for _, expected := range []string{"-map 0:v:0", "-map 1:a:0", "-c:v copy", "-c:a copy", "-movflags +faststart"} {
		if !strings.Contains(mux, expected) {
			t.Errorf("mux command missing %q: %s", expected, mux)
		}
	}
	if strings.Contains(mux, "-shortest") || strings.Contains(mux, "-c:a aac") {
		t.Fatalf("mux command must copy final audio without shortening or encoding: %s", mux)
	}
	if first.Metrics["audio_mix_count"] != int64(0) || first.Metrics["audio_encode_count"] != int64(0) || first.Metrics["final_audio_copy"] != int64(1) {
		t.Fatalf("audio-copy metrics = %#v", first.Metrics)
	}
	if _, err := os.Stat(filepath.Join(outputRoot, "receiver-only-job.video-only.mp4")); !os.IsNotExist(err) {
		t.Fatalf("video-only intermediate was not cleaned up after first run: %v", err)
	}

	second, err := exec.Execute(runtimeassets.WithBindings(context.Background(), bindings), nil, spec)
	if err != nil {
		t.Fatalf("second certification execution: %v", err)
	}
	if second.Outputs[0].Hash != first.Outputs[0].Hash {
		t.Fatalf("certification output is not deterministic: first=%s second=%s", first.Outputs[0].Hash, second.Outputs[0].Hash)
	}
	if len(runner.requests) != 4 || !reflect.DeepEqual(runner.requests[:2], runner.requests[2:]) {
		t.Fatal("A/B/C/D FFmpeg commands changed between deterministic replays")
	}
	if _, err := os.Stat(filepath.Join(outputRoot, "receiver-only-job.video-only.mp4")); !os.IsNotExist(err) {
		t.Fatalf("video-only intermediate was not cleaned up after second run: %v", err)
	}
}
