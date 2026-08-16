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

func TestRenderBatchReceiverCertification_RejectsTrimmedAndGappedSources(t *testing.T) {
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
	if first.Status != "failed" || first.ErrorCode != "COPY_ONLY_VIDEO_INCOMPATIBLE" {
		t.Fatalf("incompatible certification result = %+v", first)
	}
	if len(runner.requests) != 0 {
		t.Fatalf("incompatible plan started FFmpeg: %d requests", len(runner.requests))
	}
}
