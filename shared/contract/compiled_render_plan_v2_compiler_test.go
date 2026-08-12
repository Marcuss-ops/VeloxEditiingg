package contract

import (
	"strings"
	"testing"
)

func TestCompileRenderPlanV2FromManifest_UsesIntegerFrameAndMicrosecondTiming(t *testing.T) {
	const videoSHA = "1111111111111111111111111111111111111111111111111111111111111111"
	const audioSHA = "2222222222222222222222222222222222222222222222222222222222222222"
	manifest := map[string]any{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]any{
			"width": 1920, "height": 1080, "fps_num": 30000, "fps_den": 1001, "pixel_format": "yuv420p",
		},
		"assets": []map[string]any{
			{"id": "video-a", "uri": "velox-asset://video-a", "kind": "video", "sha256": videoSHA, "size_bytes": 100, "duration_ms": 5000},
			{"id": "voice-a", "uri": "velox-asset://voice-a", "kind": "audio", "sha256": audioSHA, "size_bytes": 100, "duration_ms": 2500},
			{"id": "audio-final", "uri": "velox-asset://audio-final", "kind": "final_audio", "format": "audio/mp4", "sha256": audioSHA, "size_bytes": 200, "duration_ms": 2500},
		},
		"tracks": []map[string]any{
			{"id": "main", "kind": "video", "events": []map[string]any{
				{"asset_id": "video-a", "timeline_start_ms": 1000, "source_start_ms": 250, "duration_ms": 1500},
			}},
			{"id": "voice", "kind": "voiceover", "events": []map[string]any{
				{"asset_id": "voice-a", "timeline_start_ms": 0, "duration_ms": 2500},
			}},
		},
		"output": map[string]any{
			"container": "mp4", "video_codec": "h264", "audio_codec": "aac",
			"audio_sample_rate": 48000, "audio_channels": 2,
		},
	}

	plan, err := CompileRenderPlanV2FromManifest(manifest)
	if err != nil {
		t.Fatalf("CompileRenderPlanV2FromManifest: %v", err)
	}
	if err := ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("generated plan failed validation: %v", err)
	}
	if plan.DurationUS != 2_500_000 {
		t.Fatalf("duration_us = %d, want 2500000", plan.DurationUS)
	}
	if plan.FinalAudio.Mode != AudioModeFinalAudioCopy || plan.FinalAudio.AssetID != "audio-final" {
		t.Fatalf("final audio = %+v", plan.FinalAudio)
	}
	if len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) != 1 {
		t.Fatalf("video tracks = %+v", plan.VideoTracks)
	}
	segment := plan.VideoTracks[0].Segments[0]
	if segment.TimelineStartFrame != 30 {
		t.Fatalf("timeline_start_frame = %d, want nearest frame 30", segment.TimelineStartFrame)
	}
	if segment.FrameCount != 45 {
		t.Fatalf("frame_count = %d, want nearest frame 45", segment.FrameCount)
	}
	if segment.SourceInUS != 250_000 || segment.SourceDurationUS != 1_500_000 {
		t.Fatalf("source timing = %d/%d, want 250000/1500000", segment.SourceInUS, segment.SourceDurationUS)
	}
	canonical, sha, err := CompileRenderPlanV2JSON(manifest)
	if err != nil {
		t.Fatalf("CompileRenderPlanV2JSON: %v", err)
	}
	if string(canonical) == "" || len(sha) != 64 || sha != HashCompiledPlanV2(canonical) {
		t.Fatalf("canonical/hash mismatch: bytes=%d sha=%q", len(canonical), sha)
	}
	if strings.Contains(string(canonical), "duration_seconds") {
		t.Fatalf("V2 canonical plan contains float timing: %s", canonical)
	}
}

func TestCompileRenderPlanV2FromManifest_RequiresFinalAudio(t *testing.T) {
	manifest := map[string]any{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]any{"width": 640, "height": 360, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []map[string]any{
			{"id": "video", "uri": "velox-asset://video", "kind": "video", "sha256": strings.Repeat("a", 64), "size_bytes": 10, "duration_ms": 1000},
			{"id": "audio", "uri": "velox-asset://audio", "kind": "audio", "sha256": strings.Repeat("b", 64), "size_bytes": 10, "duration_ms": 1000},
		},
		"tracks": []map[string]any{
			{"id": "video", "kind": "video", "events": []map[string]any{{"asset_id": "video", "timeline_start_ms": 0, "duration_ms": 1000}}},
			{"id": "voice", "kind": "voiceover", "events": []map[string]any{{"asset_id": "audio", "timeline_start_ms": 0, "duration_ms": 1000}}},
		},
		"output": map[string]any{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
	if _, err := CompileRenderPlanV2FromManifest(manifest); err == nil || !strings.Contains(err.Error(), "final_audio") {
		t.Fatalf("missing final_audio error = %v", err)
	}
}
