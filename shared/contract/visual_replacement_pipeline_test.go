package contract

import (
	"errors"
	"strings"
	"testing"
)

// visualReplacementManifest builds a strict render_manifest with a single
// 120s base video clip, a 5s prepared replacement video asset, a voiceover
// track and a verified final_audio — the exact shape the V2 compiler needs
// for FINAL_AUDIO_COPY.
func visualReplacementManifest(t *testing.T) map[string]any {
	t.Helper()
	return map[string]any{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]any{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []map[string]any{
			{"id": "base", "uri": "velox-asset://base", "kind": "video", "format": "mp4", "sha256": strings.Repeat("a", 64), "size_bytes": 1000, "duration_ms": 120000},
			{"id": "prepared", "uri": "velox-asset://prepared", "kind": "video", "format": "mp4", "sha256": strings.Repeat("b", 64), "size_bytes": 500, "duration_ms": 5000},
			{"id": "vo", "uri": "velox-asset://vo", "kind": "audio", "format": "aac", "sha256": strings.Repeat("c", 64), "size_bytes": 300, "duration_ms": 120000},
			{"id": "final_audio", "uri": "velox-asset://final_audio", "kind": "final_audio", "format": "aac", "sha256": strings.Repeat("d", 64), "size_bytes": 400, "duration_ms": 120000},
		},
		"tracks": []map[string]any{
			{"id": "v1", "kind": "video", "events": []map[string]any{
				{"asset_id": "base", "timeline_start_ms": 0, "duration_ms": 120000},
			}},
			{"id": "vo1", "kind": "voiceover", "events": []map[string]any{
				{"asset_id": "vo", "timeline_start_ms": 0, "duration_ms": 120000},
			}},
		},
		"output": map[string]any{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
}

func singleReplacement60To65() []VisualReplacement {
	return []VisualReplacement{{
		ReplacementID:   "vr_001",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("b", 64),
		TimelineStartUS: 60_000_000,
		TimelineEndUS:   65_000_000,
		ProfileID:       "velox-h264-1080p30-v1",
	}}
}

// TestCompileRenderPlanV2_VisualReplacement_SplitsTimeline pins the pipeline
// end-to-end: render_manifest + visual_replacements → CompiledRenderPlanV2
// with the expected BASE/PREPARED/BASE segments (section 10 of the plan).
func TestCompileRenderPlanV2_VisualReplacement_SplitsTimeline(t *testing.T) {
	plan, err := CompileRenderPlanV2FromManifestWithReplacements(visualReplacementManifest(t), singleReplacement60To65())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.VideoTracks) != 1 {
		t.Fatalf("expected 1 video track, got %d", len(plan.VideoTracks))
	}
	segs := plan.VideoTracks[0].Segments
	if len(segs) != 3 {
		t.Fatalf("expected 3 segments, got %d: %+v", len(segs), segs)
	}
	assertVideoSegmentV2(t, segs[0], "base", 0, 1800, 0, 60_000_000)             // BASE 0→60
	assertVideoSegmentV2(t, segs[1], "prepared", 1800, 150, 0, 5_000_000)        // PREPARED 60→65
	assertVideoSegmentV2(t, segs[2], "base", 1950, 1650, 65_000_000, 55_000_000) // BASE 65→120
}

// TestCompileRenderPlanV2_VisualReplacement_CopyOnlyCertification pins the
// worker certification invariants (section 17) at the plan level:
//   - packet_copy_segments == total_segments == 3 (the worker must not reject)
//   - segments contiguous with no gaps/overlaps, covering exactly the duration
//   - frames decoded/encoded/composited == 0 is guaranteed by construction:
//     every segment is a plain VideoSource with no transform/legacy flags
//   - final audio unchanged: FINAL_AUDIO_COPY with duration == plan duration
//
// The execution-level certification (the worker reporting the exact counters)
// is asserted by the native worker tests once the C++ packet-mux path lands.
func TestCompileRenderPlanV2_VisualReplacement_CopyOnlyCertification(t *testing.T) {
	plan, err := CompileRenderPlanV2FromManifestWithReplacements(visualReplacementManifest(t), singleReplacement60To65())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if plan.DurationUS != 120_000_000 {
		t.Fatalf("plan duration = %d, want 120s", plan.DurationUS)
	}
	segs := plan.VideoTracks[0].Segments
	if len(segs) != 3 {
		t.Fatalf("packet_copy_segments = %d, want 3", len(segs))
	}
	var cursor int64
	for _, seg := range segs {
		if seg.TimelineStartFrame != cursor {
			t.Fatalf("segment %q starts at frame %d, want %d (contiguous timeline)", seg.SegmentID, seg.TimelineStartFrame, cursor)
		}
		if seg.FrameCount <= 0 || seg.SourceDurationUS <= 0 {
			t.Fatalf("segment %q must carry positive frame count and source duration", seg.SegmentID)
		}
		cursor += seg.FrameCount
	}
	if cursor != 30*120 {
		t.Fatalf("total frames = %d, want %d (120s @30fps)", cursor, 30*120)
	}
	// audio invariato: final audio duration equals plan duration, muxed without a mix.
	if plan.FinalAudio.DurationUS != plan.DurationUS {
		t.Fatalf("final audio duration = %d, want plan duration %d (audio invariato)", plan.FinalAudio.DurationUS, plan.DurationUS)
	}
	if plan.FinalAudio.Mode != AudioModeFinalAudioCopy {
		t.Fatalf("final audio mode = %q, want %q", plan.FinalAudio.Mode, AudioModeFinalAudioCopy)
	}
}

// TestCompileRenderPlanV2_VisualReplacement_RejectsOverlap pins that the
// master-side resolver rejects an overlapping replacement during compile, so
// the worker never sees an ambiguous timeline.
func TestCompileRenderPlanV2_VisualReplacement_RejectsOverlap(t *testing.T) {
	reps := []VisualReplacement{
		{ReplacementID: "r1", AssetID: "prepared", SHA256: strings.Repeat("b", 64), TimelineStartUS: 60_000_000, TimelineEndUS: 65_000_000},
		{ReplacementID: "r2", AssetID: "prepared", SHA256: strings.Repeat("b", 64), TimelineStartUS: 63_000_000, TimelineEndUS: 70_000_000},
	}
	_, err := CompileRenderPlanV2FromManifestWithReplacements(visualReplacementManifest(t), reps)
	if err == nil {
		t.Fatalf("expected overlap rejection, got nil")
	}
	var re *VisualReplacementError
	if !errors.As(err, &re) || re.Code != VisualReplacementCodeOverlap {
		t.Fatalf("expected VisualReplacementError OVERLAP, got %T: %v", err, err)
	}
}

func assertVideoSegmentV2(t *testing.T, seg VideoSegmentV2, assetID string, startFrame, frameCount, sourceIn, sourceDur int64) {
	t.Helper()
	if seg.AssetID != assetID || seg.TimelineStartFrame != startFrame || seg.FrameCount != frameCount ||
		seg.SourceInUS != sourceIn || seg.SourceDurationUS != sourceDur {
		t.Fatalf("segment mismatch:\n got  %+v\n want asset=%s start=%d frames=%d sourceIn=%d sourceDur=%d",
			seg, assetID, startFrame, frameCount, sourceIn, sourceDur)
	}
}
