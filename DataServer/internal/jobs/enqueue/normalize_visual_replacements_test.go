package enqueue

import (
	"context"
	"errors"
	"testing"

	"velox-shared/contract"
	"velox-shared/contract/deliveryplan"
)

// vrNormalizerManifest builds a strict render_manifest with a 120s base video
// clip, a 5s prepared replacement video asset, a voiceover track and a
// verified final_audio — the exact shape the strict V2 compiler needs to
// resolve visual_replacements master-side.
func vrNormalizerManifest() map[string]interface{} {
	const (
		baseSHA  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		vrSHA    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		audioSHA = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	return map[string]interface{}{
		"schema": "velox.render-manifest.v1",
		"canvas": map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "pixel_format": "yuv420p"},
		"assets": []interface{}{
			map[string]interface{}{"id": "base", "uri": "velox-asset://base", "kind": "video", "format": "mp4", "sha256": baseSHA, "size_bytes": 1000, "duration_ms": 120000},
			map[string]interface{}{"id": "prepared", "uri": "velox-asset://prepared", "kind": "video", "format": "mp4", "sha256": vrSHA, "size_bytes": 500, "duration_ms": 5000},
			map[string]interface{}{"id": "vo", "uri": "velox-asset://vo", "kind": "audio", "format": "aac", "sha256": audioSHA, "size_bytes": 300, "duration_ms": 120000},
			map[string]interface{}{"id": "final_audio", "uri": "velox-asset://final_audio", "kind": "final_audio", "format": "aac", "sha256": audioSHA, "size_bytes": 400, "duration_ms": 120000},
		},
		"tracks": []interface{}{
			map[string]interface{}{"id": "v1", "kind": "video", "events": []interface{}{
				map[string]interface{}{"asset_id": "base", "timeline_start_ms": 0, "duration_ms": 120000},
			}},
			map[string]interface{}{"id": "vo1", "kind": "voiceover", "events": []interface{}{
				map[string]interface{}{"asset_id": "vo", "timeline_start_ms": 0, "duration_ms": 120000},
			}},
		},
		"output": map[string]interface{}{"container": "mp4", "video_codec": "h264", "audio_codec": "aac", "audio_sample_rate": 48000, "audio_channels": 2},
	}
}

// TestNormalizeSceneVideoRejectsInvalidVisualReplacements pins that
// overlap / invalid-range / out-of-bounds replacements are refused during
// normalization with a machine-readable VISUAL_REPLACEMENT_* issue BEFORE any
// task/worker offer is produced, instead of a generic "render_manifest
// compile failed" classification.
func TestNormalizeSceneVideoRejectsInvalidVisualReplacements(t *testing.T) {
	cases := []struct {
		name         string
		replacements []map[string]interface{}
		wantCode     string
	}{
		{
			name: "overlap",
			replacements: []map[string]interface{}{
				{"replacement_id": "vr_a", "asset_id": "prepared", "timeline_start_us": 60_000_000, "timeline_end_us": 65_000_000, "profile_id": "velox-h264-1080p30-v1"},
				{"replacement_id": "vr_b", "asset_id": "prepared", "timeline_start_us": 63_000_000, "timeline_end_us": 70_000_000, "profile_id": "velox-h264-1080p30-v1"},
			},
			wantCode: contract.VisualReplacementCodeOverlap,
		},
		{
			name: "reversed range",
			replacements: []map[string]interface{}{
				{"replacement_id": "vr_a", "asset_id": "prepared", "timeline_start_us": 65_000_000, "timeline_end_us": 60_000_000, "profile_id": "velox-h264-1080p30-v1"},
			},
			wantCode: contract.VisualReplacementCodeInvalidRange,
		},
		{
			name: "zero duration",
			replacements: []map[string]interface{}{
				{"replacement_id": "vr_a", "asset_id": "prepared", "timeline_start_us": 60_000_000, "timeline_end_us": 60_000_000, "profile_id": "velox-h264-1080p30-v1"},
			},
			wantCode: contract.VisualReplacementCodeInvalidRange,
		},
		{
			name: "out of bounds",
			replacements: []map[string]interface{}{
				{"replacement_id": "vr_a", "asset_id": "prepared", "timeline_start_us": 119_000_000, "timeline_end_us": 125_000_000, "profile_id": "velox-h264-1080p30-v1"},
			},
			wantCode: contract.VisualReplacementCodeOutOfBounds,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := make([]interface{}, len(tc.replacements))
			for i, r := range tc.replacements {
				items[i] = r
			}
			payload := map[string]interface{}{
				"video_name":          "Visual replacement reject",
				"script_text":         "Visual replacement reject script",
				"render_manifest":     vrNormalizerManifest(),
				"visual_replacements": items,
			}
			_, err := normalizeSceneVideoPayloadContext(context.Background(), payload)
			if err == nil {
				t.Fatalf("want %s rejection, got nil", tc.wantCode)
			}
			var verr *deliveryplan.ValidationError
			if !errors.As(err, &verr) || verr == nil {
				t.Fatalf("error = %T (%v), want *deliveryplan.ValidationError", err, err)
			}
			if verr.Field() != "visual_replacements" {
				t.Fatalf("field = %q, want visual_replacements", verr.Field())
			}
			if verr.Issue != tc.wantCode {
				t.Fatalf("issue = %q, want %q (full: %v)", verr.Issue, tc.wantCode, verr.Error())
			}
		})
	}
}

// TestNormalizeSceneVideoKeepsNonReplacementCompileErrorClassified pins that
// a manifest compile failure that is NOT a visual-replacement violation keeps
// the generic render_manifest classification (no false VISUAL_REPLACEMENT_*
// issue is invented).
func TestNormalizeSceneVideoKeepsNonReplacementCompileErrorClassified(t *testing.T) {
	manifest := vrNormalizerManifest()
	// A non-48kHz sample rate passes the legacy manifest validator (>0) but
	// fails the strict V2 compiler ("final_audio requires 48000 Hz stereo") —
	// a manifest-level failure, not a replacement-level one.
	manifest["output"].(map[string]interface{})["audio_sample_rate"] = 44100

	payload := map[string]interface{}{
		"video_name":      "Non-replacement compile failure",
		"script_text":     "Non-replacement compile failure script",
		"render_manifest": manifest,
	}
	_, err := normalizeSceneVideoPayloadContext(context.Background(), payload)
	if err == nil {
		t.Fatal("want compile failure, got nil")
	}
	var verr *deliveryplan.ValidationError
	if !errors.As(err, &verr) || verr == nil {
		t.Fatalf("error = %T (%v), want *deliveryplan.ValidationError", err, err)
	}
	if verr.Field() != "render_manifest" {
		t.Fatalf("field = %q, want render_manifest (replacement codes must not leak)", verr.Field())
	}
	if verr.Issue == contract.VisualReplacementCodeOverlap ||
		verr.Issue == contract.VisualReplacementCodeInvalidRange ||
		verr.Issue == contract.VisualReplacementCodeOutOfBounds {
		t.Fatalf("issue = %q, want a non-replacement manifest issue", verr.Issue)
	}
}
