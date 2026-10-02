package grpcserver

import (
	"testing"
)

// TestDiagnoseRenderInput_KindClipCounters pins the two counters behind
// job_e2adca259c034c1e / job_551b66f95cf94670: kind="clip" scenes without
// a clip asset must be counted (they compile mute), and clip scenes are
// split into selected (runtime_audio.clip_audio_scene_ids → mixed with
// narration) vs dropped (final mix present, unselected → video-only).
func TestDiagnoseRenderInput_KindClipCounters(t *testing.T) {
	payload := map[string]interface{}{
		"copy_only": true,
		"runtime_audio": map[string]interface{}{
			"voiceover_asset_id":   "narr-1",
			"clip_audio_scene_ids": []interface{}{"scene-0000"},
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id": "scene-0062", "kind": "clip",
				"duration_seconds": float64(6.36),
				"stock":            map[string]interface{}{"url": "velox-drive://testimony"},
			},
			map[string]interface{}{
				"scene_id": "scene-0002", "kind": "stock",
				"duration_seconds": float64(5),
				"stock":            map[string]interface{}{"url": "velox-drive://bg"},
			},
			map[string]interface{}{
				"scene_id": "scene-0000", "kind": "clip",
				"duration_seconds": float64(5),
				"clip":             map[string]interface{}{"url": "velox-drive://intro"},
			},
			map[string]interface{}{
				"scene_id": "scene-0001", "kind": "clip",
				"duration_seconds": float64(5),
				"clip":             map[string]interface{}{"url": "velox-drive://unselected"},
			},
		},
	}
	got := diagnoseRenderInput(payload)
	if got.Scenes != 4 {
		t.Fatalf("scenes = %d, want 4", got.Scenes)
	}
	if got.KindClipWithoutClip != 1 {
		t.Fatalf("kind_clip_without_clip = %d, want 1 (scene-0062)", got.KindClipWithoutClip)
	}
	if got.ClipAudioSelected != 1 {
		t.Fatalf("clip_audio_selected = %d, want 1 (scene-0000)", got.ClipAudioSelected)
	}
	if got.ClipAudioDropped != 1 {
		t.Fatalf("clip_audio_dropped = %d, want 1 (scene-0001)", got.ClipAudioDropped)
	}
	if !got.RuntimeAudioPresent {
		t.Fatal("runtime_audio_present = false, want true")
	}
}

// TestDiagnoseRenderInput_NoMixNoCounters ensures no clip-audio counters
// move when no final mix owns the timeline.
func TestDiagnoseRenderInput_NoMixNoOmission(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id": "s1", "kind": "clip",
				"duration_seconds": float64(5),
				"clip":             map[string]interface{}{"url": "velox-drive://c"},
			},
		},
	}
	got := diagnoseRenderInput(payload)
	if got.ClipAudioSelected != 0 || got.ClipAudioDropped != 0 {
		t.Fatalf("selected=%d dropped=%d, want 0/0 without final mix", got.ClipAudioSelected, got.ClipAudioDropped)
	}
	if got.KindClipWithoutClip != 0 {
		t.Fatalf("kind_clip_without_clip = %d, want 0", got.KindClipWithoutClip)
	}
}
