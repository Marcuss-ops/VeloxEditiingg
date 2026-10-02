package grpcserver

import (
	"testing"
)

// TestDiagnoseRenderInput_KindClipCounters pins the two counters behind
// job_e2adca259c034c1e: kind="clip" scenes without a clip asset must be
// counted (they compile mute), and clip scenes under a final runtime mix
// must be counted as audio-omitted (the worker drops scene clip audio by
// design when the mix owns the timeline).
func TestDiagnoseRenderInput_KindClipCounters(t *testing.T) {
	payload := map[string]interface{}{
		"copy_only": true,
		"runtime_audio": map[string]interface{}{
			"voiceover_asset_id": "narr-1",
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
		},
	}
	got := diagnoseRenderInput(payload)
	if got.Scenes != 3 {
		t.Fatalf("scenes = %d, want 3", got.Scenes)
	}
	if got.KindClipWithoutClip != 1 {
		t.Fatalf("kind_clip_without_clip = %d, want 1 (scene-0062)", got.KindClipWithoutClip)
	}
	if got.ClipAudioOmitted != 1 {
		t.Fatalf("clip_audio_omitted = %d, want 1 (scene-0000 under final mix)", got.ClipAudioOmitted)
	}
	if !got.RuntimeAudioPresent {
		t.Fatal("runtime_audio_present = false, want true")
	}
}

// TestDiagnoseRenderInput_NoMixNoOmission ensures clip audio is not
// reported omitted when no final mix owns the timeline.
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
	if got.ClipAudioOmitted != 0 {
		t.Fatalf("clip_audio_omitted = %d, want 0 without final mix", got.ClipAudioOmitted)
	}
	if got.KindClipWithoutClip != 0 {
		t.Fatalf("kind_clip_without_clip = %d, want 0", got.KindClipWithoutClip)
	}
}
