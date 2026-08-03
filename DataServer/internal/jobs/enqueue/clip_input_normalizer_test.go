package enqueue

import (
	"strings"
	"testing"
)

func canonicalClipScene() map[string]interface{} {
	return map[string]interface{}{
		"scene_id": "scene-1",
		"text":     "Canonical scene",
		"clip": map[string]interface{}{
			"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 7000,
		},
		"stock": []interface{}{map[string]interface{}{
			"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000,
		}},
		"voiceover": map[string]interface{}{
			"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 12000,
		},
	}
}

func TestNormalizeClipPayloadCanonicalNarratedScene(t *testing.T) {
	entries, items, clips, tracks, mode, err := normalizeClipPayload(map[string]interface{}{
		"job_id": "canonical-job",
		"scenes": []interface{}{canonicalClipScene()},
	})
	if err != nil {
		t.Fatalf("normalizeClipPayload: %v", err)
	}
	if mode != "clip_stock" || len(entries) != 1 || len(items) != 2 || len(clips) != 1 || len(tracks) != 2 {
		t.Fatalf("mode=%q entries=%d items=%d clips=%d tracks=%d", mode, len(entries), len(items), len(clips), len(tracks))
	}
	for _, assetKey := range []string{"clip", "stock", "voiceover"} {
		if _, ok := entries[0][assetKey]; !ok {
			t.Fatalf("canonical entry missing %s", assetKey)
		}
	}
}

func TestNormalizeClipPayloadCanonicalSceneJSON(t *testing.T) {
	sceneJSON := `[{
		"clip":{"asset_id":"clip-1","url":"velox-asset://clip-1","duration_ms":3000},
		"voiceover":{"asset_id":"voice-1","url":"velox-asset://voice-1","duration_ms":4000}
	}]`
	entries, items, clips, tracks, mode, err := normalizeClipPayload(map[string]interface{}{"scenes_json": sceneJSON})
	if err != nil {
		t.Fatalf("normalizeClipPayload: %v", err)
	}
	if mode != "clip_stock" || len(entries) != 1 || len(items) != 2 || len(clips) != 1 || len(tracks) != 2 {
		t.Fatalf("mode=%q entries=%d items=%d clips=%d tracks=%d", mode, len(entries), len(items), len(clips), len(tracks))
	}
}

func TestNormalizeClipPayloadRejectsRawClips(t *testing.T) {
	_, _, _, _, _, err := normalizeClipPayload(map[string]interface{}{
		"clips": []interface{}{"https://example.test/clip.mp4"},
	})
	if err == nil || !strings.Contains(err.Error(), "legacy clips input is unsupported") {
		t.Fatalf("want raw clips rejection, got %v", err)
	}
}

func TestNormalizeClipPayloadRejectsLegacySceneAliases(t *testing.T) {
	aliases := []string{
		"clip_link", "clip_links", "drive_links", "local_path", "bindings",
		"reference_voiceover", "voiceover_link", "voiceover_path", "voiceover_paths",
		"stock_clip_paths", "stock_clip_sources", "intro_clip_paths", "start_clip_paths",
		"stock_links", "stock_clip_links", "clip_duration_seconds", "duration_seconds",
	}
	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			scene := canonicalClipScene()
			scene[alias] = "legacy-value"
			_, _, _, _, _, err := normalizeClipPayload(map[string]interface{}{"scenes": []interface{}{scene}})
			if err == nil || !strings.Contains(err.Error(), "legacy field") {
				t.Fatalf("want %s rejection, got %v", alias, err)
			}
		})
	}
}

func TestNormalizeClipPayloadRejectsMissingDurationWhenUnprobeable(t *testing.T) {
	scene := canonicalClipScene()
	scene["clip"] = map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1"}
	_, _, _, _, _, err := normalizeClipPayload(map[string]interface{}{"scenes": []interface{}{scene}})
	if err == nil || !strings.Contains(err.Error(), "duration unavailable") {
		t.Fatalf("want duration error, got %v", err)
	}
}

func TestNormalizeClipPayloadMaterializesProbedDuration(t *testing.T) {
	// The production probe cannot inspect velox-asset URLs in this unit test;
	// exercise the same materialization through the renderer helper directly.
	scene := map[string]interface{}{
		"clip":      map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1"},
		"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1"},
	}
	entries, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{
		probe: func(url string) float64 {
			if url == "velox-asset://clip-1" {
				return 2.5
			}
			return 4.0
		},
	})
	if err != nil {
		t.Fatalf("buildNarratedClipPayload: %v", err)
	}
	if got := entries[0]["clip"].(map[string]interface{})["duration_ms"]; got != int64(2500) {
		t.Fatalf("clip duration_ms = %v, want 2500", got)
	}
	if got := entries[0]["voiceover"].(map[string]interface{})["duration_ms"]; got != int64(4000) {
		t.Fatalf("voiceover duration_ms = %v, want 4000", got)
	}
}

func TestNormalizeScenesInputPreservesCanonicalAudioTracks(t *testing.T) {
	tracks := []interface{}{map[string]interface{}{
		"source_url": "velox-asset://music", "role": "background_music", "duration_seconds": 20.0,
	}}
	_, _, _, got, _, err := normalizeClipPayload(map[string]interface{}{
		"audio_tracks": tracks,
		"scenes":       []interface{}{canonicalClipScene()},
	})
	if err != nil {
		t.Fatalf("normalizeClipPayload: %v", err)
	}
	if len(got) != 3 || got[0]["role"] != "background_music" || got[1]["role"] != "voiceover" || got[2]["role"] != "scene_clip_audio" {
		t.Fatalf("unexpected tracks: %#v", got)
	}
}
