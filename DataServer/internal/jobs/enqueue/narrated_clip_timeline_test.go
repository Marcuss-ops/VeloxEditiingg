package enqueue

import (
	"strings"
	"testing"
)

func canonicalNarratedScene(clipURL, voiceURL string, clipMS, voiceMS int64) map[string]interface{} {
	return map[string]interface{}{
		"clip": map[string]interface{}{
			"asset_id":    "clip-asset",
			"url":         clipURL,
			"duration_ms": clipMS,
		},
		"stock": []interface{}{map[string]interface{}{
			"asset_id":    "stock-asset",
			"url":         "velox-asset://stock-asset",
			"duration_ms": 5000,
		}},
		"voiceover": map[string]interface{}{
			"asset_id":    "voice-asset",
			"url":         voiceURL,
			"duration_ms": voiceMS,
		},
	}
}

func TestSupportsNarratedClipScenesRequiresCanonicalVoiceover(t *testing.T) {
	if !supportsNarratedClipScenes([]map[string]interface{}{canonicalNarratedScene("velox-asset://clip-asset", "velox-asset://voice-asset", 7000, 12000)}) {
		t.Fatal("canonical nested voiceover should select narrated renderer")
	}
	for _, scene := range []map[string]interface{}{
		{"voiceover_path": "legacy.mp3"},
		{"bindings": map[string]interface{}{"voiceover": map[string]interface{}{"url": "legacy.mp3"}}},
	} {
		if supportsNarratedClipScenes([]map[string]interface{}{scene}) {
			t.Fatalf("legacy voiceover shape %#v selected narrated renderer", scene)
		}
	}
}

func TestBuildNarratedClipPayloadPreservesCanonicalAssetsAndDurations(t *testing.T) {
	scene := canonicalNarratedScene("velox-asset://clip-asset", "velox-asset://voice-asset", 7000, 12000)
	entries, items, clips, tracks, mode, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{})
	if err != nil {
		t.Fatalf("buildNarratedClipPayload: %v", err)
	}
	if mode != "clip_stock" || len(entries) != 1 || len(items) != 2 || len(clips) != 1 || len(tracks) != 2 {
		t.Fatalf("mode=%q entries=%d items=%d clips=%d tracks=%d", mode, len(entries), len(items), len(clips), len(tracks))
	}
	if got := entries[0]["clip"].(map[string]interface{})["asset_id"]; got != "clip-asset" {
		t.Fatalf("clip asset_id = %v", got)
	}
	if got := entries[0]["voiceover"].(map[string]interface{})["url"]; got != "velox-asset://voice-asset" {
		t.Fatalf("voiceover url = %v", got)
	}
	clipAsset := entries[0]["clip"].(map[string]interface{})
	voiceAsset := entries[0]["voiceover"].(map[string]interface{})
	if got := clipAsset["duration_ms"]; got != int64(7000) {
		t.Fatalf("clip duration_ms = %v, want 7000", got)
	}
	if got := voiceAsset["duration_ms"]; got != int64(12000) {
		t.Fatalf("voiceover duration_ms = %v, want 12000", got)
	}
	if got := items[0]["duration"]; got != 12.0 {
		t.Fatalf("stock duration = %v, want 12", got)
	}
	if got := items[1]["duration"]; got != 7.0 {
		t.Fatalf("clip duration = %v, want 7", got)
	}
	if got := tracks[1]["start_time_offset"]; got != 12.0 {
		t.Fatalf("clip audio offset = %v, want 12", got)
	}
}

func TestBuildNarratedClipPayloadAddsRandomTransitionSoundEffects(t *testing.T) {
	scenes := []map[string]interface{}{
		canonicalNarratedScene("velox-asset://clip-asset", "velox-asset://voice-asset", 7_000, 12_000),
		canonicalNarratedScene("velox-asset://clip-asset", "velox-asset://voice-asset", 5_000, 8_000),
	}
	_, _, _, tracks, _, err := buildNarratedClipPayload(scenes, narratedClipOptions{
		randomSeed:              "boxers-test",
		transitionSoundEffects:  []string{"https://effects/a.mp3", "https://effects/b.mp3"},
		transitionSoundEffectDB: -20,
	})
	if err != nil {
		t.Fatalf("buildNarratedClipPayload: %v", err)
	}
	if len(tracks) != 7 { // 2 voiceovers + 2 clip audio + 3 transitions
		t.Fatalf("tracks = %d, want 7", len(tracks))
	}
	wantOffsets := []float64{12, 19, 27}
	sfxIndex := 0
	for i, track := range tracks {
		if track["role"] != "sfx" {
			continue
		}
		want := wantOffsets[sfxIndex]
		if got := track["start_time_offset"]; got != want {
			t.Fatalf("track[%d] offset = %v, want %v", i, got, want)
		}
		if got := track["volume"]; got != 0.1 {
			t.Fatalf("track[%d] volume = %v, want 0.1 (-20 dB)", i, got)
		}
		sfxIndex++
	}
	if sfxIndex != len(wantOffsets) {
		t.Fatalf("sfx count = %d, want %d", sfxIndex, len(wantOffsets))
	}
}

func TestBuildNarratedClipPayloadProbesMissingCanonicalDuration(t *testing.T) {
	scene := map[string]interface{}{
		"clip":      map[string]interface{}{"asset_id": "clip", "url": "velox-asset://clip"},
		"voiceover": map[string]interface{}{"asset_id": "voice", "url": "velox-asset://voice"},
	}
	calls := 0
	_, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{probe: func(url string) float64 {
		calls++
		if url == "velox-asset://voice" {
			return 12
		}
		return 7
	}})
	if err != nil {
		t.Fatalf("probeable canonical assets should succeed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("probe calls = %d, want 2", calls)
	}
}

func TestBuildNarratedClipPayloadRejectsUnprobeableMissingDuration(t *testing.T) {
	scene := map[string]interface{}{
		"clip":      map[string]interface{}{"asset_id": "clip", "url": "velox-asset://clip"},
		"voiceover": map[string]interface{}{"asset_id": "voice", "url": "velox-asset://voice"},
	}
	_, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{probe: func(string) float64 { return 0 }})
	if err == nil || !strings.Contains(err.Error(), "duration unavailable") {
		t.Fatalf("want canonical duration error, got %v", err)
	}
}

func TestBuildNarratedClipPayloadRejectsMissingCanonicalAssetURL(t *testing.T) {
	_, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{{
		"clip":      map[string]interface{}{"asset_id": "clip"},
		"voiceover": map[string]interface{}{"asset_id": "voice"},
	}}, narratedClipOptions{})
	if err == nil || !strings.Contains(err.Error(), "canonical clip asset must include asset_id and url") {
		t.Fatalf("want strict canonical asset error, got %v", err)
	}
}

func TestBuildNarratedClipPayloadRejectsIncompleteCanonicalAsset(t *testing.T) {
	cases := []map[string]interface{}{
		{"clip": map[string]interface{}{"url": "velox-asset://clip", "duration_ms": 1000}},
		{"clip": map[string]interface{}{"asset_id": "clip", "duration_ms": 1000}},
		{"clip": map[string]interface{}{"asset_id": "clip", "url": "velox-asset://clip"}},
	}
	for i, scene := range cases {
		if _, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{probe: func(string) float64 { return 0 }}); err == nil {
			t.Errorf("case %d: incomplete canonical asset unexpectedly succeeded", i)
		}
	}
}

func TestBuildNarratedClipPayloadRejectsMalformedVoiceoverAndStock(t *testing.T) {
	cases := []map[string]interface{}{
		{"clip": map[string]interface{}{"asset_id": "clip", "url": "velox-asset://clip", "duration_ms": 1000}, "voiceover": map[string]interface{}{"asset_id": "voice"}},
		{"clip": map[string]interface{}{"asset_id": "clip", "url": "velox-asset://clip", "duration_ms": 1000}, "voiceover": map[string]interface{}{"asset_id": "voice", "url": "velox-asset://voice", "duration_ms": 1000}, "stock": []interface{}{map[string]interface{}{"asset_id": "stock"}}},
	}
	for i, scene := range cases {
		if _, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{}); err == nil {
			t.Errorf("case %d: malformed canonical asset unexpectedly succeeded", i)
		}
	}
}

func TestBuildNarratedClipPayloadDoesNotReadLegacyAliases(t *testing.T) {
	scene := map[string]interface{}{
		"clip_link":           "legacy-clip.mp4",
		"voiceover_path":      "legacy-voice.mp3",
		"reference_voiceover": "legacy-reference.mp3",
		"bindings": map[string]interface{}{
			"clip": map[string]interface{}{"drive_link": "legacy-binding.mp4"},
		},
		"stock_links": []interface{}{"legacy-stock.mp4"},
	}
	if _, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{scene}, narratedClipOptions{}); err == nil {
		t.Fatal("legacy renderer aliases must not produce a successful narrated timeline")
	}
}
