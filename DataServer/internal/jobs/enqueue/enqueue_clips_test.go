package enqueue

import (
	"strings"
	"testing"
)

func TestBuildClipPayloadForMasterDoesNotReemitVoiceoverPaths(t *testing.T) {
	result, err := BuildClipPayloadForMaster(map[string]interface{}{
		"video_name": "Canonical narrated clip",
		"scenes": []interface{}{
			map[string]interface{}{
				"clip":      map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 2000},
				"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 3000},
			},
		},
	}, t.TempDir(), t.TempDir(), "")
	if err != nil {
		t.Fatalf("BuildClipPayloadForMaster: %v", err)
	}
	if _, present := result["voiceover_paths"]; present {
		t.Fatalf("retired top-level voiceover_paths must not be emitted: %#v", result["voiceover_paths"])
	}
	if _, present := result["audio_tracks"]; !present {
		t.Fatal("canonical narrated payload must retain generated audio_tracks")
	}
}

func TestBuildNarratedClipPayloadUsesCanonicalDurations(t *testing.T) {
	scenes := []map[string]interface{}{{
		"clip":      map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 2500},
		"stock":     []interface{}{map[string]interface{}{"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000}},
		"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 7250},
	}}
	entries, items, clips, tracks, mode, err := buildNarratedClipPayload(scenes, narratedClipOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if mode != "clip_stock" || len(entries) != 1 || len(items) != 2 || len(clips) != 1 || len(tracks) != 2 {
		t.Fatalf("mode=%q entries=%d items=%d clips=%d tracks=%d", mode, len(entries), len(items), len(clips), len(tracks))
	}
	clipAsset := entries[0]["clip"].(map[string]interface{})
	voiceAsset := entries[0]["voiceover"].(map[string]interface{})
	if got := clipAsset["duration_ms"]; got != int64(2500) {
		t.Fatalf("clip duration_ms=%v want 2500", got)
	}
	if got := voiceAsset["duration_ms"]; got != int64(7250) {
		t.Fatalf("voiceover duration_ms=%v want 7250", got)
	}
	if got := items[0]["duration"]; got != 7.25 {
		t.Fatalf("stock duration=%v want 7.25", got)
	}
	if got := items[1]["duration"]; got != 2.5 {
		t.Fatalf("clip duration=%v want 2.5", got)
	}
	if got := tracks[1]["start_time_offset"]; got != 7.25 {
		t.Fatalf("clip offset=%v want 7.25", got)
	}
}

func TestBuildNarratedClipPayloadProbesCanonicalAssetsWithoutDuration(t *testing.T) {
	scenes := []map[string]interface{}{{
		"clip":      map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1"},
		"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1"},
	}}
	calls := 0
	_, _, _, _, _, err := buildNarratedClipPayload(scenes, narratedClipOptions{probe: func(url string) float64 {
		calls++
		if url == "voice.mp3" {
			return 7.25
		}
		return 2.5
	}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("probe calls=%d want 2", calls)
	}
}

func TestBuildNarratedClipPayloadRejectsMissingCanonicalURL(t *testing.T) {
	_, _, _, _, _, err := buildNarratedClipPayload([]map[string]interface{}{{
		"clip":      map[string]interface{}{"asset_id": "clip-1"},
		"voiceover": map[string]interface{}{"asset_id": "voice-1"},
	}}, narratedClipOptions{})
	if err == nil || !strings.Contains(err.Error(), "canonical clip asset must include asset_id and url") {
		t.Fatalf("want strict canonical asset error, got %v", err)
	}
}

func TestBuildNarratedClipPayloadKeepsLongClipInOffsetClock(t *testing.T) {
	scenes := []map[string]interface{}{
		{
			"clip":      map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 30000},
			"stock":     []interface{}{map[string]interface{}{"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000}},
			"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 2000},
		},
		{
			"clip":      map[string]interface{}{"asset_id": "clip-2", "url": "velox-asset://clip-2", "duration_ms": 2000},
			"stock":     []interface{}{map[string]interface{}{"asset_id": "stock-2", "url": "velox-asset://stock-2", "duration_ms": 5000}},
			"voiceover": map[string]interface{}{"asset_id": "voice-2", "url": "velox-asset://voice-2", "duration_ms": 5000},
		},
	}
	_, _, _, tracks, _, err := buildNarratedClipPayload(scenes, narratedClipOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := tracks[2]["start_time_offset"]; got != 32.0 {
		t.Fatalf("second voiceover offset=%v want 32", got)
	}
	if got := tracks[3]["start_time_offset"]; got != 37.0 {
		t.Fatalf("second clip offset=%v want 37", got)
	}
}

func TestBuildNarratedClipPayloadVoiceoverFreeSceneHasNoBed(t *testing.T) {
	scenes := []map[string]interface{}{{
		"clip": map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 7000},
	}}
	entries, items, clips, tracks, _, err := buildNarratedClipPayload(scenes, narratedClipOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || len(items) != 1 || len(clips) != 1 || len(tracks) != 1 {
		t.Fatalf("entries=%d items=%d clips=%d tracks=%d", len(entries), len(items), len(clips), len(tracks))
	}
	if items[0]["role"] != "scene_clip" || tracks[0]["role"] != "scene_clip_audio" {
		t.Fatalf("unexpected output items=%#v tracks=%#v", items, tracks)
	}
}
