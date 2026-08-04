package enqueue

import (
	"fmt"
	"testing"
)

const mikeTysonCanonicalSharedStockURL = "velox-asset://mike-tyson/shared-stock-01"

func mikeTysonCanonicalScenes() []interface{} {
	assignments := []struct {
		id          string
		clipSeconds int
		voiceMS     int
	}{
		{"mike-tyson-01", 3, 12000},
		{"mike-tyson-02", 2, 11000},
		{"mike-tyson-03", 4, 13000},
		{"mike-tyson-04", 3, 10000},
		{"mike-tyson-05", 2, 14000},
		{"mike-tyson-06", 4, 9000},
		{"mike-tyson-07", 3, 15000},
		{"mike-tyson-08", 2, 8000},
		{"mike-tyson-09", 3, 16000},
	}

	scenes := make([]interface{}, 0, len(assignments))
	for index, assignment := range assignments {
		scenes = append(scenes, map[string]interface{}{
			"scene_id": assignment.id,
			"index":    index,
			"kind":     "clip",
			"clip": map[string]interface{}{
				"asset_id":    fmt.Sprintf("mike-tyson/clip-%02d", index+1),
				"url":         fmt.Sprintf("velox-asset://mike-tyson/clip-%02d", index+1),
				"duration_ms": assignment.clipSeconds * 1000,
			},
			"stock": []interface{}{map[string]interface{}{
				"asset_id":    mikeTysonCanonicalSharedStockURL[len("velox-asset://"):],
				"url":         mikeTysonCanonicalSharedStockURL,
				"duration_ms": 7000,
			}},
			"voiceover": map[string]interface{}{
				"asset_id":    fmt.Sprintf("mike-tyson/voiceover-%02d", index+1),
				"url":         fmt.Sprintf("velox-asset://mike-tyson/voiceover-%02d", index+1),
				"duration_ms": assignment.voiceMS,
			},
		})
	}
	return scenes
}

func mikeTysonCanonicalAudioTracks() []interface{} {
	// The canonical scene.voiceover assets generate nine voiceover tracks;
	// this explicit intro track makes the complete workload's ten voiceovers.
	return []interface{}{
		map[string]interface{}{
			"source_url":        "velox-asset://mike-tyson/voiceover-intro",
			"role":              "voiceover",
			"volume":            1.0,
			"start_time_offset": 0.0,
			"duration_seconds":  10.0,
		},
		map[string]interface{}{
			"source_url": "velox-asset://mike-tyson/background-music",
			"role":       "background_music",
			"volume":     0.12,
			"loop":       true,
		},
	}
}

func TestNormalizeClipPayload_MikeTysonFullCanonicalWorkload(t *testing.T) {
	raw := map[string]interface{}{
		"job_id":       "mike-tyson-full-001",
		"video_name":   "Mike Tyson — complete editorial workload",
		"scenes":       mikeTysonCanonicalScenes(),
		"audio_tracks": mikeTysonCanonicalAudioTracks(),
	}

	entries, items, clips, tracks, mode, err := normalizeClipPayload(raw)
	if err != nil {
		t.Fatalf("normalizeClipPayload(full Mike Tyson payload): %v", err)
	}
	if mode != "clip_stock" {
		t.Fatalf("mode = %q, want clip_stock", mode)
	}
	if len(entries) != 9 || len(clips) != 9 {
		t.Fatalf("entries=%d clips=%d, want 9 each", len(entries), len(clips))
	}
	if len(items) != 18 {
		t.Fatalf("items = %d, want 18 (voiceover bed + final clip for each assignment)", len(items))
	}

	for index := 0; index < 9; index++ {
		bed := items[index*2]
		clip := items[index*2+1]
		if bed["role"] != "voiceover_bed" || bed["url"] != mikeTysonCanonicalSharedStockURL {
			t.Fatalf("items[%d] = %#v, want shared stock voiceover_bed", index*2, bed)
		}
		if clip["role"] != "scene_clip" {
			t.Fatalf("items[%d] role = %v, want scene_clip", index*2+1, clip["role"])
		}
	}

	voiceovers := 0
	music := 0
	clipAudio := 0
	voiceoverSources := make(map[string]int)
	for _, track := range tracks {
		switch track["role"] {
		case "voiceover":
			voiceovers++
			source, _ := track["source_url"].(string)
			voiceoverSources[source]++
			if source != "velox-asset://mike-tyson/voiceover-intro" {
				found := false
				for index := 1; index <= 9; index++ {
					if source == fmt.Sprintf("velox-asset://mike-tyson/voiceover-%02d", index) {
						found = true
						if got := track["duration_seconds"]; got != []float64{0, 12, 11, 13, 10, 14, 9, 15, 8, 16}[index] {
							t.Errorf("voiceover %d duration = %v, want %v", index, got, []float64{0, 12, 11, 13, 10, 14, 9, 15, 8, 16}[index])
						}
					}
				}
				if !found {
					t.Errorf("unexpected voiceover source %q", source)
				}
			}
		case "background_music":
			music++
			if got := track["source_url"]; got != "velox-asset://mike-tyson/background-music" {
				t.Errorf("music source = %v, want canonical Mike Tyson music", got)
			}
			if got := track["loop"]; got != true {
				t.Errorf("music loop = %v, want true", got)
			}
		case "scene_clip_audio":
			clipAudio++
		}
	}
	if voiceovers != 10 || music != 1 || clipAudio != 9 {
		t.Fatalf("audio roles voiceovers=%d music=%d clip_audio=%d, want 10/1/9", voiceovers, music, clipAudio)
	}
	if voiceoverSources["velox-asset://mike-tyson/voiceover-intro"] != 1 {
		t.Fatalf("intro voiceover occurrences = %d, want 1", voiceoverSources["velox-asset://mike-tyson/voiceover-intro"])
	}
	for index := 1; index <= 9; index++ {
		source := fmt.Sprintf("velox-asset://mike-tyson/voiceover-%02d", index)
		if voiceoverSources[source] != 1 {
			t.Fatalf("voiceover %s occurrences = %d, want 1", source, voiceoverSources[source])
		}
	}

	finalDuration := 0.0
	for _, item := range items {
		duration, ok := item["duration"].(float64)
		if !ok {
			t.Fatalf("timeline item duration has type %T, want float64: %#v", item["duration"], item)
		}
		finalDuration += duration
	}
	if finalDuration != 134.0 {
		t.Fatalf("normalized visual timeline duration = %v, want 134", finalDuration)
	}
}
