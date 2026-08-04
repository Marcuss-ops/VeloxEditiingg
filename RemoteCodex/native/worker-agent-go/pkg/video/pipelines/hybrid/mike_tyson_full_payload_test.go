package hybrid

import (
	"context"
	"testing"
)

const (
	mikeTysonSharedStockURL       = "velox-asset://mike-tyson/shared-stock-01"
	mikeTysonExpectedFinalSeconds = 134.0
)

// mikeTysonFullPayload is the deterministic fixture for the complete Mike
// Tyson editorial workload. The real intake produces the same shape after
// canonical asset resolution: nine visual assignments, one shared stock pool
// reused by every assignment, ten voiceover tracks, and one music track.
func mikeTysonFullPayload() map[string]interface{} {
	assignments := []struct {
		id          string
		clipURL     string
		voiceover   string
		voiceLength float64
		clipLength  float64
	}{
		{"mike-tyson-01", "velox-asset://mike-tyson/clip-01", "velox-asset://mike-tyson/voiceover-01", 12, 3},
		{"mike-tyson-02", "velox-asset://mike-tyson/clip-02", "velox-asset://mike-tyson/voiceover-02", 11, 2},
		{"mike-tyson-03", "velox-asset://mike-tyson/clip-03", "velox-asset://mike-tyson/voiceover-03", 13, 4},
		{"mike-tyson-04", "velox-asset://mike-tyson/clip-04", "velox-asset://mike-tyson/voiceover-04", 10, 3},
		{"mike-tyson-05", "velox-asset://mike-tyson/clip-05", "velox-asset://mike-tyson/voiceover-05", 14, 2},
		{"mike-tyson-06", "velox-asset://mike-tyson/clip-06", "velox-asset://mike-tyson/voiceover-06", 9, 4},
		{"mike-tyson-07", "velox-asset://mike-tyson/clip-07", "velox-asset://mike-tyson/voiceover-07", 15, 3},
		{"mike-tyson-08", "velox-asset://mike-tyson/clip-08", "velox-asset://mike-tyson/voiceover-08", 8, 2},
		{"mike-tyson-09", "velox-asset://mike-tyson/clip-09", "velox-asset://mike-tyson/voiceover-09", 16, 3},
	}

	items := make([]interface{}, 0, len(assignments)*2)
	scenes := make([]interface{}, 0, len(assignments))
	for index, assignment := range assignments {
		items = append(items,
			map[string]interface{}{
				"type":                       "video",
				"url":                        mikeTysonSharedStockURL,
				"duration":                   assignment.voiceLength,
				"role":                       "voiceover_bed",
				"scene":                      index,
				"voiceover_duration_seconds": assignment.voiceLength,
				"fit":                        "contain",
			},
			map[string]interface{}{
				"type":                        "video",
				"url":                         assignment.clipURL,
				"duration":                    assignment.clipLength,
				"role":                        "scene_clip",
				"scene":                       index,
				"final_clip_duration_seconds": assignment.clipLength,
				"fit":                         "contain",
			},
		)
		scenes = append(scenes, map[string]interface{}{
			"scene_id": assignment.id,
			"index":    index,
			"kind":     "clip",
			"clip": map[string]interface{}{
				"asset_id":    assignment.id + "-clip",
				"url":         assignment.clipURL,
				"duration_ms": assignment.clipLength * 1000,
			},
			"stock": []interface{}{map[string]interface{}{
				"asset_id": mikeTysonSharedStockURL[len("velox-asset://"):],
				"url":      mikeTysonSharedStockURL,
			}},
			"voiceover": map[string]interface{}{
				"asset_id":    assignment.id + "-voiceover",
				"url":         assignment.voiceover,
				"duration_ms": assignment.voiceLength * 1000,
			},
		})
	}

	voiceoverTracks := make([]interface{}, 0, 10)
	voiceoverTracks = append(voiceoverTracks, map[string]interface{}{
		"source_url":        "velox-asset://mike-tyson/voiceover-intro",
		"role":              "voiceover",
		"volume":            1.0,
		"start_time_offset": 0.0,
		"duration_seconds":  10.0,
	})
	for index, assignment := range assignments {
		voiceoverTracks = append(voiceoverTracks, map[string]interface{}{
			"source_url":        assignment.voiceover,
			"role":              "voiceover",
			"volume":            1.0,
			"start_time_offset": float64(index) * 10.0,
			"duration_seconds":  assignment.voiceLength,
		})
	}

	return map[string]interface{}{
		"job_id":      "mike-tyson-full-001",
		"pipeline_id": "hybrid.v1",
		"video_name":  "Mike Tyson — complete editorial workload",
		"fit":         "contain",
		"scenes":      scenes,
		"items":       items,
		"audio_tracks": append(voiceoverTracks, map[string]interface{}{
			"source_url": "velox-asset://mike-tyson/background-music",
			"role":       "background_music",
			"volume":     0.12,
			"loop":       true,
			// Deliberately omitted: the engine must bound looped music
			// to the final rendered video duration.
		}),
	}
}

func TestCompile_MikeTysonFullPayload_RenderPlanAndFinalDuration(t *testing.T) {
	input := mikeTysonFullPayload()

	scenes, ok := input["scenes"].([]interface{})
	if !ok {
		t.Fatalf("payload visual assignments type = %T, want []interface{}", input["scenes"])
	}
	if len(scenes) != 9 {
		t.Fatalf("payload visual assignments = %d, want 9", len(scenes))
	}
	items, ok := input["items"].([]interface{})
	if !ok || len(items) != 18 {
		t.Fatalf("payload visual timeline items = %T/%d, want 18 (bed + clip per assignment)", input["items"], len(items))
	}
	audioTracks, ok := input["audio_tracks"].([]interface{})
	if !ok || len(audioTracks) != 11 {
		t.Fatalf("payload audio tracks = %T/%d, want 11 (10 voiceovers + music)", input["audio_tracks"], len(audioTracks))
	}

	sharedStockUses := 0
	for index := 0; index < len(items); index += 2 {
		bed := items[index].(map[string]interface{})
		clip := items[index+1].(map[string]interface{})
		if bed["role"] != "voiceover_bed" || bed["url"] != mikeTysonSharedStockURL {
			t.Fatalf("assignment %d bed = %#v, want shared stock voiceover_bed", index/2, bed)
		}
		if clip["role"] != "scene_clip" {
			t.Fatalf("assignment %d clip role = %v, want scene_clip", index/2, clip["role"])
		}
		sharedStockUses++
	}
	if sharedStockUses != 9 {
		t.Fatalf("shared stock uses = %d, want 9", sharedStockUses)
	}

	plan, err := Compile(context.Background(), "mike-tyson-full-001", input, "/tmp/mike-tyson-full.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(full Mike Tyson payload): %v", err)
	}
	if got := len(plan.Timeline); got != 18 {
		t.Fatalf("RenderPlan.Timeline len = %d, want 18", got)
	}
	if got := len(plan.AudioTracks); got != 11 {
		t.Fatalf("RenderPlan.AudioTracks len = %d, want 11", got)
	}

	// Keep expected URLs explicit rather than deriving them from the plan;
	// this catches dropped, duplicated, or reordered assignments.
	wantDurations := []float64{12, 3, 11, 2, 13, 4, 10, 3, 14, 2, 9, 4, 15, 3, 8, 2, 16, 3}
	wantURLs := []string{
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-01",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-02",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-03",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-04",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-05",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-06",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-07",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-08",
		mikeTysonSharedStockURL, "velox-asset://mike-tyson/clip-09",
	}
	finalDuration := 0.0
	for index, timelineItem := range plan.Timeline {
		if timelineItem.Source.Type != "video" {
			t.Errorf("Timeline[%d].Source.Type = %q, want video", index, timelineItem.Source.Type)
		}
		if got := timelineItem.Source.URL; got != wantURLs[index] {
			t.Errorf("Timeline[%d].Source.URL = %q, want %q", index, got, wantURLs[index])
		}
		if got := timelineItem.DurationSeconds; got != wantDurations[index] {
			t.Errorf("Timeline[%d].DurationSeconds = %v, want %v", index, got, wantDurations[index])
		}
		finalDuration += timelineItem.DurationSeconds
	}
	if finalDuration != mikeTysonExpectedFinalSeconds {
		t.Fatalf("computed final duration = %v, want %v", finalDuration, mikeTysonExpectedFinalSeconds)
	}

	wantVoiceoverURLs := []string{"velox-asset://mike-tyson/voiceover-intro"}
	for index := 1; index <= 9; index++ {
		wantVoiceoverURLs = append(wantVoiceoverURLs, "velox-asset://mike-tyson/voiceover-0"+string(rune('0'+index)))
	}
	voiceoverCount := 0
	musicCount := 0
	for index, track := range plan.AudioTracks {
		switch track.Role {
		case "voiceover":
			if track.SourceURL != wantVoiceoverURLs[voiceoverCount] {
				t.Errorf("AudioTracks[%d].SourceURL = %q, want %q", index, track.SourceURL, wantVoiceoverURLs[voiceoverCount])
			}
			voiceoverCount++
		case "background_music":
			musicCount++
			if track.SourceURL != "velox-asset://mike-tyson/background-music" {
				t.Errorf("music source = %q, want canonical Mike Tyson music", track.SourceURL)
			}
			if !track.Loop {
				t.Error("background music must be looped")
			}
			if track.DurationSeconds != 0 {
				t.Errorf("music track duration = %v, want 0 so engine binds it to final duration", track.DurationSeconds)
			}
		default:
			t.Errorf("AudioTracks[%d] role = %q, want voiceover or background_music", index, track.Role)
		}
	}
	if voiceoverCount != 10 {
		t.Fatalf("render-plan voiceover tracks = %d, want 10", voiceoverCount)
	}
	if musicCount != 1 {
		t.Fatalf("render-plan music tracks = %d, want 1", musicCount)
	}
}
