package enqueue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/contract"
)

// =====================================================================
// Normalization tests
// =====================================================================
//
// Verifies the canonical-input contract:
//   - NormalizeScenesPayload accepts the three on-wire shapes
//     (scenes[] / flat images[] / scenes_json string) and dedups.
//   - BuildPipelinePayload extracts the inner "result" envelope, walks
//     nested/markdown/multi-voice/flat shapes, and rejects nil/empty
//     results so the downstream enqueue receives a fully populated
//     canonical payload.
//   - FlattenPipelineResult is the canonical-input re-writer for
//     pipeline result objects (it must NOT mutate the input map).
//   - ShouldForwardPipelineResult gates whether a pipeline result is
//     eligible to be re-enqueued (status==completed + scenes+voiceover).

func TestNormalizeSceneVideoPayload_IsIdempotent(t *testing.T) {
	input := map[string]interface{}{
		"job_id":          "idempotent-job",
		"job_run_id":      "idempotent-run",
		"correlation_id":  "idempotent-correlation",
		"video_name":      "Idempotent render",
		"script_text":     "The canonical payload must remain stable.",
		"voiceover_paths": []interface{}{"velox-asset://voiceover.mp3"},
		"delivery_plan":   []interface{}{map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3}},
		"video_metadata":  map[string]interface{}{"width": 1920, "height": 1080, "fps_num": 30, "fps_den": 1, "video_codec": "h264"},
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id":         "scene-0",
				"text":             "Scene 0",
				"image_url":        "velox-asset://scene-0.png",
				"duration_seconds": 5.0,
			},
		},
	}

	first, err := normalizeSceneVideoPayload(input)
	if err != nil {
		t.Fatalf("first normalization: %v", err)
	}
	firstBeforeSecond, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first normalized payload before second pass: %v", err)
	}
	second, err := normalizeSceneVideoPayload(first)
	if err != nil {
		t.Fatalf("second normalization: %v", err)
	}

	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first normalized payload: %v", err)
	}
	if string(firstBeforeSecond) != string(firstJSON) {
		t.Fatalf("second normalization mutated the first normalized payload:\nbefore: %s\nafter:  %s", firstBeforeSecond, firstJSON)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second normalized payload: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("normalization is not idempotent:\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
	if err := contract.ValidatePayload(second); err != nil {
		t.Fatalf("second normalized payload failed canonical validation: %v", err)
	}

	for _, alias := range contract.LegacyAliasKeys {
		if _, present := second[alias]; present {
			t.Fatalf("normalized payload contains legacy alias %q", alias)
		}
	}
}

func TestNormalizeScenesPayload(t *testing.T) {
	t.Parallel()

	t.Run("scenes_array", func(t *testing.T) {
		t.Parallel()
		payload := map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{"text": "S1", "image_link": "https://example.com/i1.png"},
				map[string]interface{}{"text": "S2", "image_link": "https://example.com/i2.png"},
			},
		}
		entries, images, err := NormalizeScenesPayload(payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 || len(images) != 2 {
			t.Errorf("want 2 entries/2 images, got %d/%d", len(entries), len(images))
		}
		for i, e := range entries {
			if d, _ := e["duration_seconds"].(float64); d <= 0 {
				t.Errorf("scene %d: want positive duration, got %v", i, d)
			}
		}
	})

	t.Run("flat_images", func(t *testing.T) {
		t.Parallel()
		payload := map[string]interface{}{
			"images": []string{"https://example.com/a.png", "https://example.com/b.png", "https://example.com/c.png"},
		}
		entries, images, err := NormalizeScenesPayload(payload)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 3 || len(images) != 3 {
			t.Errorf("want 3/3, got %d/%d", len(entries), len(images))
		}
		zoom, _ := entries[0]["zoom"].(map[string]interface{})
		if zoom["type"] != "light_zoom_in" {
			t.Errorf("want zoom.type light_zoom_in, got %v", zoom["type"])
		}
	})

	t.Run("dedup", func(t *testing.T) {
		t.Parallel()
		payload := map[string]interface{}{"images": []string{"a.png", "a.png", "b.png"}}
		_, images, _ := NormalizeScenesPayload(payload)
		if len(images) != 2 {
			t.Errorf("want 2 deduped, got %d", len(images))
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		_, _, err := NormalizeScenesPayload(map[string]interface{}{})
		if err == nil {
			t.Error("want error for empty")
		}
	})

	t.Run("invalid_json", func(t *testing.T) {
		t.Parallel()
		_, _, err := NormalizeScenesPayload(map[string]interface{}{"scenes_json": "not json"})
		if err == nil {
			t.Error("want error for invalid json")
		}
	})
}

func TestBuildPipelinePayload(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	jsonPath := filepath.Join(tempDir, "script.json")
	os.WriteFile(jsonPath, []byte(`{"scenes":[{"text":"S1","image_link":"https://example.com/i.png"}]}`), 0o644)
	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	os.WriteFile(voicePath, []byte("dummy"), 0o644)

	t.Run("nested", func(t *testing.T) {
		t.Parallel()
		result := map[string]interface{}{
			"status": "completed", "result": map[string]interface{}{
				"title": "Pipeline Video", "script_text": "Test script.", "json_path": jsonPath,
				"voiceover": map[string]interface{}{"local_path": voicePath},
			},
		}
		payload, err := BuildPipelinePayload(result)
		if err != nil {
			t.Fatal(err)
		}
		if payload["video_name"] != "Pipeline Video" || payload["job_type"] != "process_video" {
			t.Errorf("unexpected: %v %v", payload["video_name"], payload["job_type"])
		}

		for _, alias := range []string{"title", "voiceover_path", "audio_path", "run_id", "id"} {
			if _, present := payload[alias]; present {
				t.Errorf("%q alias must NOT be present in canonical pipeline payload, got %v", alias, payload[alias])
			}
		}
	})

	t.Run("markdown", func(t *testing.T) {
		t.Parallel()
		mdPath := filepath.Join(tempDir, "script.md")
		os.WriteFile(mdPath, []byte("# Title\n\nContent."), 0o644)
		result := map[string]interface{}{
			"status": "completed", "result": map[string]interface{}{
				"title": "MD Video", "markdown_path": mdPath, "json_path": jsonPath,
				"voiceover": map[string]interface{}{"local_path": voicePath},
			},
		}
		payload, _ := BuildPipelinePayload(result)
		if payload["script_text"] != "# Title\n\nContent." {
			t.Errorf("want markdown text, got %q", payload["script_text"])
		}
	})

	t.Run("multi_voice", func(t *testing.T) {
		t.Parallel()
		v1 := filepath.Join(tempDir, "v1.mp3")
		v2 := filepath.Join(tempDir, "v2.mp3")
		os.WriteFile(v1, []byte("d"), 0o644)
		os.WriteFile(v2, []byte("d"), 0o644)
		result := map[string]interface{}{
			"status": "completed", "result": map[string]interface{}{
				"title": "Multi", "script_text": "Text.", "json_path": jsonPath,
				"voiceover_paths": []string{v1, v2},
			},
		}
		payload, _ := BuildPipelinePayload(result)
		paths, _ := payload["voiceover_paths"].([]string)
		if len(paths) != 2 {
			t.Errorf("want 2 paths, got %d", len(paths))
		}
	})

	t.Run("flat", func(t *testing.T) {
		t.Parallel()
		result := map[string]interface{}{
			"title": "Flat", "script_text": "Flat script.", "json_path": jsonPath, "voiceover_path": voicePath,
		}
		payload, err := BuildPipelinePayload(result)
		if err != nil {
			t.Fatal(err)
		}
		if payload["video_name"] != "Flat" {
			t.Errorf("want Flat, got %v", payload["video_name"])
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name   string
			result map[string]interface{}
		}{
			{"nil", nil},
			{"no_voiceover", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"title": "X", "json_path": jsonPath}}},
			{"no_title", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"json_path": jsonPath, "voiceover": map[string]interface{}{"local_path": voicePath}}}},
			{"no_scenes", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"title": "X", "json_path": jsonPath, "voiceover": map[string]interface{}{"local_path": voicePath}}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := BuildPipelinePayload(tc.result)
				if err == nil {
					t.Error("want error")
				}
			})
		}
	})
}

func TestNormalizeSceneVideoPayload_AttachesLegacyClipTimeline(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Legacy clip smoke",
		"script_text": "Narrated legacy clip body.",
		"voiceover_paths": []interface{}{
			"velox-asset://voiceovers/scene-1.mp3",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene 1",
				"clip_link":        "https://drive.example.com/clip-1.mp4",
				"duration_seconds": float64(5),
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	items, ok := normalized["items"].([]map[string]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("items = %#v, want one timeline item", normalized["items"])
	}
	if got := items[0]["url"]; got != "https://drive.example.com/clip-1.mp4" {
		t.Fatalf("items[0].url = %v", got)
	}
	clips, ok := normalized["clips"].([]string)
	if !ok || len(clips) != 1 || clips[0] != "https://drive.example.com/clip-1.mp4" {
		t.Fatalf("clips = %#v", normalized["clips"])
	}
	tracks, ok := normalized["audio_tracks"].([]map[string]interface{})
	if !ok || len(tracks) != 1 {
		t.Fatalf("audio_tracks = %#v, want one voiceover track", normalized["audio_tracks"])
	}
	if got := tracks[0]["source_url"]; got != "velox-asset://voiceovers/scene-1.mp3" {
		t.Fatalf("audio_tracks[0].source_url = %v", got)
	}
}

func TestNormalizeSceneVideoPayload_PreservesBackgroundMusicAndMergesTwoVoiceovers(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Mixed audio timeline smoke",
		"script_text": "Background music plus two scene voiceovers.",
		"voiceover_paths": []interface{}{
			"velox-asset://voiceover-scene-0",
			"velox-asset://voiceover-scene-1",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id":         "scene-0",
				"clip_link":        "velox-asset://clip-scene-0",
				"duration_seconds": float64(6),
			},
			map[string]interface{}{
				"scene_id":         "scene-1",
				"clip_link":        "velox-asset://clip-scene-1",
				"duration_seconds": float64(6),
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url":        "velox-asset://background-music",
				"role":              "background_music",
				"volume":            float64(0.12),
				"start_time_offset": float64(0),
				"duration_seconds":  float64(12),
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	tracks, ok := normalized["audio_tracks"].([]map[string]interface{})
	if !ok {
		t.Fatalf("audio_tracks = %#v, want []map[string]interface{}", normalized["audio_tracks"])
	}
	if len(tracks) != 3 {
		t.Fatalf("audio_tracks len = %d, want 3 (background music + two voiceovers)", len(tracks))
	}

	bySource := make(map[string]map[string]interface{}, len(tracks))
	for _, track := range tracks {
		source, _ := track["source_url"].(string)
		bySource[source] = track
	}

	assertTrack := func(source, role string, volume, offset, duration float64) {
		t.Helper()
		track, exists := bySource[source]
		if !exists {
			t.Fatalf("missing %s track %q in %#v", role, source, tracks)
		}
		if got := track["role"]; got != role {
			t.Errorf("%s role = %v, want %q", source, got, role)
		}
		if got := asFloat(track["volume"]); got != volume {
			t.Errorf("%s volume = %v, want %v", source, got, volume)
		}
		if got := asFloat(track["start_time_offset"]); got != offset {
			t.Errorf("%s start_time_offset = %v, want %v", source, got, offset)
		}
		if got := asFloat(track["duration_seconds"]); got != duration {
			t.Errorf("%s duration_seconds = %v, want %v", source, got, duration)
		}
	}

	assertTrack("velox-asset://background-music", "background_music", 0.12, 0, 12)
	assertTrack("velox-asset://voiceover-scene-0", "voiceover", 1, 0, 6)
	assertTrack("velox-asset://voiceover-scene-1", "voiceover", 1, 6, 6)
}

func TestNormalizeSceneVideoPayload_DeduplicatesCombinedAudioTracks(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Deduplicated audio timeline smoke",
		"script_text": "Duplicate and offset audio tracks.",
		"voiceover_paths": []interface{}{
			"velox-asset://voiceover-scene-0",
		},
		"scenes": []interface{}{

			map[string]interface{}{
				"clip_link":        "velox-asset://clip-scene-0",
				"duration_seconds": float64(12),
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url":        "velox-asset://background-music",
				"role":              "background_music",
				"volume":            float64(0.12),
				"start_time_offset": float64(0),
				"duration_seconds":  float64(12),
			},
			map[string]interface{}{
				"source_url":        "velox-asset://background-music",
				"role":              "background_music",
				"volume":            float64(0.5),
				"start_time_offset": float64(0),
				"duration_seconds":  float64(12),
			},
			map[string]interface{}{
				"source_url":        "velox-asset://background-music",
				"role":              "background_music",
				"volume":            float64(0.12),
				"start_time_offset": float64(6),
				"duration_seconds":  float64(6),
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	tracks, ok := normalized["audio_tracks"].([]map[string]interface{})
	if !ok {
		t.Fatalf("audio_tracks = %#v, want []map[string]interface{}", normalized["audio_tracks"])
	}
	if len(tracks) != 3 {
		t.Fatalf("audio_tracks len = %d, want 3 (duplicate removed, distinct offset and voiceover kept)", len(tracks))
	}
	if got := tracks[0]["role"]; got != "background_music" {
		t.Errorf("first track role = %v, want background_music", got)
	}
	if got := asFloat(tracks[0]["volume"]); got != 0.12 {
		t.Errorf("first duplicate volume = %v, want first occurrence volume 0.12", got)
	}
	if got := asFloat(tracks[0]["start_time_offset"]); got != 0 {
		t.Errorf("first track offset = %v, want 0", got)
	}
	if got := asFloat(tracks[0]["duration_seconds"]); got != 12 {
		t.Errorf("first track duration = %v, want 12", got)
	}
	if got := asFloat(tracks[1]["start_time_offset"]); got != 6 {
		t.Errorf("second track offset = %v, want 6", got)
	}
	if got := tracks[2]["role"]; got != "voiceover" {
		t.Errorf("third track role = %v, want voiceover", got)
	}
	if got := tracks[2]["source_url"]; got != "velox-asset://voiceover-scene-0" {
		t.Errorf("third track source = %v, want voiceover asset", got)
	}
}

func TestNormalizeSceneVideoPayload_UsesNestedVoiceoverDurationForClipTimeline(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Narrated clip smoke",
		"script_text": "Narrated clip body.",
		"voiceover_paths": []interface{}{
			"velox-asset://voice-1",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene 1",
				"clip_link":        "velox-asset://clip-1",
				"duration_seconds": float64(5),
				"clip": map[string]interface{}{
					"url":         "velox-asset://clip-1",
					"duration_ms": float64(5000),
				},
				"voiceover": map[string]interface{}{
					"url":         "velox-asset://voice-1",
					"duration_ms": float64(24216),
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	items, ok := normalized["items"].([]map[string]interface{})
	if !ok || len(items) != 1 {
		t.Fatalf("items = %#v, want one timeline item", normalized["items"])
	}
	if got := asFloat(items[0]["duration"]); got != 24.216 {
		t.Fatalf("items[0].duration = %v, want 24.216", got)
	}
	tracks, ok := normalized["audio_tracks"].([]map[string]interface{})
	if !ok || len(tracks) != 1 {
		t.Fatalf("audio_tracks = %#v, want one voiceover track", normalized["audio_tracks"])
	}
	if got := asFloat(tracks[0]["duration_seconds"]); got != 24.216 {
		t.Fatalf("audio_tracks[0].duration_seconds = %v, want 24.216", got)
	}
	scenes, ok := normalized["scenes"].([]map[string]interface{})
	if !ok || len(scenes) != 1 {
		t.Fatalf("scenes = %#v, want one scene", normalized["scenes"])
	}
	if got := asFloat(scenes[0]["duration_seconds"]); got != 24.216 {
		t.Fatalf("scenes[0].duration_seconds = %v, want 24.216", got)
	}
}

func TestNormalizeSceneVideoPayload_PreservesVisualTimelineFields(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Subtitle layer smoke",
		"script_text": "Subtitle layer body.",
		"voiceover_paths": []interface{}{
			"velox-asset://voice-1",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene 1",
				"clip_link":        "velox-asset://clip-1",
				"duration_seconds": float64(5),
			},
		},
		"subtitle_tracks": []interface{}{
			map[string]interface{}{
				"source": "velox-asset://subtitles-1",
				"preset": "active_word_pop",
				"font":   "Inter",
			},
		},
		"layers": []interface{}{
			map[string]interface{}{
				"id":               "important-1",
				"type":             "text",
				"role":             "important_phrase",
				"text":             "Important",
				"start_seconds":    float64(1),
				"duration_seconds": float64(2),
				"preset":           "important_phrase_v1",
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	if got, ok := normalized["subtitle_tracks"].([]map[string]interface{}); !ok || len(got) != 1 {
		t.Fatalf("subtitle_tracks = %#v, want one preserved track", normalized["subtitle_tracks"])
	}
	if got, ok := normalized["layers"].([]interface{}); !ok || len(got) != 1 {
		t.Fatalf("layers = %#v, want one preserved layer", normalized["layers"])
	}
}

func TestNormalizeSceneVideoPayload_DerivesSubtitleTrackFromSceneSubtitles(t *testing.T) {
	t.Parallel()

	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Nested subtitle smoke",
		"script_text": "Nested subtitle body.",
		"voiceover_paths": []interface{}{
			"velox-asset://voice-1",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene 1",
				"clip_link":        "velox-asset://clip-1",
				"duration_seconds": float64(5),
				"subtitles": map[string]interface{}{
					"url":    "velox-asset://subtitle-1",
					"format": "srt",
					"preset": "active_word_pop",
					"font":   "Inter",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	normalized, err = ProjectLegacyWorkerPayload(normalized)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}

	tracks, ok := normalized["subtitle_tracks"].([]map[string]interface{})
	if !ok || len(tracks) != 1 {
		t.Fatalf("subtitle_tracks = %#v, want one derived track", normalized["subtitle_tracks"])
	}
	if got := tracks[0]["source"]; got != "velox-asset://subtitle-1" {
		t.Fatalf("subtitle_tracks[0].source = %v, want velox-asset://subtitle-1", got)
	}
	if got := tracks[0]["format"]; got != "srt" {
		t.Fatalf("subtitle_tracks[0].format = %v, want srt", got)
	}
}

func TestFlattenPipelineResult(t *testing.T) {
	t.Parallel()
	nested := map[string]interface{}{"ok": true, "result": map[string]interface{}{"title": "T", "text": "X"}}
	flat := FlattenPipelineResult(nested)
	if flat["title"] != "T" || flat["ok"] != true {
		t.Errorf("unexpected: %v", flat)
	}
	plain := map[string]interface{}{"ok": true, "title": "Flat"}
	if FlattenPipelineResult(plain)["title"] != "Flat" {
		t.Error("flat result mismatch")
	}
}

func TestShouldForwardPipelineResult(t *testing.T) {
	t.Parallel()
	sceneJSON := `[{"text":"S1","image_link":"https://example.com/i.png"}]`
	voicePath := filepath.Join(t.TempDir(), "v.mp3")
	os.WriteFile(voicePath, []byte("d"), 0o644)

	valid := map[string]interface{}{"status": "completed", "result": map[string]interface{}{"scenes_json": sceneJSON, "voiceover_path": voicePath}}
	if !ShouldForwardPipelineResult(valid) {
		t.Error("want true for valid")
	}
	if ShouldForwardPipelineResult(nil) {
		t.Error("want false for nil")
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "failed"}) {
		t.Error("want false for failed")
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "completed", "result": map[string]interface{}{"voiceover_path": voicePath}}) {
		t.Error("want false for no scenes")
	}
	if !ShouldForwardPipelineResult(map[string]interface{}{"status": "completed", "result": map[string]interface{}{"scenes_json": sceneJSON}}) {
		t.Error("want true for renderable scenes without voiceover")
	}
	// audio_tracks-only payload: background music + scenes, no voiceover,
	// should be forwardable (the worker muxes audio_tracks into the final AAC).
	// Uses scenes_json — the canonical shape after ToWorkerPayload marshals
	// the typed DTO scenes back to JSON.
	bgmSceneJSON := `[{"text":"BGM-only scene","duration_seconds":12}]`
	if !ShouldForwardPipelineResult(map[string]interface{}{
		"status":      "completed",
		"scenes_json": bgmSceneJSON,
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url": "velox-asset://music-1",
				"role":       "background_music",
				"volume":     float64(0.12),
			},
		},
	}) {
		t.Error("want true for audio_tracks-only payload (BGM + scenes, no voiceover)")
	}
	// audio_tracks with no source_url should NOT count as forwardable.
	if ShouldForwardPipelineResult(map[string]interface{}{
		"status":      "completed",
		"scenes_json": `[{"text":"Empty tracks","duration_seconds":5}]`,
		"audio_tracks": []interface{}{
			map[string]interface{}{"role": "background_music", "volume": float64(0.1)},
		},
	}) {
		t.Error("want false for audio_tracks with no source_url")
	}
}
