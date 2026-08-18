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
				"video_name": "Pipeline Video", "script_text": "Test script.", "json_path": jsonPath,
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
				"video_name": "MD Video", "markdown_path": mdPath, "json_path": jsonPath,
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
				"video_name": "Multi", "script_text": "Text.", "json_path": jsonPath,
				"voiceover_paths": []string{v1, v2},
			},
		}
		payload, _ := BuildPipelinePayload(result)
		if _, present := payload["voiceover_paths"]; present {
			t.Errorf("renderer payload must not contain positional voiceover_paths: %#v", payload["voiceover_paths"])
		}
	})

	t.Run("flat", func(t *testing.T) {
		t.Parallel()
		result := map[string]interface{}{
			"video_name": "Flat", "script_text": "Flat script.", "json_path": jsonPath, "voiceover_path": voicePath,
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
			{"no_voiceover", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"video_name": "X", "json_path": jsonPath}}},
			{"no_title", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"json_path": jsonPath, "voiceover": map[string]interface{}{"local_path": voicePath}}}},
			{"no_scenes", map[string]interface{}{"status": "completed", "result": map[string]interface{}{"video_name": "X", "json_path": jsonPath, "voiceover": map[string]interface{}{"local_path": voicePath}}}},
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
				"subtitles": map[string]interface{}{
					"url":    "velox-asset://subtitles-1",
					"format": "srt",
				},
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
	scenes, ok := normalized["scenes"].([]map[string]interface{})
	if !ok || len(scenes) != 1 {
		t.Fatalf("scenes = %#v, want one canonical scene", normalized["scenes"])
	}
	subtitles, ok := scenes[0]["subtitles"].(map[string]interface{})
	if !ok || subtitles["url"] != "velox-asset://subtitles-1" {
		t.Fatalf("scene subtitles = %#v, want canonical nested subtitles", scenes[0]["subtitles"])
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
	scenes, ok := normalized["scenes"].([]map[string]interface{})
	if !ok || len(scenes) != 1 {
		t.Fatalf("scenes = %#v, want one canonical scene", normalized["scenes"])
	}
	if _, ok := scenes[0]["subtitles"].(map[string]interface{}); !ok {
		t.Fatalf("scene subtitles = %#v, want canonical nested subtitles", scenes[0]["subtitles"])
	}
	if _, present := normalized["subtitle_tracks"]; present {
		t.Fatalf("normalized payload contains retired top-level subtitle_tracks")
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
	if !ShouldForwardPipelineResult(map[string]interface{}{
		"scenes_json": sceneJSON,
	}) {
		t.Error("want true when status is absent for a legacy payload")
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "failed"}) {
		t.Error("want false for failed")
	}
	for _, status := range []string{"succeeded", "done", "published"} {
		if ShouldForwardPipelineResult(map[string]interface{}{
			"status":      status,
			"scenes_json": sceneJSON,
		}) {
			t.Errorf("status %q is a lifecycle/publication alias and must not be accepted as input-assembly completion", status)
		}
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "completed", "result": map[string]interface{}{"voiceover_path": voicePath}}) {
		t.Error("want false for no scenes")
	}
	if !ShouldForwardPipelineResult(map[string]interface{}{"status": "completed", "result": map[string]interface{}{"scenes_json": sceneJSON}}) {
		t.Error("want true for renderable scenes without voiceover")
	}
	// audio_tracks-only payload is no longer forwardable: top-level
	// audio_tracks was retired and does not count as a renderable audio
	// source. Only voiceover or renderable media make a result forwardable.
	bgmSceneJSON := `[{"text":"BGM-only scene","duration_seconds":12}]`
	if ShouldForwardPipelineResult(map[string]interface{}{
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
		t.Error("want false for audio_tracks-only payload (audio_tracks retired)")
	}
	// audio_tracks with no source_url must not count as forwardable.
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
