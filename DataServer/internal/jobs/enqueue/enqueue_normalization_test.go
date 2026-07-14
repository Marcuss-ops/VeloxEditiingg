package enqueue

import (
	"os"
	"path/filepath"
	"testing"
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
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "completed", "result": map[string]interface{}{"scenes_json": sceneJSON}}) {
		t.Error("want false for no voiceover")
	}
}
