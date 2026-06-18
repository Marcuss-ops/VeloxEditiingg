package enqueue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildSceneImagePayload(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()

	payload := map[string]interface{}{
		"video_name":          "Test Video",
		"source_text":         "This is a test script.",
		"language":            "it",
		"voiceover_path":      "/tmp/test-voiceover.mp3",
		"drive_output_folder": "https://drive.google.com/drive/folders/test",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
			map[string]interface{}{"text": "Scene 2", "image_link": "https://example.com/img2.png"},
		},
	}

	result, err := BuildSceneImagePayload(payload, tempDir, filepath.Join(tempDir, "videos"))
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	for k, want := range map[string]interface{}{
		"video_name": "Test Video", "job_type": "process_video", "video_mode": "scene_image",
		"submitted_via": "api_script_generate_with_images", "source": "script_generate_with_images",
		"voiceover_path": "/tmp/test-voiceover.mp3", "scene_count": 2, "voiceover_count": 1,
		"script_text": "This is a test script.",
	} {
		if result[k] != want {
			t.Errorf("%s: got %v, want %v", k, result[k], want)
		}
	}

	for _, id := range []string{"job_id", "job_run_id", "correlation_id"} {
		if v, _ := result[id].(string); v == "" {
			t.Errorf("%s should be non-empty", id)
		}
	}

	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 2 {
		t.Errorf("want 2 scenes, got %d", len(scenes))
	}
	paths, _ := result["scene_image_paths"].([]string)
	if len(paths) != 2 {
		t.Errorf("want 2 scene_image_paths, got %d", len(paths))
	}
	params, _ := result["parameters"].(map[string]interface{})
	if params["job_type"] != "process_video" {
		t.Errorf("parameters.job_type: got %v", params["job_type"])
	}
}

func TestBuildSceneImagePayload_YoutubeGroupAlias(t *testing.T) {
	t.Parallel()
	payload := map[string]interface{}{
		"video_name": "YT Test", "channel_id": "amish",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
	}
	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if result["youtube_group"] != "amish" || result["channel_id"] != "amish" {
		t.Errorf("youtube_group/channel_id: got %v/%v", result["youtube_group"], result["channel_id"])
	}
}

func TestBuildSceneImagePayload_Fallbacks(t *testing.T) {
	t.Parallel()
	payload := map[string]interface{}{
		"voiceover_path": "/tmp/v.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "S1", "image_link": "https://example.com/i1.png"},
			map[string]interface{}{"text": "S2", "image_link": "https://example.com/i2.png"},
			map[string]interface{}{"text": "S3"},
		},
	}
	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 3 {
		t.Fatalf("want 3 scenes, got %d", len(scenes))
	}
	if img, _ := scenes[2]["image_link"].(string); img == "" {
		t.Error("scene 3 should have fallback image_link")
	}
}

func TestBuildSceneImagePayload_Errors(t *testing.T) {
	t.Parallel()
	base := map[string]interface{}{"voiceover_path": "/tmp/v.mp3"}
	_, err := BuildSceneImagePayload(base, t.TempDir(), t.TempDir())
	if err == nil {
		t.Error("want error for missing scenes")
	}
	_, err = BuildSceneImagePayload(map[string]interface{}{"scenes": []interface{}{}}, t.TempDir(), t.TempDir())
	if err == nil {
		t.Error("want error for missing voiceover")
	}
}

func TestBuildSceneImagePayloadForMaster(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	srcVoice := filepath.Join(tempDir, "voiceover.mp3")
	os.WriteFile(srcVoice, []byte("fake-audio"), 0o600)
	srcImage := filepath.Join(tempDir, "scene.jpg")
	os.WriteFile(srcImage, []byte("fake-image"), 0o600)

	payload := map[string]interface{}{
		"video_name": "Master Test", "voiceover_path": srcVoice,
		"scenes": []interface{}{map[string]interface{}{"text": "S1", "image_link": srcImage}},
	}
	result, err := BuildSceneImagePayloadForMaster(payload, tempDir, filepath.Join(tempDir, "videos"), "http://master.example")
	if err != nil {
		t.Fatal(err)
	}
	vp, _ := result["voiceover_path"].(string)
	if vp != srcVoice {
		t.Errorf("want voiceover_path %q, got %q", srcVoice, vp)
	}
	sp, _ := result["scene_image_paths"].([]string)
	if len(sp) != 1 || sp[0] != srcImage {
		t.Fatalf("want scene_image_paths [%q], got %v", srcImage, sp)
	}
	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 1 {
		t.Fatalf("want 1 scene, got %d", len(scenes))
	}
	if img, _ := scenes[0]["image_link"].(string); img != srcImage {
		t.Fatalf("want image_link %q, got %q", srcImage, img)
	}
}

func TestBuildSceneImagePayloadForMaster_PreservesRemoteSources(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()

	voiceURL := "https://drive.google.com/file/d/voice-drive-id/view?usp=sharing"
	sceneURL := "https://drive.google.com/file/d/scene-drive-id/view?usp=sharing"
	payload := map[string]interface{}{
		"video_name":     "Remote Sources",
		"voiceover_path": voiceURL,
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": sceneURL}},
	}

	result, err := BuildSceneImagePayloadForMaster(payload, tempDir, filepath.Join(tempDir, "videos"), "http://master.example")
	if err != nil {
		t.Fatal(err)
	}

	if got, _ := result["voiceover_path"].(string); got != voiceURL {
		t.Fatalf("want remote voiceover preserved as %q, got %q", voiceURL, got)
	}
	if got, _ := result["audio_path"].(string); got != voiceURL {
		t.Fatalf("want remote audio_path preserved as %q, got %q", voiceURL, got)
	}

	sp, _ := result["scene_image_paths"].([]string)
	if len(sp) != 1 || sp[0] != sceneURL {
		t.Fatalf("want remote scene image preserved, got %v", sp)
	}

	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 1 {
		t.Fatalf("want 1 scene, got %d", len(scenes))
	}
	if img, _ := scenes[0]["image_link"].(string); img != sceneURL {
		t.Fatalf("want remote scene image link preserved as %q, got %q", sceneURL, img)
	}
	if _, err := os.Stat(filepath.Join(tempDir, "worker_downloads")); !os.IsNotExist(err) {
		t.Fatalf("did not expect staged assets for remote sources")
	}
}

func TestBuildSceneImagePayload_PreservesIDs(t *testing.T) {
	t.Parallel()
	payload := map[string]interface{}{
		"voiceover_path": "/tmp/v.mp3", "job_id": "custom-id", "job_run_id": "custom-run", "correlation_id": "custom-corr",
		"scenes": []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
	}
	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if result["job_id"] != "custom-id" || result["job_run_id"] != "custom-run" || result["correlation_id"] != "custom-corr" {
		t.Errorf("IDs not preserved: %v %v %v", result["job_id"], result["job_run_id"], result["correlation_id"])
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

func TestRenderJobResponse(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{
			"job_id": "j1", "status": "COMPLETED", "video_name": "V", "scene_count": 5,
			"voiceover_count": 3, "video_mode": "scene_image",
		}
		r := RenderJobResponse(job, false)
		if r["ok"] != true || r["job_id"] != "j1" || r["status"] != "COMPLETED" {
			t.Errorf("unexpected: %v", r)
		}
		if _, has := r["job"]; has {
			t.Error("no 'job' key when full=false")
		}
	})

	t.Run("full", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j2", "request": map[string]interface{}{"raw": "x"}}
		r := RenderJobResponse(job, true)
		if r["job"] == nil || r["request"] == nil {
			t.Error("want job/request keys when full=true")
		}
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		r := RenderJobResponse(nil, false)
		if r["ok"] != false {
			t.Errorf("want ok=false, got %v", r["ok"])
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j3", "status": "FAILED", "error": "boom"}
		r := RenderJobResponse(job, false)
		if r["error"] != "boom" {
			t.Errorf("want error 'boom', got %v", r["error"])
		}
	})
}

func TestBuildSceneImagePayload_RoundTrip(t *testing.T) {
	t.Parallel()
	payload := map[string]interface{}{
		"video_name": "RT", "source_text": "Script.", "voiceover_path": "/tmp/v.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"},
		},
	}
	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(result)
	var decoded map[string]interface{}
	json.Unmarshal(data, &decoded)
	if decoded["video_name"] != "RT" || decoded["job_type"] != "process_video" {
		t.Errorf("round-trip mismatch: %v", decoded)
	}
}
