package enqueue

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// =============================================================================
// BuildSceneImagePayload tests
// =============================================================================

func TestBuildSceneImagePayload_WithScenesArray(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	videosDir := filepath.Join(tempDir, "videos")

	payload := map[string]interface{}{
		"video_name":          "Test Video",
		"source_text":         "This is a test script.",
		"language":            "it",
		"voiceover_path":      "/tmp/test-voiceover.mp3",
		"drive_output_folder": "https://drive.google.com/drive/folders/test",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
			map[string]interface{}{
				"text":       "Scene 2",
				"image_link": "https://example.com/img2.png",
			},
		},
	}

	result, err := BuildSceneImagePayload(payload, tempDir, videosDir)
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	if result["video_name"] != "Test Video" {
		t.Fatalf("want video_name 'Test Video', got %v", result["video_name"])
	}
	if result["job_type"] != "process_video" {
		t.Fatalf("want job_type 'process_video', got %v", result["job_type"])
	}
	if result["video_mode"] != "scene_image" {
		t.Fatalf("want video_mode 'scene_image', got %v", result["video_mode"])
	}
	if result["submitted_via"] != "api_script_generate_with_images" {
		t.Fatalf("want submitted_via 'api_script_generate_with_images', got %v", result["submitted_via"])
	}
	if result["source"] != "script_generate_with_images" {
		t.Fatalf("want source 'script_generate_with_images', got %v", result["source"])
	}
	if result["voiceover_path"] != "/tmp/test-voiceover.mp3" {
		t.Fatalf("want voiceover_path, got %v", result["voiceover_path"])
	}
	if result["scene_count"] != 2 {
		t.Fatalf("want scene_count 2, got %v", result["scene_count"])
	}
	if result["voiceover_count"] != 1 {
		t.Fatalf("want voiceover_count 1, got %v", result["voiceover_count"])
	}
	if result["script_text"] != "This is a test script." {
		t.Fatalf("want script_text, got %v", result["script_text"])
	}

	jobID, ok := result["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("want non-empty job_id, got %v", result["job_id"])
	}
	runID, ok := result["job_run_id"].(string)
	if !ok || runID == "" {
		t.Fatalf("want non-empty job_run_id, got %v", result["job_run_id"])
	}
	corrID, ok := result["correlation_id"].(string)
	if !ok || corrID == "" {
		t.Fatalf("want non-empty correlation_id, got %v", result["correlation_id"])
	}

	scenes, ok := result["scenes"].([]map[string]interface{})
	if !ok || len(scenes) != 2 {
		t.Fatalf("want 2 scenes, got %v", result["scenes"])
	}

	sceneImagePaths, ok := result["scene_image_paths"].([]string)
	if !ok || len(sceneImagePaths) != 2 {
		t.Fatalf("want 2 scene_image_paths, got %v", result["scene_image_paths"])
	}

	// Parameters should also be populated
	params, ok := result["parameters"].(map[string]interface{})
	if !ok {
		t.Fatalf("want parameters map, got %T", result["parameters"])
	}
	if params["job_type"] != "process_video" {
		t.Fatalf("want parameters.job_type 'process_video', got %v", params["job_type"])
	}
}

func TestBuildSceneImagePayload_PreservesYouTubeGroupAlias(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":     "YT Alias Test",
		"channel_id":     "amish",
		"voiceover_path": "/tmp/test-voiceover.mp3",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
		},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	if result["youtube_group"] != "amish" {
		t.Fatalf("want youtube_group amish, got %v", result["youtube_group"])
	}
	if result["channel_id"] != "amish" {
		t.Fatalf("want channel_id amish, got %v", result["channel_id"])
	}
	params, ok := result["parameters"].(map[string]interface{})
	if !ok {
		t.Fatalf("want parameters map, got %T", result["parameters"])
	}
	if params["youtube_group"] != "amish" {
		t.Fatalf("want parameters.youtube_group amish, got %v", params["youtube_group"])
	}
}

func TestBuildSceneImagePayload_WithSceneImageFallbacks(t *testing.T) {
	t.Parallel()

	// Scene 3 has no image_link — should get a fallback from scenes 1 or 2
	payload := map[string]interface{}{
		"video_name":     "Fallback Test",
		"voiceover_path": "/tmp/voice.mp3",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
			map[string]interface{}{
				"text":       "Scene 2",
				"image_link": "https://example.com/img2.png",
			},
			map[string]interface{}{
				"text": "Scene 3 (no image)",
			},
		},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 3 {
		t.Fatalf("want 3 scenes, got %d", len(scenes))
	}

	// Scene 3 should have an image_link from fallback
	scene3 := scenes[2]
	if img, _ := scene3["image_link"].(string); img == "" {
		t.Fatalf("scene 3 should have a fallback image_link, got empty")
	}

	// scene_image_paths have 3 entries but scene 3 falls back to an existing image
	paths, _ := result["scene_image_paths"].([]string)
	if len(paths) < 2 || len(paths) > 3 {
		t.Fatalf("want 2-3 scene_image_paths, got %d (%v)", len(paths), paths)
	}
}

func TestBuildSceneImagePayload_WithScenesJSON(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":     "JSON Test",
		"voiceover_path": "/tmp/voice.mp3",
		"scenes_json":    `[{"text":"Scene A","image_link":"https://example.com/a.png"},{"text":"Scene B","image_link":"https://example.com/b.png"}]`,
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	if result["scene_count"] != 2 {
		t.Fatalf("want scene_count 2, got %v", result["scene_count"])
	}
}

func TestBuildSceneImagePayload_WithFlatImages(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":     "Flat Images Test",
		"voiceover_path": "/tmp/voice.mp3",
		"images":         []string{"https://example.com/img1.png", "https://example.com/img2.png", "https://example.com/img3.png"},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	if result["scene_count"] != 3 {
		t.Fatalf("want scene_count 3 from flat images, got %v", result["scene_count"])
	}

	scenes, _ := result["scenes"].([]map[string]interface{})
	if len(scenes) != 3 {
		t.Fatalf("want 3 scenes, got %d", len(scenes))
	}
	// Each scene should have a text
	for i, scene := range scenes {
		if text, _ := scene["text"].(string); text == "" {
			t.Fatalf("scene %d should have text", i)
		}
	}
}

func TestBuildSceneImagePayloadForMaster_StagesVoiceover(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	videosDir := filepath.Join(tempDir, "videos")
	srcVoice := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(srcVoice, []byte("fake-audio"), 0o600); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	payload := map[string]interface{}{
		"video_name":     "Master Stage Test",
		"voiceover_path": srcVoice,
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
		},
	}

	result, err := BuildSceneImagePayloadForMaster(payload, tempDir, videosDir, "http://master.example")
	if err != nil {
		t.Fatalf("BuildSceneImagePayloadForMaster: %v", err)
	}

	voiceoverPath, _ := result["voiceover_path"].(string)
	if !strings.HasPrefix(voiceoverPath, "http://master.example/api/worker/assets/voiceover/") {
		t.Fatalf("want staged voiceover url, got %q", voiceoverPath)
	}

	jobID, _ := result["job_id"].(string)
	if jobID == "" {
		t.Fatal("expected job_id to be populated")
	}

	stagedLocal := filepath.Join(tempDir, "worker_downloads", "script_assets", jobID, filepath.Base(srcVoice))
	content, err := os.ReadFile(stagedLocal)
	if err != nil {
		t.Fatalf("read staged asset: %v", err)
	}
	if string(content) != "fake-audio" {
		t.Fatalf("staged asset content mismatch: %q", string(content))
	}
}

func TestBuildSceneImagePayload_MissingVoiceover(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name": "No Voice Test",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
		},
	}

	_, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err == nil {
		t.Fatal("want error for missing voiceover_path, got nil")
	}
}

func TestBuildSceneImagePayload_MissingScenes(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":     "No Scenes Test",
		"voiceover_path": "/tmp/voice.mp3",
	}

	_, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err == nil {
		t.Fatal("want error for missing scenes, got nil")
	}
}

func TestBuildSceneImagePayload_AutoDetectAudioDuration(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	// Create a real audio file so duration detection works
	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("fake mp3 data"), 0o644); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	payload := map[string]interface{}{
		"video_name":     "Duration Test",
		"voiceover_path": voicePath,
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
		},
	}

	result, err := BuildSceneImagePayload(payload, tempDir, tempDir)
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	// scene_duration_secs should be set (even if duration detection returns 0, fallback is 5)
	duration, _ := result["scene_duration_secs"].(float64)
	if duration <= 0 {
		t.Fatalf("want positive scene_duration_secs, got %v", duration)
	}
}

func TestBuildSceneImagePayload_GeneratesOutputPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	videosDir := filepath.Join(tempDir, "videos")

	payload := map[string]interface{}{
		"video_name":     "Output Path Test",
		"voiceover_path": "/tmp/voice.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
		},
	}

	result, err := BuildSceneImagePayload(payload, tempDir, videosDir)
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	outputPath, _ := result["output_path"].(string)
	if outputPath == "" {
		t.Fatal("want non-empty output_path")
	}
}

func TestBuildSceneImagePayload_PreservesExplicitIDs(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":     "Explicit IDs Test",
		"voiceover_path": "/tmp/voice.mp3",
		"job_id":         "my-custom-job-id",
		"job_run_id":     "my-custom-run-id",
		"correlation_id": "my-custom-corr-id",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
		},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	if result["job_id"] != "my-custom-job-id" {
		t.Fatalf("want job_id preserved, got %v", result["job_id"])
	}
	if result["job_run_id"] != "my-custom-run-id" {
		t.Fatalf("want job_run_id preserved, got %v", result["job_run_id"])
	}
	if result["correlation_id"] != "my-custom-corr-id" {
		t.Fatalf("want correlation_id preserved, got %v", result["correlation_id"])
	}
}

// =============================================================================
// NormalizeScenesPayload tests
// =============================================================================

func TestNormalizeScenesPayload_WithScenesArray(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
			map[string]interface{}{
				"text":       "Scene 2",
				"image_link": "https://example.com/img2.png",
			},
		},
	}

	entries, images, err := NormalizeScenesPayload(payload)
	if err != nil {
		t.Fatalf("NormalizeScenesPayload: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 scene entries, got %d", len(entries))
	}
	if len(images) != 2 {
		t.Fatalf("want 2 image paths, got %d", len(images))
	}
	if images[0] != "https://example.com/img1.png" {
		t.Fatalf("want img1, got %s", images[0])
	}
	if images[1] != "https://example.com/img2.png" {
		t.Fatalf("want img2, got %s", images[1])
	}

	// Each entry should have duration_seconds set
	for i, entry := range entries {
		dur, _ := entry["duration_seconds"].(float64)
		if dur <= 0 {
			t.Fatalf("scene %d: want positive duration_seconds, got %v", i, dur)
		}
	}
}

func TestNormalizeScenesPayload_WithScenesJSON(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"scenes_json": `[{"text":"A","image_link":"https://example.com/a.png"},{"text":"B","image_link":"https://example.com/b.png"}]`,
	}

	entries, images, err := NormalizeScenesPayload(payload)
	if err != nil {
		t.Fatalf("NormalizeScenesPayload: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries from scenes_json, got %d", len(entries))
	}
	if len(images) != 2 {
		t.Fatalf("want 2 images from scenes_json, got %d", len(images))
	}
}

func TestNormalizeScenesPayload_WithFlatImages(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"images": []string{"https://example.com/a.png", "https://example.com/b.png", "https://example.com/c.png"},
	}

	entries, images, err := NormalizeScenesPayload(payload)
	if err != nil {
		t.Fatalf("NormalizeScenesPayload: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries from flat images, got %d", len(entries))
	}
	if len(images) != 3 {
		t.Fatalf("want 3 images from flat images, got %d", len(images))
	}

	// Each auto-generated scene should have zoom settings
	for i, entry := range entries {
		zoom, ok := entry["zoom"].(map[string]interface{})
		if !ok {
			t.Fatalf("scene %d: want zoom map, got %T", i, entry["zoom"])
		}
		if zoom["type"] != "light_zoom_in" {
			t.Fatalf("scene %d: want zoom.type 'light_zoom_in', got %v", i, zoom["type"])
		}
	}
}

func TestNormalizeScenesPayload_WithDedup(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"images": []string{"https://example.com/a.png", "https://example.com/a.png", "https://example.com/b.png"},
	}

	_, images, err := NormalizeScenesPayload(payload)
	if err != nil {
		t.Fatalf("NormalizeScenesPayload: %v", err)
	}
	if len(images) != 2 {
		t.Fatalf("want 2 deduplicated images, got %d (%v)", len(images), images)
	}
}

func TestNormalizeScenesPayload_EmptyPayload(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{}
	_, _, err := NormalizeScenesPayload(payload)
	if err == nil {
		t.Fatal("want error for empty payload, got nil")
	}
}

func TestNormalizeScenesPayload_WithSceneImageFallbacks(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
			map[string]interface{}{
				"text":       "Scene 2",
				"image_link": "https://example.com/img2.png",
			},
			map[string]interface{}{
				"text": "Scene 3 (no image)",
			},
		},
	}

	entries, images, err := NormalizeScenesPayload(payload)
	if err != nil {
		t.Fatalf("NormalizeScenesPayload: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	// Scene 3 should have a fallback image_link
	img3, _ := entries[2]["image_link"].(string)
	if img3 == "" {
		t.Fatal("scene 3 should have fallback image_link")
	}
	// images are deduplicated — 3 scenes but scene 3 falls back to img1 or img2
	if len(images) < 2 || len(images) > 3 {
		t.Fatalf("want 2-3 images, got %d (%v)", len(images), images)
	}
}

// =============================================================================
// BuildPipelinePayload tests
// =============================================================================

func TestBuildPipelinePayload_WithNestedResult(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{"scenes": [{"text": "Scene 1", "image_link": "https://example.com/scene1.png"}]}`), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	result := map[string]interface{}{
		"ok":       true,
		"status":   "completed",
		"trace_id": "trace_123",
		"result": map[string]interface{}{
			"title":       "Pipeline Video",
			"script_text": "This is the generated script.",
			"json_path":   jsonPath,
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	payload, err := BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}

	if payload["video_name"] != "Pipeline Video" {
		t.Fatalf("want video_name 'Pipeline Video', got %v", payload["video_name"])
	}
	if payload["title"] != "Pipeline Video" {
		t.Fatalf("want title 'Pipeline Video', got %v", payload["title"])
	}
	if payload["script_text"] != "This is the generated script." {
		t.Fatalf("want script_text, got %v", payload["script_text"])
	}
	if payload["scenes_json"] == "" {
		t.Fatal("want non-empty scenes_json")
	}
	if payload["voiceover_path"] != voicePath {
		t.Fatalf("want voiceover_path %q, got %v", voicePath, payload["voiceover_path"])
	}
	if payload["job_type"] != "process_video" {
		t.Fatalf("want job_type 'process_video', got %v", payload["job_type"])
	}
	if payload["submitted_via"] != "pipeline_generate_with_images" {
		t.Fatalf("want submitted_via, got %v", payload["submitted_via"])
	}
	if payload["source"] != "pipeline_generate_with_images" {
		t.Fatalf("want source, got %v", payload["source"])
	}
}

func TestBuildPipelinePayload_WithMarkdownPath(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{"scenes": [{"text": "Scene 1", "image_link": "https://example.com/scene1.png"}]}`), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	markdownPath := filepath.Join(tempDir, "script.md")
	if err := os.WriteFile(markdownPath, []byte("# Title\n\nGenerated script content from markdown."), 0o644); err != nil {
		t.Fatalf("write markdown: %v", err)
	}

	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":         "Markdown Video",
			"markdown_path": markdownPath,
			"json_path":     jsonPath,
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	payload, err := BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}

	if payload["script_text"] != "# Title\n\nGenerated script content from markdown." {
		t.Fatalf("want full script_text from markdown (trimmed), got %q", payload["script_text"])
	}
}

func TestBuildPipelinePayload_WithVoiceoverPaths(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{"scenes": [{"text": "Scene 1", "image_link": "https://example.com/scene1.png"}]}`), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	voicePath1 := filepath.Join(tempDir, "voice1.mp3")
	voicePath2 := filepath.Join(tempDir, "voice2.mp3")
	os.WriteFile(voicePath1, []byte("dummy"), 0o644)
	os.WriteFile(voicePath2, []byte("dummy"), 0o644)

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":           "Multi Voice Video",
			"script_text":     "Test script.",
			"json_path":       jsonPath,
			"voiceover_paths": []string{voicePath1, voicePath2},
		},
	}

	payload, err := BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}

	paths, _ := payload["voiceover_paths"].([]string)
	if len(paths) != 2 {
		t.Fatalf("want 2 voiceover_paths, got %d", len(paths))
	}
}

func TestBuildPipelinePayload_MissingVoiceover(t *testing.T) {
	t.Parallel()

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":       "No Voice Video",
			"script_text": "Test script.",
			"scenes_json": `[{"text":"Scene 1","image_link":"https://example.com/img1.png"}]`,
		},
	}

	_, err := BuildPipelinePayload(result)
	if err == nil {
		t.Fatal("want error for missing voiceover, got nil")
	}
}

func TestBuildPipelinePayload_MissingTitle(t *testing.T) {
	t.Parallel()

	voicePath := filepath.Join(t.TempDir(), "voice.mp3")
	os.WriteFile(voicePath, []byte("dummy"), 0o644)

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"script_text": "Test script.",
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	_, err := BuildPipelinePayload(result)
	if err == nil {
		t.Fatal("want error for missing title, got nil")
	}
}

func TestBuildPipelinePayload_MissingScenes(t *testing.T) {
	t.Parallel()

	voicePath := filepath.Join(t.TempDir(), "voice.mp3")
	os.WriteFile(voicePath, []byte("dummy"), 0o644)

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":       "No Scenes Video",
			"script_text": "Test script.",
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	_, err := BuildPipelinePayload(result)
	if err == nil {
		t.Fatal("want error for missing scenes, got nil")
	}
}

func TestBuildPipelinePayload_NilResult(t *testing.T) {
	t.Parallel()

	_, err := BuildPipelinePayload(nil)
	if err == nil {
		t.Fatal("want error for nil result, got nil")
	}
}

func TestBuildPipelinePayload_FlatResult(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{"scenes": [{"text": "Scene 1", "image_link": "https://example.com/scene1.png"}]}`), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	// Flat result without nested "result" key
	result := map[string]interface{}{
		"status":         "completed",
		"title":          "Flat Video",
		"script_text":    "Flat script text.",
		"json_path":      jsonPath,
		"voiceover_path": voicePath,
	}

	payload, err := BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}

	if payload["video_name"] != "Flat Video" {
		t.Fatalf("want video_name 'Flat Video', got %v", payload["video_name"])
	}
}

// =============================================================================
// FlattenPipelineResult tests
// =============================================================================

func TestFlattenPipelineResult_Nested(t *testing.T) {
	t.Parallel()

	result := map[string]interface{}{
		"ok":     true,
		"status": "completed",
		"result": map[string]interface{}{
			"title": "Nested Title",
			"text":  "Nested text.",
		},
	}

	flat := FlattenPipelineResult(result)
	if flat["title"] != "Nested Title" {
		t.Fatalf("want title from nested result, got %v", flat["title"])
	}
	if flat["text"] != "Nested text." {
		t.Fatalf("want text from nested result, got %v", flat["text"])
	}
	if flat["ok"] != true {
		t.Fatalf("want ok from top-level, got %v", flat["ok"])
	}
}

func TestFlattenPipelineResult_Flat(t *testing.T) {
	t.Parallel()

	result := map[string]interface{}{
		"ok":    true,
		"title": "Flat Title",
	}

	flat := FlattenPipelineResult(result)
	if flat["title"] != "Flat Title" {
		t.Fatalf("want title, got %v", flat["title"])
	}
	if len(flat) != 2 {
		t.Fatalf("want 2 keys, got %d", len(flat))
	}
}

// =============================================================================
// ShouldForwardPipelineResult tests
// =============================================================================

func TestShouldForwardPipelineResult_Completed(t *testing.T) {
	t.Parallel()

	voicePath := filepath.Join(t.TempDir(), "voice.mp3")
	os.WriteFile(voicePath, []byte("dummy"), 0o644)

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"scenes_json":    `[{"text":"Scene 1","image_link":"https://example.com/img1.png"}]`,
			"voiceover_path": voicePath,
		},
	}

	if !ShouldForwardPipelineResult(result) {
		t.Fatal("want true for completed result with scenes and voiceover")
	}
}

func TestShouldForwardPipelineResult_FailedStatus(t *testing.T) {
	t.Parallel()

	result := map[string]interface{}{
		"status": "failed",
		"result": map[string]interface{}{
			"scenes_json":    `[{"text":"Scene 1","image_link":"https://example.com/img1.png"}]`,
			"voiceover_path": "/tmp/voice.mp3",
		},
	}

	if ShouldForwardPipelineResult(result) {
		t.Fatal("want false for failed status")
	}
}

func TestShouldForwardPipelineResult_NoScenes(t *testing.T) {
	t.Parallel()

	voicePath := filepath.Join(t.TempDir(), "voice.mp3")
	os.WriteFile(voicePath, []byte("dummy"), 0o644)

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"voiceover_path": voicePath,
		},
	}

	if ShouldForwardPipelineResult(result) {
		t.Fatal("want false when scenes missing")
	}
}

func TestShouldForwardPipelineResult_NoVoiceover(t *testing.T) {
	t.Parallel()

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"scenes_json": `[{"text":"Scene 1","image_link":"https://example.com/img1.png"}]`,
		},
	}

	if ShouldForwardPipelineResult(result) {
		t.Fatal("want false when voiceover missing")
	}
}

func TestShouldForwardPipelineResult_Nil(t *testing.T) {
	t.Parallel()

	if ShouldForwardPipelineResult(nil) {
		t.Fatal("want false for nil")
	}
}

// =============================================================================
// RenderJobResponse tests
// =============================================================================

func TestRenderJobResponse_Basic(t *testing.T) {
	t.Parallel()

	job := map[string]interface{}{
		"job_id":              "job-123",
		"status":              "COMPLETED",
		"video_name":          "My Video",
		"job_run_id":          "run-456",
		"run_id":              "run-456",
		"created_at":          "2026-01-01T00:00:00Z",
		"updated_at":          "2026-01-01T01:00:00Z",
		"started_at":          "2026-01-01T00:30:00Z",
		"completed_at":        "2026-01-01T01:00:00Z",
		"output_path":         "/videos/my-video.mp4",
		"drive_output_folder": "https://drive.google.com/drive/folders/folder123",
		"scene_count":         5,
		"voiceover_count":     3,
		"video_mode":          "scene_image",
	}

	response := RenderJobResponse(job, false)
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}
	if response["job_id"] != "job-123" {
		t.Fatalf("want job_id job-123, got %v", response["job_id"])
	}
	if response["script_id"] != "job-123" {
		t.Fatalf("want script_id job-123, got %v", response["script_id"])
	}
	if response["status"] != "COMPLETED" {
		t.Fatalf("want status COMPLETED, got %v", response["status"])
	}
	if response["video_name"] != "My Video" {
		t.Fatalf("want video_name, got %v", response["video_name"])
	}
	if response["scene_count"] != 5 {
		t.Fatalf("want scene_count 5, got %v", response["scene_count"])
	}
	if response["voiceover_count"] != 3 {
		t.Fatalf("want voiceover_count 3, got %v", response["voiceover_count"])
	}
	if response["video_mode"] != "scene_image" {
		t.Fatalf("want video_mode scene_image, got %v", response["video_mode"])
	}

	// Full should be false — no "job" or "request" keys
	if _, hasJob := response["job"]; hasJob {
		t.Fatal("want no 'job' key when full=false")
	}
	if _, hasReq := response["request"]; hasReq {
		t.Fatal("want no 'request' key when full=false")
	}
}

func TestRenderJobResponse_Full(t *testing.T) {
	t.Parallel()

	job := map[string]interface{}{
		"job_id":     "job-123",
		"status":     "PENDING",
		"video_name": "Full Video",
		"request":    map[string]interface{}{"raw": "data"},
	}

	response := RenderJobResponse(job, true)
	if response["job"] == nil {
		t.Fatal("want 'job' key when full=true")
	}
	if response["request"] == nil {
		t.Fatal("want 'request' key when full=true")
	}
}

func TestRenderJobResponse_Nil(t *testing.T) {
	t.Parallel()

	response := RenderJobResponse(nil, false)
	if response["ok"] != false {
		t.Fatalf("want ok=false for nil job, got %v", response["ok"])
	}
}

func TestRenderJobResponse_WithError(t *testing.T) {
	t.Parallel()

	job := map[string]interface{}{
		"job_id": "job-err",
		"status": "FAILED",
		"error":  "something went wrong",
	}

	response := RenderJobResponse(job, false)
	if response["error"] != "something went wrong" {
		t.Fatalf("want error, got %v", response["error"])
	}
}

func TestRenderJobResponse_WithResult(t *testing.T) {
	t.Parallel()

	job := map[string]interface{}{
		"job_id": "job-res",
		"status": "COMPLETED",
		"result": map[string]interface{}{
			"output_video": "/videos/output.mp4",
		},
	}

	response := RenderJobResponse(job, false)
	if response["result"] == nil {
		t.Fatal("want result key")
	}
}

func TestRenderJobResponse_EmptyJob(t *testing.T) {
	t.Parallel()

	response := RenderJobResponse(map[string]interface{}{}, false)
	if response["ok"] != true {
		t.Fatalf("want ok=true for empty job, got %v", response["ok"])
	}
	if response["job_id"] != "" {
		t.Fatalf("want empty job_id, got %v", response["job_id"])
	}
}

// =============================================================================
// FlattenPipelineResult more tests
// =============================================================================

func TestFlattenPipelineResult_NestedDoesntOverwrite(t *testing.T) {
	t.Parallel()

	// Top-level keys are copied first, then nested keys — nested overwrites matching keys.
	result := map[string]interface{}{
		"title": "Top Level Title",
		"result": map[string]interface{}{
			"title": "Nested Title",
		},
	}

	flat := FlattenPipelineResult(result)
	// Nested result is iterated after top-level, so nested value wins
	if flat["title"] != "Nested Title" {
		t.Fatalf("want nested title to win (nested iterated last), got %v", flat["title"])
	}
}

// =============================================================================
// buildScriptText helper test (via BuildSceneImagePayload)
// =============================================================================

func TestBuildSceneImagePayload_FallbackScriptText(t *testing.T) {
	t.Parallel()

	// No source_text, no script — script text is built from topic + source_text fallback
	payload := map[string]interface{}{
		"video_name":     "Fallback Script",
		"topic":          "Interesting Topic",
		"voiceover_path": "/tmp/voice.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/img1.png"},
		},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	scriptText, _ := result["script_text"].(string)
	if scriptText == "" {
		t.Fatal("want non-empty script_text")
	}
}

// =============================================================================
// Scene JSON validation tests
// =============================================================================

func TestNormalizeScenesPayload_InvalidScenesJSON(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"scenes_json": "not valid json",
	}

	_, _, err := NormalizeScenesPayload(payload)
	if err == nil {
		t.Fatal("want error for invalid scenes_json, got nil")
	}
}

func TestBuildPipelinePayload_InvalidScenesJSONFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "invalid.json")
	if err := os.WriteFile(jsonPath, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	voicePath := filepath.Join(tempDir, "voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("dummy"), 0o644); err != nil {
		t.Fatalf("write voiceover: %v", err)
	}

	result := map[string]interface{}{
		"status": "completed",
		"result": map[string]interface{}{
			"title":       "Invalid JSON Video",
			"script_text": "Test script.",
			"json_path":   jsonPath,
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	_, err := BuildPipelinePayload(result)
	if err == nil {
		t.Fatal("want error for invalid JSON file, got nil")
	}
}

// =============================================================================
// Round-trip test: BuildSceneImagePayload → JSON marshal/unmarshal
// =============================================================================

func TestBuildSceneImagePayload_RoundTrip(t *testing.T) {
	t.Parallel()

	payload := map[string]interface{}{
		"video_name":          "Round Trip Test",
		"source_text":         "Round trip script.",
		"voiceover_path":      "/tmp/voice.mp3",
		"drive_output_folder": "https://drive.google.com/drive/folders/roundtrip",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene 1",
				"image_link": "https://example.com/img1.png",
			},
			map[string]interface{}{
				"text":       "Scene 2",
				"image_link": "https://example.com/img2.png",
			},
		},
	}

	result, err := BuildSceneImagePayload(payload, t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("BuildSceneImagePayload: %v", err)
	}

	// Serialize and deserialize to verify JSON compatibility
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Verify key fields survived round-trip
	if decoded["video_name"] != "Round Trip Test" {
		t.Fatalf("round-trip video_name mismatch: %v", decoded["video_name"])
	}
	if decoded["job_type"] != "process_video" {
		t.Fatalf("round-trip job_type mismatch: %v", decoded["job_type"])
	}
}
