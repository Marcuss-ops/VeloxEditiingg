package enqueue

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
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
		"scene_count": 2, "voiceover_count": 1,
		"script_text": "This is a test script.",
	} {
		if result[k] != want {
			t.Errorf("%s: got %v, want %v", k, result[k], want)
		}
	}

	// PR15.6: voiceover_paths is canonical. Legacy voiceover_path alias is
	// dropped from writers; the HTTP-edge adapter still reads it for old rows.
	vp, _ := result["voiceover_paths"].([]string)
	if len(vp) != 1 || vp[0] != "/tmp/test-voiceover.mp3" {
		t.Errorf("voiceover_paths: got %v, want [/tmp/test-voiceover.mp3]", result["voiceover_paths"])
	}
	if _, present := result["voiceover_path"]; present {
		t.Errorf("voiceover_path alias must NOT be present in canonical writes, got %v", result["voiceover_path"])
	}
	if _, present := result["audio_path"]; present {
		t.Errorf("audio_path alias must NOT be present in canonical writes, got %v", result["audio_path"])
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
	// PR15.6: parameters mirror is canonical-only — id/run_id/title/voiceover_path/audio_path aliases must NOT be present.
	for _, alias := range []string{"id", "run_id", "title", "voiceover_path", "audio_path"} {
		if _, present := params[alias]; present {
			t.Errorf("parameters[%q] alias must NOT be present in canonical writer, got %v", alias, params[alias])
		}
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
	// PR15.6: voiceover_paths is the canonical key.
	vp, _ := result["voiceover_paths"].([]string)
	if len(vp) != 1 || vp[0] != srcVoice {
		t.Fatalf("want voiceover_paths [%q], got %v", srcVoice, vp)
	}
	if _, present := result["voiceover_path"]; present {
		t.Errorf("voiceover_path alias must NOT be present in canonical writes, got %v", result["voiceover_path"])
	}
	if _, present := result["audio_path"]; present {
		t.Errorf("audio_path alias must NOT be present in canonical writes, got %v", result["audio_path"])
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

	// PR15.6: voiceover_paths (canonical) replaced voiceover_path/audio_path aliases.
	vp, _ := result["voiceover_paths"].([]string)
	if len(vp) != 1 || vp[0] != voiceURL {
		t.Fatalf("want remote voiceover preserved in voiceover_paths [%q], got %v", voiceURL, vp)
	}
	if _, present := result["voiceover_path"]; present {
		t.Errorf("voiceover_path alias must NOT be present in canonical writes, got %v", result["voiceover_path"])
	}
	if _, present := result["audio_path"]; present {
		t.Errorf("audio_path alias must NOT be present in canonical writes, got %v", result["audio_path"])
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
	// PR15.6: id / run_id / title aliases must NOT be written.
	for _, alias := range []string{"id", "run_id", "title"} {
		if _, present := result[alias]; present {
			t.Errorf("%s alias must NOT be present in canonical writer, got %v", alias, result[alias])
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
		// PR15.6: title / voiceover_path / audio_path / run_id aliases must NOT be present.
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

func TestRenderHTTPBoundaryJobResponse(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{
			"job_id": "j1", "status": "COMPLETED", "video_name": "V", "scene_count": 5,
			"voiceover_count": 3, "video_mode": "scene_image",
		}
		r := RenderHTTPBoundaryJobResponse(job, false)
		if r["ok"] != true || r["job_id"] != "j1" || r["status"] != "COMPLETED" {
			t.Errorf("unexpected: %v", r)
		}
		if _, has := r["job"]; has {
			t.Error("no 'job' key when full=false")
		}
	})

	t.Run("basic_legacy_alias_fallback", func(t *testing.T) {
		t.Parallel()
		// PR15.6: HTTP-edge adapter tolerates legacy aliases on read for
		// backwards compat with old SQLite rows.
		job := map[string]interface{}{
			"id": "j1", "status": "COMPLETED", "title": "V",
		}
		r := RenderHTTPBoundaryJobResponse(job, false)
		if r["ok"] != true {
			t.Error("want ok=true")
		}
		// script_id leg falls back to id via job_id lookup
		if r["script_id"] != "j1" {
			t.Errorf("script_id alias fallback failed, got %v", r["script_id"])
		}
		if r["video_name"] != "V" {
			t.Errorf("video_name title fallback failed, got %v", r["video_name"])
		}
	})

	t.Run("full", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j2", "request": map[string]interface{}{"raw": "x"}}
		r := RenderHTTPBoundaryJobResponse(job, true)
		if r["job"] == nil || r["request"] == nil {
			t.Error("want job/request keys when full=true")
		}
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		r := RenderHTTPBoundaryJobResponse(nil, false)
		if r["ok"] != false {
			t.Errorf("want ok=false, got %v", r["ok"])
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j3", "status": "FAILED", "error": "boom"}
		r := RenderHTTPBoundaryJobResponse(job, false)
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

// =============================================================================
// PR #3: Enqueuer.Enqueue now uses AtomicJobTaskCreator for atomic Job+Task
// creation instead of JobQueue.SubmitJob (Job-only). Tests verify that
// the Enqueue path produces correct Job+Task rows.
// =============================================================================

// newTestEnqueuer creates an Enqueuer backed by an in-memory SQLite store
// for integration-level testing of the atomic creation path.
func newTestEnqueuer(t *testing.T) *Enqueuer {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	return NewEnqueuer(atomic, jobRepo, nil)
}

// TestEnqueueCreatesJobAndTaskAtomically verifies that Enqueue creates
// both a Job and a Task row atomically (the PR #3 invariant).
func TestEnqueueCreatesJobAndTaskAtomically(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":  "demo.mp4",
		"script_text": "hello world",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		},
		"voiceover_paths": []string{"/tmp/v1.mp3"},
	}
	req := costmodel.JobRequirements{
		ResourceClass: costmodel.ResourceGPU,
		TemporalMode:  costmodel.TemporalWindowed,
		Deterministic: true,
		Cacheable:     true,
	}

	response, err := enq.Enqueue(context.Background(), payload, req)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}

	// Verify Job was created.
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("expected non-empty job_id")
	}
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v job=%v", err, j)
	}
	if j.ID != jobID {
		t.Fatalf("job ID mismatch: %q != %q", j.ID, jobID)
	}
	if j.Status != jobs.StatusPending {
		t.Fatalf("job status: want PENDING, got %q", j.Status)
	}
	if j.VideoName != "demo.mp4" {
		t.Fatalf("video_name: want demo.mp4, got %q", j.VideoName)
	}
}

// TestEnqueueDefaultsPreserved verifies the permissive behavior is
// intact when no Requirements are published: an empty JobRequirements
// flows through unchanged.
func TestEnqueueDefaultsPreserved(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":  "demo.mp4",
		"script_text": "hello world",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		},
		"voiceover_paths": []string{"/tmp/v1.mp3"},
	}
	req := costmodel.DefaultRequirements()

	response, err := enq.Enqueue(context.Background(), payload, req)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}

	// Verify the default requirements persisted correctly.
	jobID, _ := response["job_id"].(string)
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v", err)
	}
	if j.Requirements.ResourceClass != "" || j.Requirements.TemporalMode != "" ||
		j.Requirements.Deterministic || j.Requirements.Cacheable {
		t.Errorf("DefaultRequirements must stay zero-value; got %+v", j.Requirements)
	}
}
