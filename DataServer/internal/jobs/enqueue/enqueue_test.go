package enqueue

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/deliveries"
	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
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
	// refactor/payload-v2-single-shape: the canonical V2 writer does NOT
	// emit a `parameters` sub-map mirror. Asserting its absence is now
	// the strict invariant — every field reads from top-level only.
	if v, present := result["parameters"]; present {
		t.Errorf("`parameters` sub-map must NOT be present in canonical V2 writes, got %v", v)
	}
	// Legacy alias keys must also NOT leak through top-level writes.
	for _, alias := range []string{"id", "run_id", "title", "voiceover_path", "audio_path"} {
		if _, present := result[alias]; present {
			t.Errorf("%q alias must NOT be present in canonical V2 writer, got %v", alias, result[alias])
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
	images, _ := result["images"].([]string)
	if len(images) != 1 || images[0] != srcImage {
		t.Fatalf("want images [%q], got %v", srcImage, images)
	}
	items, _ := result["items"].([]map[string]interface{})
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if got, _ := items[0]["url"].(string); got != srcImage {
		t.Fatalf("want item url %q, got %q", srcImage, got)
	}
	if got, _ := result["pipeline_id"].(string); got != "hybrid.v1" {
		t.Fatalf("want pipeline_id hybrid.v1, got %q", got)
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
	images, _ := result["images"].([]string)
	if len(images) != 1 || images[0] != sceneURL {
		t.Fatalf("want images [%q], got %v", sceneURL, images)
	}
	items, _ := result["items"].([]map[string]interface{})
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if got, _ := items[0]["url"].(string); got != sceneURL {
		t.Fatalf("want item url %q, got %q", sceneURL, got)
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

func TestBuildClipPayloadForMaster_UsesDetectedVoiceoverDurationForOffsets(t *testing.T) {
	tempDir := t.TempDir()
	stubDir := filepath.Join(tempDir, "bin")
	if err := os.MkdirAll(stubDir, 0o755); err != nil {
		t.Fatal(err)
	}

	ffprobeStub := filepath.Join(stubDir, "ffprobe")
	stub := `#!/bin/sh
last=""
for arg in "$@"; do
  last="$arg"
done
case "$last" in
  *voice-1.mp3) echo "11" ;;
  *voice-2.mp3) echo "13" ;;
  *) echo "0" ;;
esac
`
	if err := os.WriteFile(ffprobeStub, []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", stubDir+string(os.PathListSeparator)+oldPath)

	payload := map[string]interface{}{
		"video_name": "Narrated Clips",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":                        "Scene 1",
				"duration_seconds":            4.0,
				"final_clip_duration_seconds": 2.0,
				"bindings": map[string]interface{}{
					"voiceover": map[string]interface{}{"link": "https://example.com/voice-1.mp3"},
					"stock":     map[string]interface{}{"drive_link": "https://example.com/stock-1.mp4"},
					"clip":      map[string]interface{}{"drive_link": "https://example.com/clip-1.mp4"},
				},
			},
			map[string]interface{}{
				"text":                        "Scene 2",
				"duration_seconds":            4.0,
				"final_clip_duration_seconds": 3.0,
				"bindings": map[string]interface{}{
					"voiceover": map[string]interface{}{"link": "https://example.com/voice-2.mp3"},
					"stock":     map[string]interface{}{"drive_link": "https://example.com/stock-2.mp4"},
					"clip":      map[string]interface{}{"drive_link": "https://example.com/clip-2.mp4"},
				},
			},
		},
	}

	result, err := BuildClipPayloadForMaster(payload, tempDir, filepath.Join(tempDir, "videos"), "")
	if err != nil {
		t.Fatalf("BuildClipPayloadForMaster: %v", err)
	}

	items, ok := result["items"].([]map[string]interface{})
	if !ok || len(items) != 4 {
		t.Fatalf("want 4 items, got %#v", result["items"])
	}
	if got := asFloat(items[0]["duration"]); got != 11.0 {
		t.Fatalf("want first narrated bed duration 11.0, got %v", got)
	}
	if got := asFloat(items[1]["duration"]); got != 2.0 {
		t.Fatalf("want first final clip duration 2.0, got %v", got)
	}
	if got := asFloat(items[2]["duration"]); got != 13.0 {
		t.Fatalf("want second narrated bed duration 13.0, got %v", got)
	}

	audioTracks, ok := result["audio_tracks"].([]map[string]interface{})
	if !ok || len(audioTracks) != 2 {
		t.Fatalf("want 2 audio tracks, got %#v", result["audio_tracks"])
	}
	if got := asFloat(audioTracks[1]["start_time_offset"]); got != 13.0 {
		t.Fatalf("want second voiceover offset 13.0, got %v", got)
	}
}

func asFloat(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

func TestPrepareJobAndTask_UsesCanonicalSceneCompositeExecutorID(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	enq := NewEnqueuer(store.NewAtomicJobTaskCreator(db), nil, nil, newTestPlanResolver())

	job, spec, _, err := enq.PrepareJobAndTask(context.Background(), map[string]interface{}{
		"video_name":     "Jackie Chan",
		"script_text":    "test",
		"voiceover_path": "/tmp/voice.mp3",
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
		"scenes": []interface{}{
			map[string]interface{}{"text": "S1", "clip_link": "https://example.com/c1.mp4"},
		},
	}, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("PrepareJobAndTask: %v", err)
	}
	if job == nil || spec == nil {
		t.Fatal("expected non-nil job and spec")
	}
	// PR-delivery-plan-precondition: the happy-path mock returns
	// RetryBudget=5 so the precondition must propagate that to job.MaxRetries.
	// This is a one-line proof that the move-into-PrepareJobAndTask
	// does not break the existing happy-path tests.
	if job.MaxRetries != 5 {
		t.Errorf("job.MaxRetries = %d, want 5 (from newTestPlanResolver happy-path mock)", job.MaxRetries)
	}
	if spec.ExecutorID != "scene.composite.v1" {
		t.Fatalf("spec.ExecutorID = %q, want %q", spec.ExecutorID, "scene.composite.v1")
	}
	if spec.Version != taskgraph.SpecVersion {
		t.Fatalf("spec.Version = %d, want %d", spec.Version, taskgraph.SpecVersion)
	}
	if len(spec.RequiredCapabilities) != 1 || spec.RequiredCapabilities[0] != "artifact.commit.v1" {
		t.Fatalf("RequiredCapabilities = %v, want [artifact.commit.v1]", spec.RequiredCapabilities)
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
// for integration-level testing of the atomic creation path. The
// PlanResolver is the happy-path mock so non-precondition tests do not
// have to configure it. Precondition tests use a custom mockPlanResolver
// directly via NewEnqueuer.
func newTestEnqueuer(t *testing.T) *Enqueuer {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	return NewEnqueuer(atomic, jobRepo, nil, newTestPlanResolver())
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
		},		"voiceover_paths": []string{"/tmp/v1.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
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
		},		"voiceover_paths": []string{"/tmp/v1.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
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

// TestDeriveForwardingJobID_Idempotency verifies that the deterministic
// forwarding key always produces the same job ID.
func TestDeriveForwardingJobID_Idempotency(t *testing.T) {
	t.Parallel()
	key := "remote_engine:creator-job-123:scene.composite.v1"
	id1 := DeriveForwardingJobID(key)
	id2 := DeriveForwardingJobID(key)
	if id1 != id2 {
		t.Errorf("same key should produce same ID: %q != %q", id1, id2)
	}
	if id1 == "" {
		t.Error("job ID should not be empty")
	}
	if !strings.HasPrefix(id1, "job_") {
		t.Errorf("job ID should start with job_: %q", id1)
	}
}

func TestDeriveForwardingJobID_DifferentKeys(t *testing.T) {
	t.Parallel()
	id1 := DeriveForwardingJobID("openai:job-1:scene.composite.v1")
	id2 := DeriveForwardingJobID("openai:job-2:scene.composite.v1")
	if id1 == id2 {
		t.Errorf("different keys should produce different IDs: both are %q", id1)
	}
}

// =============================================================================
// PR-delivery-plan-precondition: tests for the enqueue-time plan resolver.
// The mockPlanResolver is a hand-rolled PlanResolver that returns a
// configured plan/error without any DB interaction so the precondition
// can be unit-tested in isolation from the deliveries stack.
// =============================================================================

// mockPlanResolver implements PlanResolver for tests. It returns the
// configured plan or error verbatim, with a defensive copy of the
// destinations slice so tests cannot accidentally mutate shared state.
type mockPlanResolver struct {
	plan *ResolvedPlan
	err  error
}

func (m *mockPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*ResolvedPlan, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: m.plan.JobID}
	out.Destinations = append(out.Destinations, m.plan.Destinations...)
	return out, nil
}

// newTestPlanResolver returns a happy-path PlanResolver (single
// destination, retry_budget=5) used by the existing non-precondition
// tests. Precondition tests construct their own mockPlanResolver to
// exercise the rejection paths.
func newTestPlanResolver() PlanResolver {
	return &mockPlanResolver{
		plan: &ResolvedPlan{
			JobID: "test-job",
			Destinations: []PlanDestination{
				{DestinationID: "destination-main", Priority: 0, RetryBudget: 5},
			},
		},
	}
}

// TestNewEnqueuer_PanicsOnNilPlanResolver verifies the fail-fast invariant
// for misconfiguration: a nil PlanResolver must panic at construction so
// the gap is caught at boot, not on the first production enqueue.
func TestNewEnqueuer_PanicsOnNilPlanResolver(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewEnqueuer(nil PlanResolver) should panic, did not")
		}
	}()
	_ = NewEnqueuer(nil, nil, nil, nil)
}

// TestEnqueue_Precondition_RejectsMissingPlan verifies that an enqueue is
// rejected when the PlanResolver returns an error (e.g. ErrNoExplicitPlan
// from the real SQLiteDeliveryPlanResolver). The atomic create must NOT
// happen and the error must surface a clear "delivery_plan" hint.
func TestEnqueue_Precondition_RejectsMissingPlan(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{err: errors.New("deliveries: no explicit delivery plan and global fallback is disabled: job_id=test")},
	)

	payload := map[string]interface{}{
		"video_name":      "no-plan",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
	}
	_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("want error when plan missing, got nil")
	}
	if !strings.Contains(err.Error(), "delivery_plan") {
		t.Errorf("want error to mention delivery_plan, got %v", err)
	}
}

// TestEnqueue_Precondition_RejectsEmptyDestinations verifies that an
// enqueue is rejected when the plan has zero destinations (treated as
// "no explicit plan").
func TestEnqueue_Precondition_RejectsEmptyDestinations(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "test"}},
	)

	payload := map[string]interface{}{
		"video_name":      "empty-dest",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}
	_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("want error when destinations empty, got nil")
	}
	if !strings.Contains(err.Error(), "no explicit delivery plan") {
		t.Errorf("want error to mention missing plan, got %v", err)
	}
}

// TestEnqueue_Precondition_RejectsZeroRetryBudget verifies that an
// enqueue is rejected when any destination has retry_budget <= 0. The
// per-delivery delivery_plan_payload.go validator already rejects at
// parse time; this is the runtime counterpart at enqueue time.
func TestEnqueue_Precondition_RejectsZeroRetryBudget(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "d1", Priority: 0, RetryBudget: 5},
				{DestinationID: "d2", Priority: 1, RetryBudget: 0}, // INVALID
			},
		}},
	)

	payload := map[string]interface{}{
		"video_name":      "zero-budget",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}
	_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("want error when retry_budget=0, got nil")
	}
	if !strings.Contains(err.Error(), "retry_budget") {
		t.Errorf("want error to mention retry_budget, got %v", err)
	}
	if !strings.Contains(err.Error(), "must be > 0") {
		t.Errorf("want error to mention 'must be > 0', got %v", err)
	}
}

// TestEnqueue_Precondition_PropagatesMaxRetryBudget verifies that the
// Job's MaxRetries is set to the max retry_budget across destinations
// so the job-level budget can cover the worst-case per-destination
// retry chain. Per-delivery retry_budget is still authoritative at
// INSERT time (see deliveries/runner.go: lease carries per-row value).
func TestEnqueue_Precondition_PropagatesMaxRetryBudget(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "d1", Priority: 0, RetryBudget: 3},
				{DestinationID: "d2", Priority: 1, RetryBudget: 7}, // max
				{DestinationID: "d3", Priority: 2, RetryBudget: 5},
			},
		}},
	)

	payload := map[string]interface{}{
		"video_name":      "max-retry",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	jobID, _ := response["job_id"].(string)
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Get job: %v", err)
	}
	if j.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7 (max of [3, 7, 5])", j.MaxRetries)
	}
}

// TestEnqueueWithForwardingKey verifies that when a payload carries
// _internal_forwarding_key, the job_id is deterministic.
func TestEnqueueWithForwardingKey(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":             "Forwarded Video",
		"script_text":            "forwarded script",
		routing.KeyForwardingKey: "remote_engine:creator-forward-1:scene.composite.v1",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		},
		"voiceover_paths": []string{"/tmp/v-forward.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
	}

	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	jobID, _ := response["job_id"].(string)
	expected := DeriveForwardingJobID("remote_engine:creator-forward-1:scene.composite.v1")
	if jobID != expected {
		t.Errorf("forwarding job_id = %q, want deterministic %q", jobID, expected)
	}

	// Second enqueue with same forwarding key should be idempotent.
	response2, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue (retry): %v", err)
	}
	jobID2, _ := response2["job_id"].(string)
	if jobID2 != jobID {
		t.Errorf("retry job_id = %q, want same %q", jobID2, jobID)
	}
}

// =============================================================================
// PR-delivery-plan-precondition: integration test that uses the REAL
// DB-backed *deliveries.SQLiteDeliveryPlanResolver (not a hand-rolled
// mock) via a local planResolverAdapter. This proves the precondition
// reads from the real job_delivery_plans table, propagates max(retry_budget)
// to job.MaxRetries, and surfaces the production ErrNoExplicitPlan path
// when no plan rows exist. The adapter mirrors the production one in
// cmd/server/bootstrap_modules.go to avoid an import cycle between
// enqueue and deliveries.
// =============================================================================

// planResolverAdapter bridges *deliveries.SQLiteDeliveryPlanResolver to
// enqueue.PlanResolver for the integration test. It is the test-side
// twin of deliveryPlanResolverAdapter in cmd/server/bootstrap_modules.go;
// the duplication is intentional (composition-root adapter for prod, local
// adapter for the test) to keep the enqueue package decoupled from
// deliveries.
type planResolverAdapter struct {
	inner *deliveries.SQLiteDeliveryPlanResolver
}

func (a *planResolverAdapter) ResolvePlan(ctx context.Context, jobID, artifactID string) (*ResolvedPlan, error) {
	if a == nil || a.inner == nil {
		return nil, nil
	}
	plan, err := a.inner.ResolvePlan(ctx, jobID, artifactID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	out := &ResolvedPlan{JobID: plan.JobID}
	for _, d := range plan.Destinations {
		out.Destinations = append(out.Destinations, PlanDestination{
			DestinationID: d.DestinationID,
			Priority:      d.Priority,
			RetryBudget:   d.RetryBudget,
		})
	}
	return out, nil
}

// TestEnforceDeliveryPlanPrecondition_IntegrationWithRealResolver
// exercises enforceDeliveryPlanPrecondition end-to-end against a real
// SQLite database with explicit job_delivery_plans rows. It validates
// that the precondition:
//
//  1. Reads from job_delivery_plans (NOT the global delivery_destinations
//     fallback) when GlobalFallback is disabled (production mode).
//  2. Propagates max(retry_budget) across destinations to job.MaxRetries.
//  3. Accepts a multi-destination plan with varying retry_budget values.
//
// The test calls enforceDeliveryPlanPrecondition directly (rather than
// going through Enqueue) so the precondition's effect on job.MaxRetries
// is observable without the atomic-create path inserting conflicting
// job_delivery_plans rows of its own.
func TestEnforceDeliveryPlanPrecondition_IntegrationWithRealResolver(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	// Disable FK constraints for this test. The precondition does NOT
	// depend on the job_delivery_plans → jobs / job_delivery_plans →
	// delivery_destinations FKs being enforced: it only reads from
	// job_delivery_plans. Disabling FKs lets the test insert
	// job_delivery_plans rows directly without having to keep a
	// placeholder Job + delivery_destinations rows in sync with the
	// production schema (which has additional NOT NULL columns like
	// delivery_destinations.provider that are irrelevant here). In
	// production, the FKs are enforced and the operator must create the
	// per-job plan rows before enqueueing — that contract is verified
	// by the unit test TestEnqueue_Precondition_RejectsMissingPlan.
	if _, err := db.DB().ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}

	const jobID = "integration-test-job-1"

	// Insert explicit per-job plan rows with varying retry_budget so the
	// precondition's max() calculation has a meaningful signal: 3, 7, 5
	// → MaxRetries must be 7.
	retryBudgets := []struct {
		destID string
		retry  int
	}{
		{"dest-a", 3},
		{"dest-b", 7},
		{"dest-c", 5},
	}
	for _, d := range retryBudgets {
		if _, execErr := db.DB().ExecContext(ctx,
			`INSERT INTO job_delivery_plans (job_id, destination_id, enabled, priority, retry_budget, created_at, updated_at) VALUES (?, ?, 1, 0, ?, ?, ?)`,
			jobID, d.destID, d.retry, now, now,
		); execErr != nil {
			t.Fatalf("insert job_delivery_plan %s: %v", d.destID, execErr)
		}
	}

	// Real DB-backed resolver, production mode (no global fallback).
	realResolver := deliveries.NewSQLiteDeliveryPlanResolver(db.DB(), false)
	adapter := &planResolverAdapter{inner: realResolver}

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		adapter,
	)

	job := &jobs.Job{ID: jobID}
	if preErr := enq.enforceDeliveryPlanPrecondition(ctx, jobID, job); preErr != nil {
		t.Fatalf("enforceDeliveryPlanPrecondition: %v", preErr)
	}
	if job.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7 (max of [3, 7, 5])", job.MaxRetries)
	}
}

// TestEnforceDeliveryPlanPrecondition_IntegrationRejectsMissingPlan
// exercises the production rejection path: GlobalFallback=false (no
// fallback to global delivery_destinations) AND no per-job
// job_delivery_plans rows → the precondition must reject with a
// validation error whose message mentions "delivery_plan" so operators
// know exactly what to do (create the missing plan rows).
func TestEnforceDeliveryPlanPrecondition_IntegrationRejectsMissingPlan(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	ctx := context.Background()
	const jobID = "missing-plan-job-1"

	// Real resolver, production mode. job_delivery_plans is empty for
	// this job_id and GlobalFallback is false, so the precondition must
	// surface deliveries.ErrNoExplicitPlan wrapped in a validationError.
	realResolver := deliveries.NewSQLiteDeliveryPlanResolver(db.DB(), false)
	adapter := &planResolverAdapter{inner: realResolver}

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		adapter,
	)

	job := &jobs.Job{ID: jobID}
	preErr := enq.enforceDeliveryPlanPrecondition(ctx, jobID, job)
	if preErr == nil {
		t.Fatal("want error when plan missing, got nil")
	}
	if !strings.Contains(preErr.Error(), "delivery_plan") {
		t.Errorf("want error to mention delivery_plan, got %v", preErr)
	}
	if !errors.Is(preErr, deliveries.ErrNoExplicitPlan) && !strings.Contains(preErr.Error(), "no explicit delivery plan") {
		t.Errorf("want error to surface ErrNoExplicitPlan or 'no explicit delivery plan', got %v", preErr)
	}
	if job.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d on rejection, want 0 (no propagation when precondition fails)", job.MaxRetries)
	}
}
