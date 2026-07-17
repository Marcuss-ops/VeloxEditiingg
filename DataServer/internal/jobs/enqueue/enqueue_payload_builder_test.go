package enqueue

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// Payload builder tests
// =====================================================================
//
// Verifies that BuildSceneImagePayload / BuildSceneImagePayloadForMaster /
// BuildClipPayloadForMaster / PrepareJobAndTask produce the canonical
// payload (no legacy alias keys like voiceover_path / audio_path / id /
// run_id / title / parameters in V2 writes) AND that the in-memory
// job+spec composition pre-atomically stamps the canonical
// scene.composite.v1 ExecutorID + taskgraph.SpecVersion + the
// RequiredCapabilities contract. Round-trip via JSON Marshal/Unmarshal
// must preserve the canonical surface.

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

func TestBuildSceneImagePayload_ChannelIDPreserved(t *testing.T) {
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
	if result["channel_id"] != "amish" {
		t.Errorf("channel_id: got %v, want amish", result["channel_id"])
	}
	if _, present := result["youtube_group"]; present {
		t.Errorf("youtube_group must NOT be present in canonical V2 writes")
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
	if !ok || len(audioTracks) != 4 {
		t.Fatalf("want 4 audio tracks, got %#v", result["audio_tracks"])
	}
	if got := asFloat(audioTracks[2]["start_time_offset"]); got != 13.0 {
		t.Fatalf("want second voiceover offset 13.0, got %v", got)
	}
	if got := asFloat(audioTracks[3]["start_time_offset"]); got != 26.0 {
		t.Fatalf("want second final clip audio offset 26.0, got %v", got)
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

	for _, alias := range []string{"id", "run_id", "title"} {
		if _, present := result[alias]; present {
			t.Errorf("%s alias must NOT be present in canonical writer, got %v", alias, result[alias])
		}
	}
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
	// PrepareJobAndTask pre-computes MaxRetries from the payload's
	// delivery_plan via extractPlanMaxRetry (so jobs.max_retries
	// reflects the worst-case per-destination budget AT INSERT time).
	// The post-create resolver-based precondition is a consistency
	// check; the in-memory struct can be re-written by
	// enforceDeliveryPlanPrecondition but the DB column value was
	// committed by extractPlanMaxRetry. Payload below has one
	// delivery_plan entry with retry_budget=3, so Prepare-time
	// MaxRetries == 3.
	if job.MaxRetries != 3 {
		t.Errorf("job.MaxRetries = %d, want 3 (PrepareJobAndTask extracts it from payload delivery_plan; precondition validates post-create)", job.MaxRetries)
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
