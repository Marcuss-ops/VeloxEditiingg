package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"velox-server/internal/jobs/enqueue"
)

func TestBuildSceneVideoPayloadFromPipelineResult(t *testing.T) {
	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{
  "scenes": [
    {
      "text": "Scene 1",
      "image": {"asset_id": "scene-image", "url": "velox-asset://scene-image"},
      "voiceover": {"asset_id": "voice", "url": "velox-asset://voice", "duration_ms": 5000}
    }
  ]
}`), 0o644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	markdownPath := filepath.Join(tempDir, "script.md")
	if err := os.WriteFile(markdownPath, []byte("# Script\n\nThis is the generated script."), 0o644); err != nil {
		t.Fatalf("write markdown: %v", err)
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
			"video_name":    "Test Video",
			"script_text":   "This is the generated script.",
			"json_path":     jsonPath,
			"markdown_path": markdownPath,
			"voiceover": map[string]interface{}{
				"local_path": voicePath,
			},
		},
	}

	payload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}

	if payload["video_name"] != "Test Video" {
		t.Fatalf("want video_name, got %v", payload["video_name"])
	}
	if payload["script_text"] != "This is the generated script." {
		t.Fatalf("want script_text, got %v", payload["script_text"])
	}
	if payload["scenes_json"] == "" {
		t.Fatalf("want scenes_json, got empty")
	}
	if _, present := payload["voiceover_paths"]; present {
		t.Fatalf("voiceover_paths must not cross the renderer boundary")
	}

	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(payload["scenes_json"].(string)), &scenes); err != nil {
		t.Fatalf("decode scenes_json: %v", err)
	}
	if len(scenes) != 1 || scenes[0]["voiceover"] == nil {
		t.Fatalf("want canonical nested voiceover scene, got %#v", scenes)
	}
	if payload["job_run_id"] != "trace_123" {
		t.Fatalf("want job_run_id trace_123, got %v", payload["job_run_id"])
	}
	if payload["correlation_id"] != "trace_123" {
		t.Fatalf("want correlation_id trace_123, got %v", payload["correlation_id"])
	}
}
