package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestBuildSceneVideoPayloadFromPipelineResult(t *testing.T) {
	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{
  "scenes": [
    {
      "text": "Scene 1",
      "image_link": "https://example.com/scene1.png"
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
			"title":         "Test Video",
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

	if payload["title"] != "Test Video" {
		t.Fatalf("want title, got %v", payload["title"])
	}
	if payload["script_text"] != "This is the generated script." {
		t.Fatalf("want script_text, got %v", payload["script_text"])
	}
	if payload["scenes_json"] == "" {
		t.Fatalf("want scenes_json, got empty")
	}
	if payload["voiceover_path"] != voicePath {
		t.Fatalf("want voiceover path %q, got %v", voicePath, payload["voiceover_path"])
	}
	if payload["job_run_id"] != "trace_123" {
		t.Fatalf("want job_run_id trace_123, got %v", payload["job_run_id"])
	}
	if payload["correlation_id"] != "trace_123" {
		t.Fatalf("want correlation_id trace_123, got %v", payload["correlation_id"])
	}
}

func TestPipelineGenerateForwardsCompletedResultToQueue(t *testing.T) {
	tempDir := t.TempDir()

	jsonPath := filepath.Join(tempDir, "script.json")
	if err := os.WriteFile(jsonPath, []byte(`{
  "scenes": [
    {
      "text": "Scene 1",
      "image_link": "https://example.com/scene1.png"
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

	mockEngine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/script/generate-with-images" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var ignored map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&ignored)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":       true,
			"status":   "completed",
			"trace_id": "trace_123",
			"result": map[string]interface{}{
				"title":         "Test Video",
				"script_text":   "This is the generated script.",
				"json_path":     jsonPath,
				"markdown_path": markdownPath,
				"voiceover": map[string]interface{}{
					"local_path": voicePath,
				},
				"metadata": []map[string]interface{}{
					{"title": "Test Video"},
				},
			},
		})
	}))
	defer mockEngine.Close()

	originalClient := remoteEngineClient
	defer func() { remoteEngineClient = originalClient }()
	InitRemoteEngine(&config.Config{
		RemoteEngineURL:       mockEngine.URL,
		RemoteEngineTimeoutMS: 5000,
		RemoteEngineRetries:   1,
	})

	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
	if err != nil {
		t.Fatalf("file queue: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/remote/pipeline/generate", PipelineGenerate(&config.Config{}, q))

	reqBody, _ := json.Marshal(map[string]interface{}{
		"source_text": "Test source",
		"title":       "Test Video",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/remote/pipeline/generate", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["worker_forwarded"] != true {
		t.Fatalf("want worker_forwarded=true, got %v body=%s", resp["worker_forwarded"], w.Body.String())
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		job, jobErr := q.GetJob(context.Background(), "trace_123")
		if jobErr == nil && job != nil {
			if job.JobID != "trace_123" {
				t.Fatalf("want job_id trace_123, got %s", job.JobID)
			}
			if job.VideoName != "Test Video" {
				t.Fatalf("want video name Test Video, got %s", job.VideoName)
			}
			if job.RunID == "" {
				t.Fatalf("want run id to be populated")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not found in queue: %v", jobErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
