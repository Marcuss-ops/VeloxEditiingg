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
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// PR-04.5 + PR #8: job creation is now canonical through AtomicJobTaskCreator.
// The legacy testSubmitQueue adapter was removed after Create was dropped
// from jobs.Writer.

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
			// PR15.6 canonical-key rename: builder emits video_name (was title).
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

	// PR15.6 canonical-only payload: read the canonical `video_name` key.
	// (Legacy `title` is no longer emitted by BuildPipelinePayload.)
	if payload["video_name"] != "Test Video" {
		t.Fatalf("want video_name, got %v", payload["video_name"])
	}
	if payload["script_text"] != "This is the generated script." {
		t.Fatalf("want script_text, got %v", payload["script_text"])
	}
	if payload["scenes_json"] == "" {
		t.Fatalf("want scenes_json, got empty")
	}
	// PR15.6 canonical-only payload: on disk + on the wire voiceover is
	// `voiceover_paths` (a slice); singular `voiceover_path` is the legacy
	// alias that the HTTP-edge adapter reads from old rows only.
	// BuildPipelinePayload returns the native []string it constructed; an
	// HTTP-edge JSON round-trip would surface []interface{} instead, so
	// accept both shapes.
	var vpFirst interface{}
	switch v := payload["voiceover_paths"].(type) {
	case []string:
		if len(v) > 0 {
			vpFirst = v[0]
		}
	case []interface{}:
		if len(v) > 0 {
			vpFirst = v[0]
		}
	default:
		t.Fatalf("want voiceover_paths to be []string or []interface{}, got %T (%v)", payload["voiceover_paths"], payload["voiceover_paths"])
	}
	if vpFirst != voicePath {
		t.Fatalf("want voiceover path %q at voiceover_paths[0], got %v", voicePath, payload["voiceover_paths"])
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

	// PR-DI-pipeline: build the Handlers via the constructor surface
	// instead of mutating package-level globals (the previous
	// remoteEngineClient/pipelineEnqueuer/InitRemoteEngine/InitPipelineEnqueuer).
	remoteClient := remoteengine.NewClient(remoteengine.Config{
		URL:       mockEngine.URL,
		TimeoutMS: 5000,
		Retries:   1,
	})

	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	// PR15.7a: previously wired via InitPipelineEnqueuer global;
	// after the refactor it is now an explicit constructor argument.
	testEnqueuer := enqueue.NewEnqueuer(atomic, nil, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()

	pipelineCfg := &config.Config{
		Render: config.RenderConfig{
			RemoteEngineURL:       mockEngine.URL,
			RemoteEngineTimeoutMS: 5000,
			RemoteEngineRetries:   1,
		},
	}
	pipelineHandlers := NewHandlers(pipelineCfg, testEnqueuer, remoteClient)
	r.POST("/api/remote/pipeline/generate", pipelineHandlers.Generate())

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
		j, jobErr := jobRepo.Get(context.Background(), "trace_123")
		if jobErr == nil && j != nil {
			if j.ID != "trace_123" {
				t.Fatalf("want job_id trace_123, got %s", j.ID)
			}
			if j.VideoName != "Test Video" {
				t.Fatalf("want video name Test Video, got %s", j.VideoName)
			}
			if j.RunID == "" {
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
