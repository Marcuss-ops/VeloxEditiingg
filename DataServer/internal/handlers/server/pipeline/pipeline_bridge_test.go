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

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// Job creation is now canonical through AtomicJobTaskCreator.

// noopPlanResolver is the happy-path PlanResolver for tests that exercise the
// basic enqueue path and do not need to configure delivery-plan rejection. It
// mirrors enqueue.newTestPlanResolver in the enqueue package's own tests.
type noopPlanResolver struct{}

func (noopPlanResolver) ResolvePlan(_ context.Context, _, _ string) (*enqueue.ResolvedPlan, error) {
	return &enqueue.ResolvedPlan{
		JobID: "test-job",
		Destinations: []enqueue.PlanDestination{
			{DestinationID: "destination-main", Priority: 0, RetryBudget: 5},
		},
	}, nil
}

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
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
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
	if _, err := db.DB().Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('drive-main', 'google_drive', 'Drive Main', 1, '{}', datetime('now'), datetime('now'))`); err != nil {
		t.Fatalf("seed delivery destination: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	// PR15.7a: previously wired via InitPipelineEnqueuer global;
	// after the refactor it is now an explicit constructor argument.
	testEnqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})

	// Blocco 5: the pipeline handler now requires a wired creatorflow.Resolver
	// for the forward-completed path. NewHandlers (no resolver) panics at
	// request time; NewHandlersWithResolver is the composition-root form.
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")

	gin.SetMode(gin.TestMode)
	r := gin.New()

	pipelineCfg := &config.Config{
		Render: config.RenderConfig{
			RemoteEngineURL:       mockEngine.URL,
			RemoteEngineTimeoutMS: 5000,
			RemoteEngineRetries:   1,
		},
	}
	pipelineHandlers := NewHandlersWithResolver(pipelineCfg, testEnqueuer, remoteClient, resolver, jobRepo, nil, nil).WithStore(db)
	r.POST("/api/remote/pipeline/generate", pipelineHandlers.Generate())

	reqBody, _ := json.Marshal(map[string]interface{}{
		"source_text": "Test source",
		"title":       "Test Video",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/remote/pipeline/generate", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d body=%s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["pipeline_run_id"] == "" {
		t.Fatalf("want pipeline_run_id in response, got %s", w.Body.String())
	}
	if resp["forwarding_id"] == "" {
		t.Fatalf("want forwarding_id in response, got %s", w.Body.String())
	}
	if resp["remote_job_id"] != "trace_123" {
		t.Fatalf("want remote_job_id trace_123, got %v", resp["remote_job_id"])
	}

	// Area 3: the handler must NOT forward synchronously. The durable
	// CreatorForwardingRunner is responsible for polling and forwarding.
	forwarding, err := db.GetCreatorForwardingBySource(context.Background(), "remote_engine", "trace_123", "scene.composite.v1")
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if forwarding == nil || forwarding.Status != string(store.CFStatusPending) {
		t.Fatalf("want one PENDING forwarding, got %#v", forwarding)
	}

	// No Velox job should have been created yet.
	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("remote_engine", "trace_123", "scene.composite.v1").String(),
	)
	if job, err := jobRepo.Get(context.Background(), expectedJobID); err == nil && job != nil {
		t.Fatalf("did not expect a Velox job to be created synchronously, got %v", job)
	}
}

func TestPipelineGeneratePersistsAsyncForwardingIdempotently(t *testing.T) {
	tempDir := t.TempDir()
	mockEngine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/script/generate-with-images" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":          true,
			"status":      "running",
			"job_id":      "remote-async-1",
			"pipeline_id": "scene.composite.v1",
		})
	}))
	defer mockEngine.Close()

	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	testEnqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")
	client := remoteengine.NewClient(remoteengine.Config{URL: mockEngine.URL, TimeoutMS: 5000, Retries: 1})
	cfg := &config.Config{Render: config.RenderConfig{RemoteEngineURL: mockEngine.URL, RemoteEngineTimeoutMS: 5000, RemoteEngineRetries: 1}}
	h := NewHandlersWithResolver(cfg, testEnqueuer, client, resolver, jobRepo, nil, nil).WithStore(db)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/remote/pipeline/generate", h.Generate())

	request := func() map[string]interface{} {
		reqBody, _ := json.Marshal(map[string]interface{}{"topic": "Async test"})
		req := httptest.NewRequest(http.MethodPost, "/api/remote/pipeline/generate", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusAccepted {
			t.Fatalf("want 202, got %d body=%s", w.Code, w.Body.String())
		}
		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return response
	}

	first := request()
	second := request()
	firstForwardingID, _ := first["forwarding_id"].(string)
	secondForwardingID, _ := second["forwarding_id"].(string)
	if firstForwardingID == "" || firstForwardingID != secondForwardingID {
		t.Fatalf("forwarding ids did not converge: first=%q second=%q", firstForwardingID, secondForwardingID)
	}

	forwarding, err := db.GetCreatorForwardingBySource(context.Background(), "remote_engine", "remote-async-1", "scene.composite.v1")
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if forwarding == nil || forwarding.Status != string(store.CFStatusPending) {
		t.Fatalf("want one PENDING forwarding, got %#v", forwarding)
	}
	var count int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = 'remote_engine' AND source_job_id = 'remote-async-1' AND target_executor_id = 'scene.composite.v1'`).Scan(&count); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if count != 1 {
		t.Fatalf("want exactly one forwarding row, got %d", count)
	}
}
