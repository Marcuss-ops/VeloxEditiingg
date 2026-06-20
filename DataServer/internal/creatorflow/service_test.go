package creatorflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

func TestForwardSchedulesAsyncPollAndWorkerHandoff(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	voicePath := filepath.Join(tempDir, "voice.mp3")
	imagePath := filepath.Join(tempDir, "scene.png")
	if err := os.WriteFile(voicePath, []byte("voice"), 0o644); err != nil {
		t.Fatalf("write voice: %v", err)
	}
	if err := os.WriteFile(imagePath, []byte("image"), 0o644); err != nil {
		t.Fatalf("write image: %v", err)
	}
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLifecycleService(jobRepo, queue.RealClock{})
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	querySvc := queue.NewQueryService(db)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3}, ts, querySvc)
	if err != nil {
		t.Fatalf("file queue: %v", err)
	}

	var mu sync.Mutex
	polls := 0
	mockCreator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/script/generate-with-images":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":       true,
				"status":   "running",
				"trace_id": "creator-async-1",
				"job_id":   "creator-async-1",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/jobs/creator-async-1":
			mu.Lock()
			polls++
			current := polls
			mu.Unlock()

			if current < 2 {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"job": map[string]interface{}{
						"id":       "creator-async-1",
						"status":   "running",
						"progress": 25,
						"result": map[string]interface{}{
							"status": "running",
						},
					},
				})
				return
			}

			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"job": map[string]interface{}{
					"id":       "creator-async-1",
					"status":   "completed",
					"progress": 100,
					"result": map[string]interface{}{
						"title":          "Async Creator Video",
						"script_text":    "Async creator script",
						"scenes_json":    `[{"text":"Scene 1","image_link":"` + imagePath + `"}]`,
						"voiceover_path": voicePath,
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockCreator.Close()

	svc := &Service{
		queue: q,
		client: func() *remoteengine.Client {
			return remoteengine.NewClient(remoteengine.Config{
				URL:       mockCreator.URL,
				TimeoutMS: 5000,
				Retries:   1,
			})
		}(),
		pollInterval: 10 * time.Millisecond,
		dataDir:      tempDir,
		videosDir:    filepath.Join(tempDir, "videos"),
		masterURL:    "http://master.test",
	}

	response, used, err := svc.Forward(context.Background(), map[string]interface{}{
		"topic": "Async Creator",
	})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if !used {
		t.Fatalf("want used=true")
	}
	if response["creator_polling"] != true {
		t.Fatalf("want creator_polling=true, got %v", response["creator_polling"])
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		job, jobErr := q.GetJob(context.Background(), "creator-async-1")
		if jobErr == nil && job != nil {
			if job.JobID != "creator-async-1" {
				t.Fatalf("want worker job_id creator-async-1, got %s", job.JobID)
			}
			if job.VideoName != "Async Creator Video" {
				t.Fatalf("want Async Creator Video, got %s", job.VideoName)
			}
			payload, payloadErr := q.QueryService().GetJobPayload(context.Background(), "creator-async-1")
			if payloadErr != nil {
				t.Fatalf("GetJobPayload: %v", payloadErr)
			}
			if got := payload["voiceover_path"]; got == nil || got.(string) != voicePath {
				t.Fatalf("want voiceover_path %q, got %v", voicePath, got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not enqueued after async poll: %v", jobErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestForwardCompletedResultEnqueuesWorkerJob(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLifecycleService(jobRepo, queue.RealClock{})
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	querySvc := queue.NewQueryService(db)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3}, ts, querySvc)
	if err != nil {
		t.Fatalf("file queue: %v", err)
	}

	result := map[string]interface{}{
		"ok":       true,
		"status":   "completed",
		"trace_id": "creator-complete-1",
		"result": map[string]interface{}{
			"title":          "Creator Video",
			"script_text":    "Creator script",
			"scenes_json":    `[{"text":"Scene 1","image_link":"https://example.com/scene1.png"}]`,
			"voiceover_path": "https://example.com/voice.mp3",
		},
	}

	response, err := ForwardCompletedResult(context.Background(), q, result)
	if err != nil {
		t.Fatalf("ForwardCompletedResult: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}
	if response["job_id"] != "creator-complete-1" {
		t.Fatalf("want job_id creator-complete-1, got %v", response["job_id"])
	}
	if response["status"] != "PENDING" {
		t.Fatalf("want pending response, got %v", response["status"])
	}

	job, jobErr := q.GetJob(context.Background(), "creator-complete-1")
	if jobErr != nil {
		t.Fatalf("GetJob: %v", jobErr)
	}
	if job == nil {
		t.Fatalf("want job")
	}
	if job.JobID != "creator-complete-1" {
		t.Fatalf("want job_id creator-complete-1, got %s", job.JobID)
	}
	if job.VideoName != "Creator Video" {
		t.Fatalf("want video name Creator Video, got %s", job.VideoName)
	}
	if job.RunID != "creator-complete-1" {
		t.Fatalf("want run_id creator-complete-1, got %s", job.RunID)
	}
	payload, payloadErr := q.QueryService().GetJobPayload(context.Background(), "creator-complete-1")
	if payloadErr != nil {
		t.Fatalf("GetJobPayload: %v", payloadErr)
	}
	if payload["submitted_via"] != "api_v1_scene_video" {
		t.Fatalf("want submitted_via api_v1_scene_video, got %v", payload["submitted_via"])
	}
	if payload["source"] != "scene_video_api" {
		t.Fatalf("want source scene_video_api, got %v", payload["source"])
	}
	if payload["voiceover_path"] != "https://example.com/voice.mp3" {
		t.Fatalf("want voiceover_path preserved, got %v", payload["voiceover_path"])
	}
}
