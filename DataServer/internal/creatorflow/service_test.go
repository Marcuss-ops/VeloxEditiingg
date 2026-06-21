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

	"velox-server/internal/jobs"
	jobenqueue "velox-server/internal/jobs/enqueue"
	"velox-server/internal/platform/clock"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// testSubmitQueue implements enqueue.JobQueue by delegating to jobs.Writer.
type testSubmitQueue struct {
	writer     jobs.Writer
	maxRetries int
}

func (q *testSubmitQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	raw, _ := json.Marshal(payload)
	job := &jobs.Job{
		ID:         jobID,
		Status:     jobs.StatusPending,
		MaxRetries: q.maxRetries,
		Payload:    string(raw),
	}
	return q.writer.Create(ctx, job)
}

// PR15.7a: both tests construct svc literal with enqueuer field, no queue
// field. The Enqueuer owns the queue; this removes duplicate references
// that previously could drift.

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
	ts, tsErr := jobs.NewLifecycleService(jobRepo, clock.System{})
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	_ = ts

	// PR15.7a: build the Enqueuer once. No voiceover rewrite expected on
	// this path (nil voiceover); the rewrite is a no-op.
	enqueuer := jobenqueue.NewEnqueuer(&testSubmitQueue{writer: jobRepo, maxRetries: 3}, nil)

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
		enqueuer: enqueuer,
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
		j, jobErr := jobRepo.Get(context.Background(), "creator-async-1")
		if jobErr == nil && j != nil {
			if j.ID != "creator-async-1" {
				t.Fatalf("want worker job_id creator-async-1, got %s", j.ID)
			}
			if j.VideoName != "Async Creator Video" {
				t.Fatalf("want Async Creator Video, got %s", j.VideoName)
			}
			payload := jobs.ToPayloadMap(j)
			// PR15.6: voiceover_paths is canonical.
			vp, _ := payload["voiceover_paths"].([]string)
			if len(vp) == 0 || vp[0] != voicePath {
				t.Fatalf("want voiceover_paths[0] %q, got %v", voicePath, vp)
			}
			if _, present := payload["voiceover_path"]; present {
				t.Fatalf("voiceover_path alias must NOT be present in canonical creator payload, got %v", payload["voiceover_path"])
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
	ts, tsErr := jobs.NewLifecycleService(jobRepo, clock.System{})
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	_ = ts

	// PR15.7a: ForwardCompletedResult takes *enqueue.Enqueuer (not raw q).
	// The free function constructs a temporary Enqueuer internally, but
	// the new contract takes the enqueuer directly so the test mirrors
	// the production call site.
	enqueuer := jobenqueue.NewEnqueuer(&testSubmitQueue{writer: jobRepo, maxRetries: 3}, nil)

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

	response, err := ForwardCompletedResult(context.Background(), enqueuer, result)
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

	j, jobErr := jobRepo.Get(context.Background(), "creator-complete-1")
	if jobErr != nil {
		t.Fatalf("Get: %v", jobErr)
	}
	if j == nil {
		t.Fatalf("want job")
	}
	if j.ID != "creator-complete-1" {
		t.Fatalf("want job_id creator-complete-1, got %s", j.ID)
	}
	if j.VideoName != "Creator Video" {
		t.Fatalf("want video name Creator Video, got %s", j.VideoName)
	}
	// PR15.6: drop the legacy `run_id` JSON tag assertion. The queue Job
	// struct still maps RunID from the `run_id` alias (deferred to PR15.5
	// jobs.Writer canonicalization). The canonical key is `job_run_id`
	// inside the persisted payload map — assert that instead.
	payload := jobs.ToPayloadMap(j)
	// PR15.6: voiceover_paths is canonical; legacy voiceover_path alias is dropped.
	vp, _ := payload["voiceover_paths"].([]string)
	if len(vp) != 1 || vp[0] != "https://example.com/voice.mp3" {
		t.Fatalf("want voiceover_paths[0] preserved as https://example.com/voice.mp3, got %v", vp)
	}
	if _, present := payload["voiceover_path"]; present {
		t.Fatalf("voiceover_path alias must NOT be present in canonical creator payload, got %v", payload["voiceover_path"])
	}
	if payload["submitted_via"] != "api_v1_scene_video" {
		t.Fatalf("want submitted_via api_v1_scene_video, got %v", payload["submitted_via"])
	}
	if payload["source"] != "scene_video_api" {
		t.Fatalf("want source scene_video_api, got %v", payload["source"])
	}
}
