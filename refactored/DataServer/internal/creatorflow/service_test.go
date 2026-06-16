package creatorflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
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
						"scenes_json":    `[{"text":"Scene 1","image_link":"https://example.com/scene1.png"}]`,
						"voiceover_path": "https://example.com/voice.mp3",
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
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not enqueued after async poll: %v", jobErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
