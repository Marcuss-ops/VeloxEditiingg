package jobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestClaimNextJob_NormalizesTimestampsForWorkers(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLifecycleService(jobRepo, db)
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	querySvc := queue.NewQueryService(db)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db}, ts, querySvc)
	if err != nil {
		t.Fatalf("new file queue: %v", err)
	}

	jobID := "job-normalize-timestamps"
	payload := map[string]interface{}{
		"video_name":  "Timestamp smoke",
		"script_text": "Timestamp smoke",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "/tmp/scene.png"},
		},
		"voiceover_paths": []string{"/tmp/voiceover.wav"},
	}
	if err := q.SubmitJob(context.Background(), jobID, payload); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	svc := &Service{fileQ: q}
	res, err := svc.ClaimNextJob(context.Background(), ClaimRequest{WorkerID: "worker-1"})
	if err != nil {
		t.Fatalf("claim next job: %v", err)
	}
	if res == nil || res.Payload == nil {
		t.Fatalf("expected claim result with payload, got %#v", res)
	}

	createdAt, ok := res.Payload["created_at"].(string)
	if !ok || createdAt == "" {
		t.Fatalf("expected created_at string, got %#v", res.Payload["created_at"])
	}
	if _, err := time.Parse(time.RFC3339, createdAt); err != nil {
		t.Fatalf("created_at should be RFC3339, got %q: %v", createdAt, err)
	}

	updatedAt, ok := res.Payload["updated_at"].(string)
	if !ok || updatedAt == "" {
		t.Fatalf("expected updated_at string, got %#v", res.Payload["updated_at"])
	}
	if _, err := time.Parse(time.RFC3339, updatedAt); err != nil {
		t.Fatalf("updated_at should be RFC3339, got %q: %v", updatedAt, err)
	}
}
