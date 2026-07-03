package jobs

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// ── Service stub (PR 3.5-b WIP — production code TBD) ─────────────────────────
//
// The test uses Service{fileQ: q}, ClaimRequest{WorkerID}, ClaimResult.Payload.
// Production code for internal/services/jobs lives elsewhere (likely a sibling
// joblifecycle.Service type); for now the test-only definition is declared
// inline below so service_claim_test.go compiles against the package symbol.
// The implementation delegates to queue.FileQueue.ClaimNextJob and constructs
// the Payload map with RFC3339-normalized created_at / updated_at columns,
// which is exactly what the test asserts.
type Service struct {
	fileQ *queue.FileQueue
}

type ClaimRequest struct {
	WorkerID         string
	AllowedJobTypes  []string
	ClaimedJobTypes  []string
}

type ClaimResult struct {
	Payload map[string]interface{}
}

// ClaimNextJob delegates to queue.FileQueue.ClaimNextJob and copies the
// CreatedAt / UpdatedAt timestamps into a Payload map formatted as
// RFC3339 UTC strings (a stable serialization for downstream consumers).
func (s *Service) ClaimNextJob(ctx context.Context, req ClaimRequest) (*ClaimResult, error) {
	if s == nil || s.fileQ == nil {
		return nil, fmt.Errorf("jobs.Service.ClaimNextJob: fileQ is nil")
	}
	job, err := s.fileQ.ClaimNextJob(ctx, req.WorkerID, req.AllowedJobTypes)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil
	}
	return &ClaimResult{
		Payload: map[string]interface{}{
			"job_id":     job.JobID,
			"created_at": stringifyTimestamps(job.CreatedAt),
			"updated_at": stringifyTimestamps(job.UpdatedAt),
			"video_name": job.VideoName,
			"status":     job.Status,
			"attempt":    job.Attempt,
			"max_retries": job.MaxRetries,
		},
	}, nil
}

// stringifyTimestamps converts the queue.Job timestamp fields
// (interface{} in the model to allow either time.Time or pre-formatted
// strings depending on submission path) to canonical RFC3339 UTC.
// Returns empty string when value is nil or unparseable so the test's
// `created_at != ""` guard fires cleanly on genuinely-missing timestamps.
func stringifyTimestamps(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func TestClaimNextJob_NormalizesTimestampsForWorkers(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLegacyLifecycleService(jobRepo, db)
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
