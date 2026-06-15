package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

func setupV2Test(t *testing.T) (*gin.Engine, *queue.FileQueue, *store.SQLiteStore, *jobservice.Service, *workers.Registry) {
	gin.SetMode(gin.TestMode)
	cfg := config.FromEnv()
	db, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("failed to create in-memory store: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
	if err != nil {
		t.Fatalf("failed to create file queue: %v", err)
	}
	reg := workers.New(db)
	svc := jobservice.NewService(cfg, q, store.NewSQLiteJobsRepository(db), nil, reg)

	jobAPI := NewJobAPI(cfg, q, workers.NewTokenManager(), svc, db)

	r := gin.New()
	RegisterV2JobRoutes(r.Group("/api/v1"), cfg, q, db, svc)
	// Register the v1 queue job endpoint (GetJobV2) so the full worker flow can be tested
	r.GET("/api/v1/queue/job", jobAPI.GetJobHandler())

	return r, q, db, svc, reg
}

func createAndClaimJob(t *testing.T, ctx context.Context, q *queue.FileQueue, workerID string) (string, string) {
	payload := map[string]interface{}{
		"video_name": "test-video",
		"job_type":   "process_video",
	}
	now := time.Now()
	jobID := "job-" + now.Format("20060102150405") + "-" + strconv.Itoa(now.Nanosecond())
	if err := q.SubmitJob(ctx, jobID, payload); err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}
	job, err := q.ClaimNextJob(ctx, workerID, nil)
	if err != nil {
		t.Fatalf("failed to claim job: %v", err)
	}
	if job == nil {
		t.Fatal("expected job to be claimed")
	}
	if job.LeaseID == "" {
		t.Fatal("expected lease_id after claim")
	}
	return jobID, job.LeaseID
}

func TestV2SubmitResultSuccess(t *testing.T) {
	r, q, db, _, reg := setupV2Test(t)
	ctx := context.Background()
	workerID := "worker-1"
	if err := reg.RegisterWorker(ctx, workerID, "test-worker", "127.0.0.1", map[string]interface{}{
		"protocol_version": workers.DefaultWorkerProtocolVersion,
		"capabilities":     map[string]interface{}{"video": true},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}
	jobID, leaseID := createAndClaimJob(t, ctx, q, workerID)

	// Insert artifact record
	artifact := &store.Artifact{
		ID:        "artifact_123",
		JobID:     jobID,
		Type:      "video",
		SHA256:    "sha256_abc",
		SizeBytes: 1024,
		Status:    "completed",
	}
	if err := db.InsertArtifact(artifact); err != nil {
		t.Fatalf("failed to insert artifact: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"worker_id":        workerID,
		"lease_id":         leaseID,
		"status":           "completed",
		"artifact_id":      "artifact_123",
		"output_sha256":    "sha256_abc",
		"idempotency_key":  "idem_key_1",
		"attempt":          1,
		"contract_version": 2,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["ok"] != true {
		t.Fatalf("want ok=true, got %v", res["ok"])
	}

	// Verify job was updated with artifact fields and status COMPLETED
	jobMap, err := q.GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if jobMap["status"] != "COMPLETED" {
		t.Fatalf("want status=COMPLETED, got %v", jobMap["status"])
	}
	if jobMap["artifact_id"] != "artifact_123" {
		t.Fatalf("want artifact_id=artifact_123, got %v", jobMap["artifact_id"])
	}
	if jobMap["output_sha256"] != "sha256_abc" {
		t.Fatalf("want output_sha256=sha256_abc, got %v", jobMap["output_sha256"])
	}
	if jobMap["upload_idempotency_key"] != "idem_key_1" {
		t.Fatalf("want idempotency_key=idem_key_1, got %v", jobMap["upload_idempotency_key"])
	}
}

func TestV2SubmitResultLeaseMismatch(t *testing.T) {
	r, q, _, _, reg := setupV2Test(t)
	ctx := context.Background()
	workerID := "worker-1"
	if err := reg.RegisterWorker(ctx, workerID, "test-worker", "127.0.0.1", map[string]interface{}{
		"protocol_version": workers.DefaultWorkerProtocolVersion,
		"capabilities":     map[string]interface{}{"video": true},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}
	jobID, leaseID := createAndClaimJob(t, ctx, q, workerID)
	_ = leaseID

	body, _ := json.Marshal(map[string]interface{}{
		"worker_id": workerID,
		"lease_id":  "wrong-lease",
		"status":    "completed",
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d body=%s", w.Code, w.Body.String())
	}
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["error"] == "" {
		t.Fatal("expected error message")
	}
}

func TestV2SubmitResultJobNotFound(t *testing.T) {
	r, _, _, _, _ := setupV2Test(t)

	body, _ := json.Marshal(map[string]interface{}{
		"worker_id": "worker-1",
		"lease_id":  "lease-123",
		"status":    "completed",
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/nonexistent-job/result", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// TODO: a missing job should ideally return 404, but the current handler returns
	// 409 because ValidateJobLease treats it as a lease error.
	if w.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestV2SubmitResultIdempotency(t *testing.T) {
	r, q, _, _, reg := setupV2Test(t)
	ctx := context.Background()
	workerID := "worker-1"
	if err := reg.RegisterWorker(ctx, workerID, "test-worker", "127.0.0.1", map[string]interface{}{
		"protocol_version": workers.DefaultWorkerProtocolVersion,
		"capabilities":     map[string]interface{}{"video": true},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}
	jobID, leaseID := createAndClaimJob(t, ctx, q, workerID)

	body, _ := json.Marshal(map[string]interface{}{
		"worker_id":       workerID,
		"lease_id":        leaseID,
		"status":          "completed",
		"output_sha256":   "sha256_idem",
		"idempotency_key": "idem_key_dup",
	})

	// First submit
	w1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("first submit: want 200, got %d body=%s", w1.Code, w1.Body.String())
	}

	// Second submit (same idempotency key, job now COMPLETED)
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("second submit: want 200, got %d body=%s", w2.Code, w2.Body.String())
	}

	var res2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &res2); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res2["ok"] != true {
		t.Fatalf("want ok=true on second submit, got %v", res2["ok"])
	}

	// Verify artifact fields are still correct after duplicate submit
	jobMap2, err := q.GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job after idempotent submit: %v", err)
	}
	if jobMap2["status"] != "COMPLETED" {
		t.Fatalf("want status=COMPLETED after idempotent submit, got %v", jobMap2["status"])
	}
	if jobMap2["output_sha256"] != "sha256_idem" {
		t.Fatalf("want output_sha256=sha256_idem after idempotent submit, got %v", jobMap2["output_sha256"])
	}
	if jobMap2["upload_idempotency_key"] != "idem_key_dup" {
		t.Fatalf("want idempotency_key=idem_key_dup after idempotent submit, got %v", jobMap2["upload_idempotency_key"])
	}
}

func TestV2SubmitResultMissingLease(t *testing.T) {
	r, q, _, _, reg := setupV2Test(t)
	ctx := context.Background()
	workerID := "worker-1"
	if err := reg.RegisterWorker(ctx, workerID, "test-worker", "127.0.0.1", map[string]interface{}{
		"protocol_version": workers.DefaultWorkerProtocolVersion,
		"capabilities":     map[string]interface{}{"video": true},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}
	jobID, _ := createAndClaimJob(t, ctx, q, workerID)

	body, _ := json.Marshal(map[string]interface{}{
		"worker_id": workerID,
		"status":    "completed",
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d body=%s", w.Code, w.Body.String())
	}
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	errMsg, _ := res["error"].(string)
	if errMsg == "" || !strings.Contains(strings.ToLower(errMsg), "lease") {
		t.Fatalf("expected error message mentioning lease, got: %v", errMsg)
	}
}

// TestV2EndToEndWorkerFlow simulates a full worker lifecycle using the v2 endpoints:
// 1. Poll for a job (GET /api/v1/queue/job)
// 2. Renew lease (POST /api/v1/jobs/:id/lease)
// 3. Submit result (POST /api/v1/jobs/:id/result)
// 4. Complete job (POST /api/v1/jobs/:id/complete)
func TestV2EndToEndWorkerFlow(t *testing.T) {
	r, q, _, _, reg := setupV2Test(t)
	ctx := context.Background()
	workerID := "worker-e2e-1"

	// Step 0: Register worker and create a job
	if err := reg.RegisterWorker(ctx, workerID, "e2e-worker", "127.0.0.1", map[string]interface{}{
		"protocol_version": workers.DefaultWorkerProtocolVersion,
		"capabilities":     map[string]interface{}{"video": true},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}
	payload := map[string]interface{}{
		"video_name": "e2e-test-video",
		"job_type":   "process_video",
	}
	now := time.Now()
	jobID := "job-" + now.Format("20060102150405") + "-" + strconv.Itoa(now.Nanosecond())
	if err := q.SubmitJob(ctx, jobID, payload); err != nil {
		t.Fatalf("failed to submit job: %v", err)
	}

	// Step 1: Worker polls for a job via v2 endpoint
	w1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/queue/job?worker_id="+workerID, nil)
	r.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("poll job: want 200, got %d body=%s", w1.Code, w1.Body.String())
	}
	var pollResp map[string]interface{}
	if err := json.Unmarshal(w1.Body.Bytes(), &pollResp); err != nil {
		t.Fatalf("poll job JSON: %v", err)
	}
	if pollResp["job"] == nil {
		t.Fatalf("expected job in poll response, got: %v", pollResp)
	}
	jobData := pollResp["job"].(map[string]interface{})
	leaseID, _ := jobData["lease_id"].(string)
	if leaseID == "" {
		t.Fatal("expected lease_id in claimed job")
	}
	attemptFloat, _ := jobData["attempt"].(float64)
	attempt := int(attemptFloat)

	// Step 2: Renew lease
	leaseBody, _ := json.Marshal(map[string]interface{}{
		"worker_id":        workerID,
		"lease_id":         leaseID,
		"lease_expires_at": time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339),
		"attempt":          attempt,
		"contract_version": 2,
	})
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/lease", bytes.NewReader(leaseBody))
	req2.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("renew lease: want 200, got %d body=%s", w2.Code, w2.Body.String())
	}

	// Step 3: Submit result
	resultBody, _ := json.Marshal(map[string]interface{}{
		"worker_id":        workerID,
		"lease_id":         leaseID,
		"status":           "completed",
		"output":           map[string]interface{}{"status": "completed"},
		"attempt":          attempt,
		"contract_version": 2,
		"artifact_id":      "artifact_e2e",
		"output_sha256":    "sha256_e2e",
		"idempotency_key":  "idem_e2e",
	})
	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/result", bytes.NewReader(resultBody))
	req3.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Fatalf("submit result: want 200, got %d body=%s", w3.Code, w3.Body.String())
	}

	// Step 4: Complete job
	completeBody, _ := json.Marshal(map[string]interface{}{
		"worker_id": workerID,
		"lease_id":  leaseID,
		"attempt":   attempt,
	})
	w4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+jobID+"/complete", bytes.NewReader(completeBody))
	req4.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Fatalf("complete job: want 200, got %d body=%s", w4.Code, w4.Body.String())
	}

	// Verify final state
	jobMap, err := q.GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job after completion: %v", err)
	}
	if jobMap["status"] != "COMPLETED" {
		t.Fatalf("want status=COMPLETED, got %v", jobMap["status"])
	}
	if jobMap["artifact_id"] != "artifact_e2e" {
		t.Fatalf("want artifact_id=artifact_e2e, got %v", jobMap["artifact_id"])
	}
	if jobMap["output_sha256"] != "sha256_e2e" {
		t.Fatalf("want output_sha256=sha256_e2e, got %v", jobMap["output_sha256"])
	}
	if jobMap["upload_idempotency_key"] != "idem_e2e" {
		t.Fatalf("want idempotency_key=idem_e2e, got %v", jobMap["upload_idempotency_key"])
	}
}

func TestV2SubmitResultInvalidBody(t *testing.T) {
	r, _, _, _, _ := setupV2Test(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/job-123/result", bytes.NewReader([]byte("not-json")))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}
