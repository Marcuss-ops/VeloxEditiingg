package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/forwardingcontract"
	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

// TestGetSubmittedJob_FailureSurfacedToOwner pins the additive failure
// contract: when the job is FAILED and the latest task attempt carries the
// worker-reported error, the polling response includes attempt_status /
// failure_code / failure_message — scoped to the owner, absent on
// non-failed jobs, and falling back to jobs.error_message when no attempt
// row exists.
func TestGetSubmittedJob_FailureSurfacedToOwner(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()

	seed := func(jobID, sourceJobID, jobStatus, jobErrorMessage string) {
		t.Helper()
		if _, err := db.Forwarding().InsertCreatorForwarding(ctx, &forwardingcontract.CreatorForwarding{
			ForwardingID:     "cf-" + jobID,
			ExternalClientID: "client-failure-owner",
			SourceProvider:   ExternalAPISourceProvider,
			SourceJobID:      sourceJobID,
			TargetExecutorID: JobSubmitTargetExecutorID,
			TargetJobID:      jobID,
			Status:           string(forwardingcontract.CFStatusForwarded),
		}); err != nil {
			t.Fatalf("seed forwarding %s: %v", jobID, err)
		}
		if _, err := db.DB().ExecContext(ctx,
			`INSERT INTO jobs (job_id, status, error_message, revision, max_retries, created_at, updated_at, migrated_at)
			 VALUES (?, ?, ?, 0, 3, datetime('now'), datetime('now'), datetime('now'))`,
			jobID, jobStatus, jobErrorMessage); err != nil {
			t.Fatalf("seed job %s: %v", jobID, err)
		}
	}

	// failed-with-attempt: worker completed the attempt with an error.
	seed("job-failed-attempt", "idem-failed-attempt", "FAILED", "")
	if _, err := db.DB().ExecContext(ctx,
		`INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id, lease_id, status, error_code, error_message, created_at, updated_at)
		 VALUES ('ta-failed-1', 'task-failed-1', 'job-failed-attempt', 1, 'worker-51', 'lease-51', 'FAILED', 'RENDER_FAILED', 'velox_video_engine exited with code 1: plan segment 5 decode error', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed failed attempt: %v", err)
	}

	// failed-no-attempt: job failed before any attempt row materialized.
	seed("job-failed-noattempt", "idem-failed-noattempt", "FAILED", "job failed: dispatch timeout waiting for worker lease")

	h := &Handlers{store: db, cfg: &config.Config{ControlPlane: config.ControlPlaneEndpoints{
		RESTPublic: "http://51.91.11.36:8000",
	}}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, c.GetHeader("X-Test-M2M-Client"))
		h.GetSubmittedJob()(c)
	})
	request := func(jobID string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+jobID, nil)
		req.Header.Set("X-Test-M2M-Client", "client-failure-owner")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
		}
		return w.Code, body
	}

	code, body := request("job-failed-attempt")
	if code != http.StatusOK {
		t.Fatalf("failed-attempt status = %d, want 200; body=%v", code, body)
	}
	if body["status"] != "FAILED" {
		t.Fatalf("status = %v, want FAILED", body["status"])
	}
	if body["attempt_status"] != "FAILED" {
		t.Fatalf("attempt_status = %v, want FAILED", body["attempt_status"])
	}
	if body["failure_code"] != "RENDER_FAILED" {
		t.Fatalf("failure_code = %v, want RENDER_FAILED", body["failure_code"])
	}
	if body["failure_message"] == "" {
		t.Fatalf("failure_message = %v, want worker-reported error", body["failure_message"])
	}
	if body["worker_id"] != "worker-51" {
		t.Fatalf("worker_id = %v, want worker-51", body["worker_id"])
	}

	code, body = request("job-failed-noattempt")
	if code != http.StatusOK {
		t.Fatalf("failed-noattempt status = %d, want 200; body=%v", code, body)
	}
	if body["failure_code"] != "JOB_FAILED" {
		t.Fatalf("failure_code fallback = %v, want JOB_FAILED", body["failure_code"])
	}
	if body["failure_message"] != "job failed: dispatch timeout waiting for worker lease" {
		t.Fatalf("failure_message fallback = %v, want job-level error_message", body["failure_message"])
	}
	if _, has := body["attempt_status"]; has {
		t.Fatalf("attempt_status must be absent when no attempt row exists, got %v", body["attempt_status"])
	}
}

// TestGetSubmittedJob_SuccessOmitsFailureFields pins the additive-only
// guarantee: a SUCCEEDED job response never carries the failure fields, so
// existing consumers polling happy-path jobs see a byte-compatible body.
func TestGetSubmittedJob_SuccessOmitsFailureFields(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf-job-ok",
		ExternalClientID: "client-failure-owner",
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      "idem-job-ok",
		TargetExecutorID: JobSubmitTargetExecutorID,
		TargetJobID:      "job-ok",
		Status:           string(forwardingcontract.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("seed forwarding: %v", err)
	}
	if _, err := db.DB().ExecContext(ctx,
		`INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at)
		 VALUES ('job-ok', 'SUCCEEDED', 0, 3, datetime('now'), datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	// Even a completed attempt with an empty error must not leak failure fields.
	if _, err := db.DB().ExecContext(ctx,
		`INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id, lease_id, status, error_code, error_message, created_at, updated_at)
		 VALUES ('ta-ok-1', 'task-ok-1', 'job-ok', 1, 'worker-51', 'lease-51', 'SUCCEEDED', '', '', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed succeeded attempt: %v", err)
	}

	h := &Handlers{store: db}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, c.GetHeader("X-Test-M2M-Client"))
		h.GetSubmittedJob()(c)
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-ok", nil)
	req.Header.Set("X-Test-M2M-Client", "client-failure-owner")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if body["status"] != "SUCCEEDED" {
		t.Fatalf("status = %v, want SUCCEEDED", body["status"])
	}
	for _, key := range []string{"attempt_status", "failure_code", "failure_message"} {
		if _, has := body[key]; has {
			t.Fatalf("%s must be absent on SUCCEEDED jobs, got %v", key, body[key])
		}
	}
}
