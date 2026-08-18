package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

func TestM2MPipelineEndpoints_CrossClientIsIndistinguishableFromMissing(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-m2m-endpoint-owner",
		ExternalClientID: "client-a",
		SourceProvider:   "remote_engine",
		SourceJobID:      "remote-m2m-endpoint-owner",
		TargetExecutorID: JobSubmitTargetExecutorID,
		TargetJobID:      "job-m2m-endpoint-owner",
		Status:           string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
	if _, err := db.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID:                   "run-m2m-endpoint-owner",
		RequestID:            "request-m2m-endpoint-owner",
		IdempotencyKey:       "idem-m2m-endpoint-owner",
		Status:               pipelineruns.StatusFailed,
		ForwardingID:         "cf-m2m-endpoint-owner",
		RemoteProvider:       "remote_engine",
		RemoteJobID:          "remote-m2m-endpoint-owner",
		VeloxJobID:           "job-m2m-endpoint-owner",
		RequestedPayloadJSON: `{"scenes":[]}`,
	}); err != nil {
		t.Fatalf("insert pipeline run: %v", err)
	}

	h := &Handlers{store: db, cfg: &config.Config{ControlPlane: config.ControlPlaneEndpoints{
		RESTPublic: "http://51.91.11.36:8000",
	}}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	withClient := func(handler gin.HandlerFunc) gin.HandlerFunc {
		return func(c *gin.Context) {
			c.Set(m2mCtxKeyClientID, "client-b")
			handler(c)
		}
	}
	r.GET("/status/:id", withClient(h.PipelineRunStatus()))
	r.POST("/cancel/:id", withClient(h.CancelPipelineRun()))
	r.POST("/retry/:id", withClient(h.RetryPipelineRun()))
	r.GET("/timeline/:id", withClient(h.PipelineRunTimeline()))
	r.GET("/artifacts/:id", withClient(h.PipelineRunArtifacts()))
	r.GET("/deliveries/:id", withClient(h.PipelineRunDeliveries()))

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"status", http.MethodGet, "/status/run-m2m-endpoint-owner"},
		{"cancel", http.MethodPost, "/cancel/run-m2m-endpoint-owner"},
		{"retry", http.MethodPost, "/retry/run-m2m-endpoint-owner"},
		{"timeline", http.MethodGet, "/timeline/run-m2m-endpoint-owner"},
		{"artifacts", http.MethodGet, "/artifacts/run-m2m-endpoint-owner"},
		{"deliveries", http.MethodGet, "/deliveries/run-m2m-endpoint-owner"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			crossClientStatus, crossClientBody := requestJSON(t, r, tc.method, tc.path)
			missingStatus, missingBody := requestJSON(t, r, tc.method, "/"+tc.name+"/run-m2m-endpoint-missing")
			if crossClientStatus != http.StatusNotFound || missingStatus != http.StatusNotFound {
				t.Fatalf("cross-client/missing status = %d/%d, want 404/404; bodies=%v/%v", crossClientStatus, missingStatus, crossClientBody, missingBody)
			}
			if crossClientBody["error"] != "job_not_found" || missingBody["error"] != "job_not_found" {
				t.Fatalf("cross-client/missing errors = %v/%v, want job_not_found", crossClientBody["error"], missingBody["error"])
			}
		})
	}
}

func requestJSON(t *testing.T, r http.Handler, method, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s %s response: %v; body=%s", method, path, err, w.Body.String())
	}
	return w.Code, body
}

// TestGetSubmittedJob_OwnerSeesScopedEnrichment pins the strictly
// client-scoped enrichment of GET /api/v1/jobs/:id: the owner sees the
// canonical jobs.Status and the artifact URL, while a cross-client or
// missing job is indistinguishable 404 job_not_found.
func TestGetSubmittedJob_OwnerSeesScopedEnrichment(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	const (
		jobID  = "job-scoped-enrichment"
		client = "client-enrichment"
	)
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-scoped-enrichment",
		ExternalClientID: client,
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      "idem-scoped-enrichment",
		TargetExecutorID: JobSubmitTargetExecutorID,
		TargetJobID:      jobID,
		Status:           string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("seed forwarding: %v", err)
	}
	if _, err := db.DB().ExecContext(ctx,
		`INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at)
		 VALUES (?, 'SUCCEEDED', 0, 3, datetime('now'), datetime('now'), datetime('now'))`, jobID); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID: "artifact-scoped", JobID: jobID, Type: "video/mp4",
		StorageURL: "https://cdn.example/scoped.mp4", SizeBytes: 42, Status: "READY",
	}); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}

	h := &Handlers{store: db, cfg: &config.Config{ControlPlane: config.ControlPlaneEndpoints{
		RESTPublic: "http://51.91.11.36:8000",
	}}}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, c.GetHeader("X-Test-M2M-Client"))
		h.GetSubmittedJob()(c)
	})
	request := func(clientID, requestedJobID string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+requestedJobID, nil)
		req.Header.Set("X-Test-M2M-Client", clientID)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
		}
		return w.Code, body
	}

	ownerStatus, ownerBody := request(client, jobID)
	if ownerStatus != http.StatusOK {
		t.Fatalf("owner status = %d, want 200; body=%v", ownerStatus, ownerBody)
	}
	if ownerBody["status"] != "SUCCEEDED" {
		t.Fatalf("owner status = %v, want SUCCEEDED (jobs.Status wins over forwarding.Status)", ownerBody["status"])
	}
	if ownerBody["artifact_url"] != "http://51.91.11.36:8000/api/internal/artifacts/artifact-scoped/download" {
		t.Fatalf("owner artifact_url = %v, want scoped URL", ownerBody["artifact_url"])
	}
	if ownerBody["artifact_size_bytes"] != float64(42) {
		t.Fatalf("owner artifact_size_bytes = %v, want 42", ownerBody["artifact_size_bytes"])
	}

	otherStatus, otherBody := request("client-other", jobID)
	missingStatus, missingBody := request("client-other", "job-does-not-exist")
	if otherStatus != http.StatusNotFound || missingStatus != http.StatusNotFound {
		t.Fatalf("cross-client/missing = %d/%d, want 404/404", otherStatus, missingStatus)
	}
	if otherBody["error"] != "job_not_found" || missingBody["error"] != "job_not_found" {
		t.Fatalf("cross-client/missing errors = %v/%v, want job_not_found", otherBody["error"], missingBody["error"])
	}
	if otherBody["message"] != missingBody["message"] {
		t.Fatalf("cross-client/missing messages differ: %v vs %v", otherBody["message"], missingBody["message"])
	}
}

// TestPipelineRunStatus_OrphanedForwardingJoinIsIndistinguishable404 pins
// the "always 404, never 500" contract for M2M callers: a pipeline_runs row
// whose creator_forwardings join has vanished (data inconsistency) resolves
// to the same job_not_found envelope as a missing run.
func TestPipelineRunStatus_OrphanedForwardingJoinIsIndistinguishable404(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-orphaned-join",
		ExternalClientID: "client-a",
		SourceProvider:   "remote_engine",
		SourceJobID:      "remote-orphaned",
		TargetExecutorID: JobSubmitTargetExecutorID,
		TargetJobID:      "job-orphaned",
		Status:           string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
	if _, err := db.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID:                   "run-orphaned-join",
		RequestID:            "request-orphaned-join",
		IdempotencyKey:       "idem-orphaned-join",
		Status:               pipelineruns.StatusRemoteQueued,
		ForwardingID:         "cf-orphaned-join",
		RemoteProvider:       "remote_engine",
		RemoteJobID:          "remote-orphaned",
		VeloxJobID:           "job-orphaned",
		RequestedPayloadJSON: `{"scenes":[]}`,
	}); err != nil {
		t.Fatalf("insert pipeline run: %v", err)
	}
	// Simulate the inconsistency: the run's forwarding row disappears.
	if _, err := db.DB().ExecContext(ctx, `DELETE FROM creator_forwardings WHERE forwarding_id = 'cf-orphaned-join'`); err != nil {
		t.Fatalf("delete forwarding: %v", err)
	}

	h := &Handlers{store: db}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/status/:id", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, "client-a")
		h.PipelineRunStatus()(c)
	})
	status, body := requestJSON(t, r, http.MethodGet, "/status/run-orphaned-join")
	if status != http.StatusNotFound {
		t.Fatalf("orphaned-join status = %d, want 404; body=%v", status, body)
	}
	if body["error"] != "job_not_found" {
		t.Fatalf("orphaned-join error = %v, want job_not_found", body["error"])
	}
}
