package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"
)

func TestGetSubmittedJob_M2MOwnershipUsesIndistinguishable404(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	const (
		jobID = "job-owned-by-client-a"
		owner = "client-a"
		other = "client-b"
	)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO creator_forwardings
			(forwarding_id, external_client_id, source_provider, source_job_id,
			 source_status, target_executor_id, target_job_id, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
		"cf-owned-by-client-a", owner, ExternalAPISourceProvider, "idem-owned-by-client-a",
		"completed", JobSubmitTargetExecutorID, jobID, "FORWARDED",
	); err != nil {
		t.Fatalf("seed creator forwarding: %v", err)
	}

	h := &Handlers{store: db}
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
			t.Fatalf("decode response (%s): %v; body=%s", clientID, err, w.Body.String())
		}
		return w.Code, body
	}

	ownerStatus, ownerBody := request(owner, jobID)
	if ownerStatus != http.StatusOK {
		t.Fatalf("owner status = %d, want 200; body=%v", ownerStatus, ownerBody)
	}
	if ownerBody["job_id"] != jobID {
		t.Fatalf("owner job_id = %v, want %s", ownerBody["job_id"], jobID)
	}

	otherStatus, otherBody := request(other, jobID)
	missingStatus, missingBody := request(other, "job-does-not-exist")
	assertIndistinguishableJobNotFound(t, otherStatus, otherBody, missingStatus, missingBody)
}

func TestSubmitThenPoll_M2MClientIsolationWithRealMiddleware(t *testing.T) {
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	if _, err := db.DB().Exec(`
		INSERT INTO delivery_destinations
			(destination_id, provider, name, enabled, configuration_json, created_at, updated_at)
		VALUES ('drive', 'google_drive', 'Drive', 1, '{}', datetime('now'), datetime('now'))`); err != nil {
		t.Fatalf("seed delivery destination: %v", err)
	}

	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	enqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})
	resolver := creatorflow.NewResolverFromDeps(enqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")
	h := NewHandlersWithResolver(&config.Config{}, enqueuer, nil, resolver, jobRepo, nil, nil).WithStore(db)

	clientASecret := store.GenerateM2MSecret()
	clientBSecret := store.GenerateM2MSecret()
	for _, key := range []store.M2MAPIKey{
		{ClientID: "client-a", SecretHash: store.HashM2MSecret(clientASecret), Scopes: []string{"jobs.submit"}, IsActive: true, RateLimitRPS: 100, RateLimitBurst: 100},
		{ClientID: "client-b", SecretHash: store.HashM2MSecret(clientBSecret), Scopes: []string{"jobs.submit"}, IsActive: true, RateLimitRPS: 100, RateLimitBurst: 100},
	} {
		if err := db.InsertM2MAPIKey(context.Background(), key); err != nil {
			t.Fatalf("seed M2M key %s: %v", key.ClientID, err)
		}
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, func(c *gin.Context) { c.Next() }, NewM2MJwAuthMiddleware(&config.Config{}, db, newM2MRateLimiter()))

	body := validSubmitJobBody("ownership-e2e-client-a")
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal submit body: %v", err)
	}
	post := func(secret string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+secret)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	created := post(clientASecret)
	if created.Code != http.StatusAccepted {
		t.Fatalf("client A POST = %d, want 202; body=%s", created.Code, created.Body.String())
	}
	var createdBody map[string]any
	if err := json.Unmarshal(created.Body.Bytes(), &createdBody); err != nil {
		t.Fatalf("decode POST response: %v", err)
	}
	jobID, ok := createdBody["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("POST response missing job_id: %s", created.Body.String())
	}

	get := func(secret, requestedJobID string) (int, map[string]any) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+requestedJobID, nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var response map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode GET response: %v; body=%s", err, w.Body.String())
		}
		return w.Code, response
	}

	ownerStatus, ownerResponse := get(clientASecret, jobID)
	if ownerStatus != http.StatusOK || ownerResponse["job_id"] != jobID {
		t.Fatalf("client A GET = %d/%v, want 200/%s; response=%v", ownerStatus, ownerResponse["job_id"], jobID, ownerResponse)
	}
	otherStatus, otherResponse := get(clientBSecret, jobID)
	missingStatus, missingResponse := get(clientBSecret, "job-never-created")
	assertIndistinguishableJobNotFound(t, otherStatus, otherResponse, missingStatus, missingResponse)
}

func assertIndistinguishableJobNotFound(t *testing.T, firstStatus int, firstBody map[string]any, secondStatus int, secondBody map[string]any) {
	t.Helper()
	if firstStatus != http.StatusNotFound || secondStatus != http.StatusNotFound {
		t.Fatalf("cross-client and missing statuses = %d/%d, want 404/404; bodies=%v/%v", firstStatus, secondStatus, firstBody, secondBody)
	}
	if firstBody["error"] != "job_not_found" || secondBody["error"] != "job_not_found" {
		t.Fatalf("cross-client and missing errors = %v/%v, want job_not_found", firstBody["error"], secondBody["error"])
	}
	if firstBody["message"] != secondBody["message"] {
		t.Fatalf("cross-client and missing messages differ: %v vs %v", firstBody["message"], secondBody["message"])
	}
}

func TestPipelineRunStatus_M2MOwnershipUsesIndistinguishable404(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	if _, err := db.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
		ForwardingID:     "cf-pipeline-owned",
		ExternalClientID: "client-a",
		SourceProvider:   "remote_engine",
		SourceJobID:      "remote-owned",
		TargetExecutorID: JobSubmitTargetExecutorID,
		Status:           string(store.CFStatusPending),
	}); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
	if _, err := db.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID:                   "run-owned-by-client-a",
		RequestID:            "request-owned-by-client-a",
		IdempotencyKey:       "idem-pipeline-owned",
		Status:               pipelineruns.StatusRemoteQueued,
		ForwardingID:         "cf-pipeline-owned",
		RemoteProvider:       "remote_engine",
		RemoteJobID:          "remote-owned",
		RequestedPayloadJSON: "{}",
	}); err != nil {
		t.Fatalf("insert pipeline run: %v", err)
	}

	h := &Handlers{store: db}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/pipeline-runs/:id", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, c.GetHeader("X-Test-M2M-Client"))
		h.PipelineRunStatus()(c)
	})
	request := func(clientID, runID string) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pipeline-runs/"+runID, nil)
		req.Header.Set("X-Test-M2M-Client", clientID)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode pipeline response: %v; body=%s", err, w.Body.String())
		}
		return w.Code, body
	}
	ownerStatus, ownerBody := request("client-a", "run-owned-by-client-a")
	if ownerStatus != http.StatusOK || ownerBody["id"] != "run-owned-by-client-a" {
		t.Fatalf("owner status/body = %d/%v, want 200/run-owned-by-client-a", ownerStatus, ownerBody)
	}
	otherStatus, otherBody := request("client-b", "run-owned-by-client-a")
	missingStatus, missingBody := request("client-b", "run-does-not-exist")
	if otherStatus != http.StatusNotFound || missingStatus != http.StatusNotFound || otherBody["error"] != missingBody["error"] {
		t.Fatalf("cross-client/missing = %d/%v and %d/%v, want indistinguishable 404", otherStatus, otherBody, missingStatus, missingBody)
	}
}

func TestInsertCreatorForwarding_PersistsExternalClientID(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	inserted, err := db.InsertCreatorForwarding(context.Background(), &store.CreatorForwarding{
		ForwardingID:     "cf-client-id-persisted",
		ExternalClientID: "client-persisted",
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      "idem-client-id-persisted",
		TargetExecutorID: JobSubmitTargetExecutorID,
		TargetJobID:      "job-client-id-persisted",
		Status:           string(store.CFStatusForwarded),
	})
	if err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
	if inserted == nil || inserted.Forwarding == nil {
		t.Fatal("insert returned no forwarding")
	}
	if inserted.Forwarding.ExternalClientID != "client-persisted" {
		t.Fatalf("inserted external client id = %q, want client-persisted", inserted.Forwarding.ExternalClientID)
	}

	got, err := db.GetCreatorForwardingByTargetJobID(context.Background(), "job-client-id-persisted", "client-persisted")
	if err != nil {
		t.Fatalf("owner lookup: %v", err)
	}
	if got == nil || got.ExternalClientID != "client-persisted" {
		t.Fatalf("owner lookup = %#v, want client-persisted", got)
	}
	if _, err := db.GetCreatorForwardingByTargetJobID(context.Background(), "job-client-id-persisted", "another-client"); err != store.ErrCreatorForwardingNoRow {
		t.Fatalf("cross-client lookup error = %v, want ErrCreatorForwardingNoRow", err)
	}
}
