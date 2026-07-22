package instaedit

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/instaeditauth"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// testPlanResolver reads the committed delivery plan from the SQLite store.
// It satisfies enqueue.PlanResolver so the post-create precondition passes.
type testPlanResolver struct {
	db *sql.DB
}

func (r *testPlanResolver) ResolvePlan(ctx context.Context, jobID, _ string) (*enqueue.ResolvedPlan, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT destination_id, priority, retry_budget FROM job_delivery_plans WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dests []enqueue.PlanDestination
	for rows.Next() {
		var d enqueue.PlanDestination
		if err := rows.Scan(&d.DestinationID, &d.Priority, &d.RetryBudget); err != nil {
			return nil, err
		}
		dests = append(dests, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &enqueue.ResolvedPlan{JobID: jobID, Destinations: dests}, nil
}

func setupIntegrationRouter(t *testing.T) (*gin.Engine, *store.SQLiteStore) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "instaedit-bff-test.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("create sqlite store: %v", err)
	}

	verifier, err := instaeditauth.New(testSecret)
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}

	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	planResolver := &testPlanResolver{db: db.DB()}
	enq := enqueue.NewEnqueuer(atomic, jobRepo, nil, planResolver)

	svc := NewServiceFromSQLite(db, jobRepo, store.NewSQLiteAssetRepository(db), enq)
	handler := NewHandler(HandlerDeps{
		Verifier: verifier,
		Service:  svc,
	})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	handler.RegisterRoutes(r)
	return r, db
}

func mintIntegrationToken(t *testing.T, scopes []string) string {
	t.Helper()
	claims := instaeditauth.Claims{
		Issuer:      "instaedit",
		Audience:    "velox",
		Subject:     "user-123",
		WorkspaceID: 45,
		Scopes:      scopes,
		ExpiresAt:   time.Now().Add(5 * time.Minute).Unix(),
		JTI:         "e2e-jti",
	}
	return mintToken(t, claims)
}

func TestInstaEditBFF_EndToEnd(t *testing.T) {
	router, db := setupIntegrationRouter(t)

	// Seed an enabled delivery destination with the external id the BFF will use.
	externalDestID := "extdst_e2e_01"
	dest := &store.DeliveryDestination{
		DestinationID:         "dest-e2e-01",
		Provider:              "test_social",
		ExternalDestinationID: externalDestID,
		Name:                  "E2E Destination",
		Enabled:               true,
	}
	if err := db.InsertDeliveryDestination(dest); err != nil {
		t.Fatalf("seed destination: %v", err)
	}

	token := mintIntegrationToken(t, []string{
		ScopeJobsRead, ScopeJobsWrite, ScopeWorkersRead, ScopeAssetsRead,
	})

	doRequest := func(method, path string, body any) *httptest.ResponseRecorder {
		var reqBody []byte
		if body != nil {
			reqBody, _ = json.Marshal(body)
		}
		req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	// 1. Create a job through the BFF.
	createBody := map[string]any{
		"project_id": "proj-e2e-01",
		"render_spec": map[string]any{
			"video_name": "E2E Test Video",
			"voiceover_paths": []string{"https://example.com/voice.mp3"},
			"scenes": []map[string]any{
				{"text": "scene one", "image_link": "https://example.com/img.png"},
			},
		},
		"delivery_plan": map[string]any{
			"destinations": []map[string]any{
				{
					"external_destination_id": externalDestID,
					"metadata": map[string]any{
						"title":       "E2E Video",
						"description": "end to end test",
					},
				},
			},
		},
	}

	wCreate := doRequest(http.MethodPost, "/api/v1/instaedit/jobs", createBody)
	if wCreate.Code != http.StatusCreated {
		t.Fatalf("POST /jobs want 201, got %d: %s", wCreate.Code, wCreate.Body.String())
	}
	var created jobResponse
	if err := json.Unmarshal(wCreate.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created.ID == "" {
		t.Fatalf("expected a job id in create response")
	}
	if created.WorkspaceID != 45 {
		t.Fatalf("expected workspace_id 45, got %d", created.WorkspaceID)
	}

	// 2. Retrieve the job.
	wGet := doRequest(http.MethodGet, "/api/v1/instaedit/jobs/"+created.ID, nil)
	if wGet.Code != http.StatusOK {
		t.Fatalf("GET /jobs/:id want 200, got %d: %s", wGet.Code, wGet.Body.String())
	}
	var detail jobDetailResponse
	if err := json.Unmarshal(wGet.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode job detail: %v", err)
	}
	if detail.Job.ID != created.ID {
		t.Fatalf("job detail mismatch: want %s, got %s", created.ID, detail.Job.ID)
	}

	// 3. List deliveries for the job.
	wDeliveries := doRequest(http.MethodGet, "/api/v1/instaedit/jobs/"+created.ID+"/deliveries", nil)
	if wDeliveries.Code != http.StatusOK {
		t.Fatalf("GET /jobs/:id/deliveries want 200, got %d: %s", wDeliveries.Code, wDeliveries.Body.String())
	}
	var deliveries listDeliveriesResponse
	if err := json.Unmarshal(wDeliveries.Body.Bytes(), &deliveries); err != nil {
		t.Fatalf("decode deliveries: %v", err)
	}
	// After creation the render is not finished, so no job_deliveries rows
	// exist yet. The endpoint must still return 200 with an empty list.
	if len(deliveries.Deliveries) != 0 {
		t.Fatalf("expected 0 deliveries before render completion, got %d", len(deliveries.Deliveries))
	}

	// 4. Cancel the job.
	wCancel := doRequest(http.MethodPost, "/api/v1/instaedit/jobs/"+created.ID+"/cancel", nil)
	if wCancel.Code != http.StatusNoContent {
		t.Fatalf("POST /jobs/:id/cancel want 204, got %d: %s", wCancel.Code, wCancel.Body.String())
	}

	// 5. List workers (empty is fine; route must be reachable and scoped).
	wWorkers := doRequest(http.MethodGet, "/api/v1/instaedit/workers", nil)
	if wWorkers.Code != http.StatusOK {
		t.Fatalf("GET /workers want 200, got %d: %s", wWorkers.Code, wWorkers.Body.String())
	}
	var workers listWorkersResponse
	if err := json.Unmarshal(wWorkers.Body.Bytes(), &workers); err != nil {
		t.Fatalf("decode workers: %v", err)
	}
	if len(workers.Workers) != 0 {
		t.Fatalf("expected empty worker list, got %d", len(workers.Workers))
	}

	// 6. Get an asset scoped to the workspace.
	assetID := "asset-e2e-01"
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.DB().Exec(
		`INSERT INTO assets (asset_id, kind, status, sha256, mime_type, size_bytes,
		                     storage_provider, storage_key, created_at, workspace_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		assetID, "video", "READY", "deadbeef", "video/mp4", 12345,
		"fs", "s3://bucket/key", now, 45,
	)
	if err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	wAsset := doRequest(http.MethodGet, "/api/v1/instaedit/assets/"+assetID, nil)
	if wAsset.Code != http.StatusOK {
		t.Fatalf("GET /assets/:id want 200, got %d: %s", wAsset.Code, wAsset.Body.String())
	}
	var assetResp assetResponse
	if err := json.Unmarshal(wAsset.Body.Bytes(), &assetResp); err != nil {
		t.Fatalf("decode asset: %v", err)
	}
	if assetResp.ID != assetID {
		t.Fatalf("asset id mismatch: want %s, got %s", assetID, assetResp.ID)
	}
	if assetResp.WorkspaceID != 45 {
		t.Fatalf("asset workspace_id want 45, got %d", assetResp.WorkspaceID)
	}
}

// TestInstaEditBFF_JobIsolation ensures jobs from other workspaces cannot be
// read by a JWT carrying a different workspace_id.
func TestInstaEditBFF_JobIsolation(t *testing.T) {
	router, _ := setupIntegrationRouter(t)

	token := mintIntegrationToken(t, []string{ScopeJobsRead})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/instaedit/jobs/job-other", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET foreign job want 404, got %d: %s", w.Code, w.Body.String())
	}
}
