package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/store"
)

func TestM2MPipelineEndpoints_CrossClientIsIndistinguishableFromMissing(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	if _, err := db.InsertCreatorForwarding(ctx, &store.CreatorForwarding{
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

	h := &Handlers{store: db}
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
