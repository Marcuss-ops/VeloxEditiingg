package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

func TestAssetDownloadProgressProjectsByteWeightedLatestState(t *testing.T) {
	db := openHandlerTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	now := time.Now().UTC()
	for _, record := range []store.AssetDownloadProgressRecord{
		{
			WorkerID: "worker-1", TransferID: "transfer-ready", AssetKey: "sha256:ready",
			AssetID: "ready", Role: "stock", State: "READY",
			BytesDownloaded: 100, BytesTotal: 100, CacheHit: true,
			JobRefs:   []store.AssetDownloadJobRef{{JobID: "job-progress", TaskID: "task-1", SceneIDs: []string{"scene-1"}}},
			UpdatedAt: now, ReceivedAt: now,
		},
		{
			WorkerID: "worker-1", TransferID: "transfer-active", AssetKey: "sha256:active",
			AssetID: "active", Role: "clip", State: "DOWNLOADING",
			BytesDownloaded: 25, BytesTotal: 200, BytesPerSecond: 10, ETASeconds: 18,
			JobRefs:   []store.AssetDownloadJobRef{{JobID: "job-progress", TaskID: "task-1", SceneIDs: []string{"scene-2"}}},
			UpdatedAt: now.Add(time.Second), ReceivedAt: now.Add(time.Second),
		},
		{
			WorkerID: "worker-1", TransferID: "transfer-active-2", AssetKey: "sha256:active-2",
			AssetID: "active-2", Role: "clip", State: "DOWNLOADING",
			BytesDownloaded: 50, BytesTotal: 150, BytesPerSecond: 5, ETASeconds: 20,
			JobRefs:   []store.AssetDownloadJobRef{{JobID: "job-progress", TaskID: "task-2", SceneIDs: []string{"scene-2b"}}},
			UpdatedAt: now.Add(1500 * time.Millisecond), ReceivedAt: now.Add(1500 * time.Millisecond),
		},
		{
			WorkerID: "worker-1", TransferID: "transfer-queued", AssetKey: "sha256:queued",
			AssetID: "queued", Role: "stock", State: "QUEUED",
			BytesTotal: 300,
			JobRefs:    []store.AssetDownloadJobRef{{JobID: "job-progress", TaskID: "task-1", SceneIDs: []string{"scene-3"}}},
			UpdatedAt:  now.Add(2 * time.Second), ReceivedAt: now.Add(2 * time.Second),
		},
		{
			WorkerID: "worker-1", TransferID: "transfer-failed", AssetKey: "sha256:failed",
			AssetID: "failed", Role: "image", State: "FAILED",
			BytesTotal: 50, ErrorCode: "verify_failed",
			JobRefs:   []store.AssetDownloadJobRef{{JobID: "job-progress", TaskID: "task-1", SceneIDs: []string{"scene-4"}}},
			UpdatedAt: now.Add(3 * time.Second), ReceivedAt: now.Add(3 * time.Second),
		},
	} {
		if err := db.IngestAssetDownloadProgress(context.Background(), record); err != nil {
			t.Fatalf("ingest %s: %v", record.AssetKey, err)
		}
	}

	if _, err := db.Forwarding().InsertCreatorForwarding(context.Background(), &store.CreatorForwarding{
		ForwardingID: "forwarding-progress-owner", ExternalClientID: "client-progress",
		SourceProvider: ExternalAPISourceProvider, SourceJobID: "source-progress",
		TargetExecutorID: JobSubmitTargetExecutorID, TargetJobID: "job-progress",
		Status: string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("seed forwarding: %v", err)
	}

	h := (&Handlers{}).WithStore(db)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, nil, func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, "client-progress")
		c.Next()
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-progress/asset-progress", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var response struct {
		ProgressPercent float64 `json:"progress_percent"`
		BytesDownloaded int64   `json:"bytes_downloaded"`
		BytesTotal      int64   `json:"bytes_total"`
		Throughput      float64 `json:"throughput_bytes_per_second"`
		ETASeconds      int64   `json:"eta_seconds"`
		Assets          struct {
			Total       int `json:"total"`
			Ready       int `json:"ready"`
			Downloading int `json:"downloading"`
			Queued      int `json:"queued"`
			Failed      int `json:"failed"`
			CacheHits   int `json:"cache_hits"`
		} `json:"assets"`
		Active []store.AssetDownloadProgressView `json:"active"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if response.BytesDownloaded != 175 || response.BytesTotal != 800 {
		t.Fatalf("bytes = %d/%d, want 175/800", response.BytesDownloaded, response.BytesTotal)
	}
	wantPercent := float64(175) / 800 * 100
	if response.ProgressPercent != wantPercent {
		t.Fatalf("progress_percent = %v, want %v", response.ProgressPercent, wantPercent)
	}
	if response.Throughput != 15 || response.ETASeconds != 20 {
		t.Fatalf("aggregate performance = throughput=%v eta=%d, want 15/20", response.Throughput, response.ETASeconds)
	}
	if response.Assets.Total != 5 || response.Assets.Ready != 1 || response.Assets.Downloading != 2 || response.Assets.Queued != 1 || response.Assets.Failed != 1 || response.Assets.CacheHits != 1 {
		t.Fatalf("asset counts = %+v, want total=5 ready=1 downloading=2 queued=1 failed=1 cache_hits=1", response.Assets)
	}
	if len(response.Active) != 2 || response.Active[0].AssetKey != "sha256:active-2" || response.Active[1].AssetKey != "sha256:active" {
		t.Fatalf("active = %+v, want active-2 and active", response.Active)
	}

}

func TestAssetDownloadProgressReturnsZeroMetricsForKnownJobWithoutAssets(t *testing.T) {
	db := openHandlerTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Forwarding().InsertCreatorForwarding(context.Background(), &store.CreatorForwarding{
		ForwardingID: "forwarding-progress-empty", ExternalClientID: "client-empty",
		SourceProvider: ExternalAPISourceProvider, SourceJobID: "source-empty",
		TargetExecutorID: JobSubmitTargetExecutorID, TargetJobID: "job-empty",
		Status: string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("seed forwarding: %v", err)
	}

	h := (&Handlers{}).WithStore(db)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id/asset-progress", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, "client-empty")
		h.AssetDownloadProgress()(c)
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-empty/asset-progress", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	var response struct {
		ProgressPercent float64 `json:"progress_percent"`
		Throughput      float64 `json:"throughput_bytes_per_second"`
		ETASeconds      int64   `json:"eta_seconds"`
		Assets          struct {
			Total int `json:"total"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if response.ProgressPercent != 0 || response.Throughput != 0 || response.ETASeconds != 0 || response.Assets.Total != 0 {
		t.Fatalf("empty-job metrics = %+v, want all zero", response)
	}
}

func TestAssetDownloadProgressRequiresClientIdentity(t *testing.T) {
	db := openHandlerTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	h := (&Handlers{}).WithStore(db)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id/asset-progress", h.AssetDownloadProgress())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-without-client/asset-progress", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status without client identity = %d, want 404; body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode missing-client envelope: %v; body=%s", err, w.Body.String())
	}
	if body["ok"] != false || body["error"] != "job_not_found" || body["message"] != "job_id does not match any known creator forwarding" {
		t.Fatalf("unexpected missing-client envelope: %v", body)
	}
}

func TestAssetDownloadProgressRejectsCrossClientLookupAsIndistinguishable404(t *testing.T) {
	db := openHandlerTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Forwarding().InsertCreatorForwarding(context.Background(), &store.CreatorForwarding{
		ForwardingID: "forwarding-progress-owner-2", ExternalClientID: "client-owner",
		SourceProvider: ExternalAPISourceProvider, SourceJobID: "source-progress-2",
		TargetExecutorID: JobSubmitTargetExecutorID, TargetJobID: "job-owned",
		Status: string(store.CFStatusForwarded),
	}); err != nil {
		t.Fatalf("seed forwarding: %v", err)
	}
	if err := db.IngestAssetDownloadProgress(context.Background(), store.AssetDownloadProgressRecord{
		WorkerID: "worker-1", AssetKey: "sha256:owned", State: "READY",
		BytesDownloaded: 10, BytesTotal: 10,
		JobRefs: []store.AssetDownloadJobRef{{JobID: "job-owned"}}, ReceivedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed progress: %v", err)
	}

	h := (&Handlers{}).WithStore(db)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id/asset-progress", func(c *gin.Context) {
		c.Set(m2mCtxKeyClientID, c.GetHeader("X-Test-M2M-Client"))
		h.AssetDownloadProgress()(c)
	})
	request := func(clientID, jobID string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+jobID+"/asset-progress", nil)
		req.Header.Set("X-Test-M2M-Client", clientID)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	owner := request("client-owner", "job-owned")
	if owner.Code != http.StatusOK {
		t.Fatalf("owner status = %d, want 200; body=%s", owner.Code, owner.Body.String())
	}
	other := request("client-other", "job-owned")
	missing := request("client-other", "job-missing")
	if other.Code != http.StatusNotFound || missing.Code != http.StatusNotFound {
		t.Fatalf("cross-client/missing status = %d/%d, want 404/404", other.Code, missing.Code)
	}
	if other.Body.String() != missing.Body.String() {
		t.Fatalf("cross-client and missing responses differ: %s vs %s", other.Body.String(), missing.Body.String())
	}
}
