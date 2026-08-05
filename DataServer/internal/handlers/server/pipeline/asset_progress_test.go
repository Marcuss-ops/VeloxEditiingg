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
	} {
		if err := db.IngestAssetDownloadProgress(context.Background(), record); err != nil {
			t.Fatalf("ingest %s: %v", record.AssetKey, err)
		}
	}

	h := (&Handlers{}).WithStore(db)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/v1/jobs/:id/asset-progress", h.AssetDownloadProgress())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-progress/asset-progress", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var response struct {
		ProgressPercent float64 `json:"progress_percent"`
		BytesDownloaded int64   `json:"bytes_downloaded"`
		BytesTotal      int64   `json:"bytes_total"`
		Assets          struct {
			Total       int `json:"total"`
			Ready       int `json:"ready"`
			Downloading int `json:"downloading"`
			CacheHits   int `json:"cache_hits"`
		} `json:"assets"`
		Active []store.AssetDownloadProgressView `json:"active"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, w.Body.String())
	}
	if response.BytesDownloaded != 125 || response.BytesTotal != 300 {
		t.Fatalf("bytes = %d/%d, want 125/300", response.BytesDownloaded, response.BytesTotal)
	}
	wantPercent := float64(125) / 300 * 100
	if response.ProgressPercent != wantPercent {
		t.Fatalf("progress_percent = %v, want %v", response.ProgressPercent, wantPercent)
	}
	if response.Assets.Total != 2 || response.Assets.Ready != 1 || response.Assets.Downloading != 1 || response.Assets.CacheHits != 1 {
		t.Fatalf("asset counts = %+v, want total=2 ready=1 downloading=1 cache_hits=1", response.Assets)
	}
	if len(response.Active) != 1 || response.Active[0].AssetKey != "sha256:active" {
		t.Fatalf("active = %+v, want active sha256:active", response.Active)
	}
}
