package smoke

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/platform/clock"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestCreateSmokeClipStock_Validation(t *testing.T) {
	cfg := config.FromEnv()
	if cfg.Database.DBPath == "" {
		cfg.Database.DBPath = filepath.Join(t.TempDir(), "velox.db")
	}
	db, err := store.NewSQLiteStore(cfg.Database.DBPath)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	ts, err := queue.NewLifecycleService(store.NewSQLiteJobRepository(db), store.NewSQLiteJobRepository(db), clock.System{})
	if err != nil {
		t.Skipf("Transition service unavailable: %v", err)
	}
	querySvc := queue.NewQueryService(db, nil)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.Workers.MaxJobAttempts}, ts, querySvc)
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}

	r := gin.New()
	r.POST("/api/v1/video/smoke-clip-stock", CreateSmokeClipStock(config.FromEnv(), q))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/smoke-clip-stock", bytes.NewReader([]byte(`{"video_name":"test"}`)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.Bytes())
	}
}

func TestCreateSmokeClipStock_Enqueue(t *testing.T) {
	cfg := config.FromEnv()
	if cfg.Database.DBPath == "" {
		cfg.Database.DBPath = filepath.Join(t.TempDir(), "velox.db")
	}
	db, err := store.NewSQLiteStore(cfg.Database.DBPath)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	ts, err := queue.NewLifecycleService(store.NewSQLiteJobRepository(db), store.NewSQLiteJobRepository(db), clock.System{})
	if err != nil {
		t.Skipf("Transition service unavailable: %v", err)
	}
	querySvc := queue.NewQueryService(db, nil)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.Workers.MaxJobAttempts}, ts, querySvc)
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}

	r := gin.New()
	r.POST("/api/v1/video/smoke-clip-stock", CreateSmokeClipStock(config.FromEnv(), q))

	payload := map[string]interface{}{
		"video_name":          "Smoke clip stock",
		"script_text":         "Smoke test per il worker remoto.",
		"video_mode":          "clip_stock",
		"voiceover_paths":     []string{"https://drive.google.com/file/d/voiceover/view"},
		"intro_clip_paths":    []string{"https://drive.google.com/file/d/intro/view"},
		"stock_clip_paths":    []string{"https://drive.google.com/file/d/stock/view"},
		"output_path":         "/tmp/velox/output/smoke_clip_stock_test.mp4",
		"drive_output_folder": "https://drive.google.com/drive/folders/test-folder",
		"clip_segments": []interface{}{
			map[string]interface{}{
				"text":             "segmento stock",
				"clip_links":       []string{"https://drive.google.com/file/d/stock/view"},
				"duration_seconds": 4,
				"kind":             "stock",
			},
		},
	}
	body, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/smoke-clip-stock", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.Bytes())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["ok"] != true {
		t.Fatalf("want ok:true, got %v", res["ok"])
	}
	if res["job_type"] != "process_video" {
		t.Fatalf("want job_type process_video, got %v", res["job_type"])
	}
	if res["video_mode"] != "clip_stock" {
		t.Fatalf("want video_mode clip_stock, got %v", res["video_mode"])
	}
}
