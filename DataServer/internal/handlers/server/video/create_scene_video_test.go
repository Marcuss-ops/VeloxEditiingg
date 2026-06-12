package video

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestCreateFromScenes_InvalidJSON(t *testing.T) {
	cfg := config.FromEnv()
	db, err := store.NewSQLiteStore(cfg.DBDSN)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.MaxJobAttempts, DBStore: db})
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}
	r := gin.New()
	r.POST("/api/v1/video/create-scenes", CreateFromScenes(config.FromEnv(), q))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/create-scenes", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.Bytes())
	}
}

func TestCreateFromScenes_Enqueue(t *testing.T) {
	cfg := config.FromEnv()
	db, err := store.NewSQLiteStore(cfg.DBDSN)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.MaxJobAttempts, DBStore: db})
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}
	r := gin.New()
	r.POST("/api/v1/video/create-scenes", CreateFromScenes(config.FromEnv(), q))

	payload := map[string]interface{}{
		"video_name":     "La storia del Colosseo",
		"script_text":    "L'Impero Romano non si limitò a costruire città; costruì miti in pietra.",
		"voiceover_path": "https://drive.google.com/file/d/voiceover/view",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Scene one",
				"image_link": "https://drive.google.com/file/d/image1/view",
			},
		},
	}
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/create-scenes", bytes.NewReader(body))
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
	if res["job_run_id"] == "" {
		t.Fatalf("want non-empty job_run_id, got %v", res["job_run_id"])
	}
	if res["correlation_id"] == "" {
		t.Fatalf("want non-empty correlation_id, got %v", res["correlation_id"])
	}
	if res["job_fingerprint"] == "" {
		t.Fatalf("want non-empty job_fingerprint, got %v", res["job_fingerprint"])
	}
	if res["scene_count"] != float64(1) {
		t.Fatalf("want scene_count 1, got %v", res["scene_count"])
	}
}

func TestCreateFromScenes_Validation(t *testing.T) {
	cfg := config.FromEnv()
	db, err := store.NewSQLiteStore(cfg.DBDSN)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.MaxJobAttempts, DBStore: db})
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}
	r := gin.New()
	r.POST("/api/v1/video/create-scenes", CreateFromScenes(config.FromEnv(), q))

	payload := map[string]interface{}{
		"video_name":     "Missing pieces",
		"script_text":    "This script is here but scenes are missing.",
		"voiceover_path": "https://drive.google.com/file/d/voiceover/view",
	}
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/create-scenes", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.Bytes())
	}
}
