package jobs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestBuildSingleJobSetsJobRunID(t *testing.T) {
	data := map[string]interface{}{
		"video_name":  "Test video",
		"start_clips": []interface{}{"https://example.com/clip.mp4"},
		"voiceovers":  []interface{}{"https://example.com/voice.mp3"},
		"created_at":  int64(1234567890),
		"job_type":    "process_video",
		"script_text": "hello world script",
	}

	jobID, normalized, fingerprint := buildSingleJob(data)
	if jobID == "" {
		t.Fatal("expected non-empty jobID")
	}
	if normalized["job_run_id"] == "" {
		t.Fatalf("expected non-empty job_run_id, got %v", normalized["job_run_id"])
	}
	if normalized["run_id"] == "" {
		t.Fatalf("expected non-empty run_id, got %v", normalized["run_id"])
	}
	if fingerprint == "" {
		t.Fatal("expected non-empty fingerprint")
	}
}

func TestCreateJobHandlerAllowsHealthCheckSmokeJob(t *testing.T) {
	cfg := config.FromEnv()
	if cfg.Database.DBPath == "" {
		cfg.Database.DBPath = filepath.Join(t.TempDir(), "velox.db")
	}
	db, err := store.NewSQLiteStore(cfg.Database.DBPath)
	if err != nil {
		t.Skipf("SQLite unavailable: %v", err)
	}
	ts, err := queue.NewTransitionService(store.NewSQLiteJobRepository(db), db)
	if err != nil {
		t.Skipf("Transition service unavailable: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: cfg.Workers.MaxJobAttempts, DBStore: db}, ts)
	if err != nil {
		t.Skipf("File queue unavailable: %v", err)
	}

	h := NewJobSubmissionHandler(cfg, q)
	r := gin.New()
	r.POST("/api/v1/jobs", h.CreateJobHandler())

	body, _ := json.Marshal(map[string]interface{}{
		"job_type":   "health_check",
		"video_name": "Smoke test health check",
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["status"] != "PENDING" {
		t.Fatalf("want PENDING, got %v", res["status"])
	}
	if res["job_id"] == "" {
		t.Fatal("expected non-empty job_id")
	}
}
