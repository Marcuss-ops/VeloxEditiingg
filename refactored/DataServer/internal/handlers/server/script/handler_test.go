package script

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
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

func TestGenerateWithImages_EnqueuesSceneImageJob(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
	if err != nil {
		t.Fatalf("new file queue: %v", err)
	}

	cfg := &config.Config{
		DataDir:   tempDir,
		VideosDir: filepath.Join(tempDir, "videos"),
		DBDSN:     dbPath,
	}

	r := gin.New()
	RegisterRoutes(r.Group("/api/script"), cfg, q, db)

	payload := map[string]interface{}{
		"video_name":          "Amish",
		"source_text":         "Se vuoi un test del flusso immagini+audio, questo payload usa gli esempi del messaggio.",
		"language":            "it",
		"voiceover_path":      "https://drive.google.com/file/d/17zAf__wEHsq6Wcs8Oguy7P9Ky_kH2CtV/view?usp=drive_link",
		"drive_output_folder": "https://drive.google.com/drive/u/1/folders/1W4k13-sjPCr1Lynu29D3UJSGRPFSoHal",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Se vi dicessi che esiste un angolo del mondo dove il tempo non è semplicemente rallentato.",
				"image_link": "https://drive.google.com/file/d/1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM/view",
				"image_links": []interface{}{
					"https://drive.google.com/file/d/1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM/view",
				},
			},
			map[string]interface{}{
				"text":       "Stiamo parlando degli Amish.",
				"image_link": "https://drive.google.com/file/d/1pZvMEF12yJgQ0trh8maIndU7JQnBGrkk/view",
				"image_links": []interface{}{
					"https://drive.google.com/file/d/1pZvMEF12yJgQ0trh8maIndU7JQnBGrkk/view",
				},
			},
			map[string]interface{}{
				"text":       "Il loro mondo è definito da una gerarchia sociale rigorosa e da regole non scritte.",
				"image_link": "",
			},
		},
	}
	body, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate-with-images", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	jobID, _ := res["job_id"].(string)
	if jobID == "" {
		t.Fatalf("expected non-empty job_id, got %#v", res["job_id"])
	}
	if got := res["video_mode"]; got != scriptSceneMode {
		t.Fatalf("want video_mode %q, got %v", scriptSceneMode, got)
	}
	if got := res["status"]; got != "PENDING" {
		t.Fatalf("want status PENDING, got %v", got)
	}
	if got := res["scene_count"]; got != float64(3) {
		t.Fatalf("want 3 scenes, got %v", got)
	}

	rawJob, err := db.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got := rawJob["video_mode"]; got != scriptSceneMode {
		t.Fatalf("want persisted video_mode %q, got %v", scriptSceneMode, got)
	}
	if got := rawJob["scenes_json"]; got == "" {
		t.Fatalf("want scenes_json persisted, got empty")
	}
	if got := rawJob["stock_clip_paths"]; got != nil {
		if arr, ok := got.([]interface{}); ok && len(arr) > 0 {
			t.Fatalf("want no stock clip paths for scene_image jobs, got %v", got)
		}
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/script/jobs/"+jobID, nil)
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status endpoint want 200, got %d body=%s", w.Code, w.Body.String())
	}
	var statusRes map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &statusRes); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if statusRes["ok"] != true {
		t.Fatalf("expected ok response, got %v", statusRes["ok"])
	}
	if statusRes["job_id"] != jobID {
		t.Fatalf("expected job_id %s, got %v", jobID, statusRes["job_id"])
	}
}
