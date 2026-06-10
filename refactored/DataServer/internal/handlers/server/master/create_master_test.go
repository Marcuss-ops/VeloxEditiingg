package master

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

func TestCreateMaster_InvalidJSON(t *testing.T) {
	cfg := config.FromEnv()
	q, err := queue.New(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	r := gin.New()
	r.POST("/api/video/create-master", CreateMaster(config.FromEnv(), q))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/video/create-master", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want status 400, got %d", w.Code)
	}
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["ok"] != false || body["error"] == nil {
		t.Errorf("want ok:false and error, got %v", body)
	}
}

func TestCreateMaster_Enqueue(t *testing.T) {
	cfg := config.FromEnv()
	q, err := queue.New(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	r := gin.New()
	r.POST("/api/video/create-master", CreateMaster(config.FromEnv(), q))

	payload := map[string]interface{}{
		"video_name":      "Test Video",
		"script_text":     "Hello world",
		"start_clips":     []interface{}{"https://example.com/clip1.mp4"},
		"voiceover_items": []interface{}{map[string]interface{}{"url": "https://example.com/vo.mp3"}},
	}
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/video/create-master", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want status 200, got %d body=%s", w.Code, w.Body.Bytes())
	}
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["ok"] != true {
		t.Errorf("want ok:true, got %v", res["ok"])
	}
	if res["job_id"] == nil || res["job_id"] == "" {
		t.Errorf("want non-empty job_id, got %v", res["job_id"])
	}
	if res["status"] != "PENDING" {
		t.Errorf("want status PENDING, got %v", res["status"])
	}
	if res["enqueue_confirmed"] != true {
		t.Errorf("want enqueue_confirmed:true, got %v", res["enqueue_confirmed"])
	}
}

func TestCreateMaster_ValidationNoClips(t *testing.T) {
	cfg := config.FromEnv()
	q, err := queue.New(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	r := gin.New()
	r.POST("/api/video/create-master", CreateMaster(config.FromEnv(), q))

	payload := map[string]interface{}{
		"video_name":  "No clips",
		"script_text": "Some script here that is long enough",
	}
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/video/create-master", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("want status 400 (no clips), got %d body=%s", w.Code, w.Body.Bytes())
	}
	var res map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	if res["error"] == nil {
		t.Errorf("want error message, got %v", res)
	}
}

func TestCreateMaster_MultiTitle(t *testing.T) {
	cfg := config.FromEnv()
	q, err := queue.New(cfg)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	r := gin.New()
	r.POST("/api/video/create-master", CreateMaster(config.FromEnv(), q))

	payload := map[string]interface{}{
		"titles":          []interface{}{"Title One", "Title Two"},
		"script_text":     "Script content that is long enough for validation",
		"start_clips":     []interface{}{"https://example.com/clip.mp4"},
		"voiceover_items": []interface{}{map[string]interface{}{"url": "https://example.com/vo.mp3"}},
	}
	body, _ := json.Marshal(payload)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/video/create-master", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("want status 200, got %d body=%s", w.Code, w.Body.Bytes())
	}
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if res["mode"] != "multi_title" {
		t.Errorf("want mode multi_title, got %v", res["mode"])
	}
	if res["total"] != 2.0 {
		t.Errorf("want total 2, got %v", res["total"])
	}
	results, _ := res["results"].([]interface{})
	if len(results) != 2 {
		t.Errorf("want 2 results, got %d", len(results))
	}
}
