package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSubmitJobBuildsWorkerPayloadCorrectly(t *testing.T) {
	req := &SubmitJobRequest{
		IdempotencyKey: "test-job-123",
		VideoName:      "Test Video",
		ScriptText:     "Hello world",
		VoiceoverPaths: []string{"velox-asset://voiceovers/intro.mp3"},
		Scenes: []SubmitScene{
			{
				Text:            "Scene 1",
				ClipLink:        "velox-asset://clips/scene1.mp4",
				DurationSeconds: 5,
			},
			{
				Text:            "Scene 2",
				ImageLink:       "velox-asset://images/scene2.jpg",
				DurationSeconds: 3,
			},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: 3},
		},
	}

	payload := buildWorkerPayloadFromSubmit(req)

	if payload["status"] != "completed" {
		t.Fatalf("status = %v, want completed", payload["status"])
	}
	if payload["job_id"] != "test-job-123" {
		t.Fatalf("job_id = %v, want test-job-123", payload["job_id"])
	}
	if payload["video_name"] != "Test Video" {
		t.Fatalf("video_name = %v, want Test Video", payload["video_name"])
	}
	if payload["script_text"] != "Hello world" {
		t.Fatalf("script_text = %v, want Hello world", payload["script_text"])
	}

	voiceovers, ok := payload["voiceover_paths"].([]string)
	if !ok || len(voiceovers) != 1 || voiceovers[0] != "velox-asset://voiceovers/intro.mp3" {
		t.Fatalf("voiceover_paths = %v, want [velox-asset://voiceovers/intro.mp3]", payload["voiceover_paths"])
	}

	scenes, ok := payload["scenes"].([]interface{})
	if !ok || len(scenes) != 2 {
		t.Fatalf("scenes length = %v, want 2", len(scenes))
	}

	scene1, ok := scenes[0].(map[string]interface{})
	if !ok {
		t.Fatal("scene[0] is not a map")
	}
	if scene1["text"] != "Scene 1" {
		t.Fatalf("scene[0].text = %v, want Scene 1", scene1["text"])
	}
	if scene1["clip_link"] != "velox-asset://clips/scene1.mp4" {
		t.Fatalf("scene[0].clip_link = %v, want velox-asset://clips/scene1.mp4", scene1["clip_link"])
	}

	deliveryPlan, ok := payload["delivery_plan"].([]interface{})
	if !ok || len(deliveryPlan) != 1 {
		t.Fatalf("delivery_plan length = %v, want 1", len(deliveryPlan))
	}
}

func TestTruncateKey(t *testing.T) {
	tests := []struct {
		s    string
		n    int
		want string
	}{
		{"short", 10, "short"},
		{"this-is-a-long-key", 10, "this-is-a-"},
		{"", 5, ""},
		{"abc", 3, "abc"},
	}

	for _, tt := range tests {
		got := truncateKey(tt.s, tt.n)
		if got != tt.want {
			t.Errorf("truncateKey(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
		}
	}
}

func TestSubmitJobRejectsEmptyIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	body, _ := json.Marshal(SubmitJobRequest{
		Scenes: []SubmitScene{{Text: "test", DurationSeconds: 5}},
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestSubmitJobRejectsNoScenes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	body, _ := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "test-123",
		Scenes:         []SubmitScene{},
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnprocessableEntity)
	}
}

func TestSubmitJobRejectsZeroDurationScene(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	body, _ := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "test-123",
		Scenes: []SubmitScene{
			{Text: "scene", DurationSeconds: 0},
		},
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusUnprocessableEntity)
	}
}
