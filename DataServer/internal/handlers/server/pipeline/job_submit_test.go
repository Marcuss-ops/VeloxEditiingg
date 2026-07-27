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

// TestLogHashIdempotencyKey locks the truncation invariant promised by the
// logHashIdempotencyKey helper. Without this, a future PR that flips to
// [:16] for "more entropy" or breaks on an upstream crypto library update
// goes unnoticed — the operator log line would silently change format.
//
// Three assertions:
//
//   (1) Length == 12 hex chars: matches the documented "48 bits of
//       entropy — ample to distinguish concurrent jobs" choice.
//
//   (2) Same input yields the same hash on every call. This locks the
//       SHA-256 determinism property that an operator relies on when
//       searching Loki / journald for a specific job.
//
//   (3) Distinct inputs yield distinct hashes. With SHA-256 truncated
//       to 48 bits, collision is theoretically possible but the test
//       catches accidental no-op regressions (e.g., helper returning
//       a constant or zero string) immediately.
func TestLogHashIdempotencyKey(t *testing.T) {
	t.Parallel()

	// (1) Length == 12 hex chars.
	got := logHashIdempotencyKey("video-001")
	if len(got) != 12 {
		t.Errorf("hash length = %d, want 12", len(got))
	}
	for _, c := range got {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("hash char %q is not lowercase hex", c)
			break
		}
	}

	// (2) Same input → same hash.
	if a, b := logHashIdempotencyKey("key-X"), logHashIdempotencyKey("key-X"); a != b {
		t.Errorf("hash not deterministic: %q vs %q", a, b)
	}

	// (3) Distinct inputs → distinct hashes.
	if a, b := logHashIdempotencyKey("video-001"), logHashIdempotencyKey("video-002"); a == b {
		t.Errorf("distinct inputs produced same hash: %q", a)
	}
}
