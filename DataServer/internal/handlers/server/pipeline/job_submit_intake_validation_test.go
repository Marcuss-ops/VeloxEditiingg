package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"velox-shared/compatibility"
)

func TestSubmitJobStrictModeRejectsRegisteredAlias(t *testing.T) {
	gin.SetMode(gin.TestMode)
	compatibility.SetMode(compatibility.ModeStrict)
	t.Cleanup(func() { compatibility.SetMode(compatibility.ModeCompat) })
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())
	body := []byte(`{"idempotency_key":"strict-alias-001","spec":{"voiceover_path":"legacy.mp3"},"scenes":[{"text":"scene","duration_seconds":5}]}`)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d; body=%s", w.Code, http.StatusUnprocessableEntity, w.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["error"] != "legacy_alias_rejected" {
		t.Fatalf("error = %v, want legacy_alias_rejected", response["error"])
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

// TestSubmitJobRejectsSubMinimumDuration locks the new lower-bound
// guard added by ValidateSubmitJobRequest (MinSceneDurationSeconds = 0.1).
// Without this test, a future contributor might widen the floor to
// 0.0 and silently let requests with `0.05` reach the resolver, where
// the worker has no useful frame to paint.
//
// 0.05 < 0.1 → 422.
func TestSubmitJobRejectsSubMinimumDuration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	body, _ := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "test-123",
		Scenes: []SubmitScene{
			{Text: "sub-min", DurationSeconds: 0.05},
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

// TestSubmitJobRejectsExcessiveDuration locks the new upper-bound
// guard (MaxSceneDurationSeconds = 86400). Anything larger would
// silently inflate server-side resource budgets (a 1e10-second
// scene would never finish a paint cycle). This test fires after
// our previous behaviour where DurationSeconds > 0 was the only
// guard.
func TestSubmitJobRejectsExcessiveDuration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	body, _ := json.Marshal(SubmitJobRequest{
		IdempotencyKey: "test-123",
		Scenes: []SubmitScene{
			{Text: "excessive", DurationSeconds: 86400.01},
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

// TestSubmitJobRejectsUnknownField locks the strict JSON decoder
// promise. The handler decodes the body with json.Decoder +
// DisallowUnknownFields(), so a typo (e.g. `ideliverency_key` for
// `idempotency_key`) fails with 400 invalid_json BEFORE any
// expensive downstream call. This protects external automation
// clients from silent "my key was ignored" bugs (which would
// otherwise produce a different idempotency_key-identity each
// request → one Job per request → the dedup promise broken).
func TestSubmitJobRejectsUnknownField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := &Handlers{}
	router.POST("/api/v1/jobs", h.SubmitJob())

	// Note the typo: `ideliverency_key` (extra `l` / shift).
	body := []byte(`{
		"ideliverency_key": "test-123",
		"scenes": [{"text": "s", "duration_seconds": 5}]
	}`)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	// The error code MUST be invalid_json so external clients can
	// distinguish a typo (unknown field) from a missing field
	// (which would have been visible in 422 invalid_payload had
	// the typo not been there).
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response unmarshal: %v", err)
	}
	if got := resp["error"]; got != "invalid_json" {
		t.Errorf("error = %v, want invalid_json", got)
	}
}

// TestSubmitJobValidateLowerBoundDuration locks the inclusive acceptance
// boundary for SubmitScene.duration_seconds at MinSceneDurationSeconds
// (= 0.1). Without this test a regression that flips `<` to `<=`
// silently rejects valid 0.1-second scenes on the inclusive boundary,
// breaking sub-second fine-grained montage cuts the Worker supports.
//
// 0.1 s MUST pass ValidateSubmitJobRequest without erroring. The handler
// test path can't reach 202 because a Handlers{} has no resolver wired,
// so this test asserts at the ValidateSubmitJobRequest layer instead —
// the layer that owns the rejection detection.
func TestSubmitJobValidateLowerBoundDuration(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "lb-001",
		Scenes: []SubmitScene{
			{Text: "sub-second", DurationSeconds: MinSceneDurationSeconds},
		},
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("%.4f s scene must be accepted at boundary, got error: %v",
			MinSceneDurationSeconds, verr)
	}
}

// TestSubmitJobValidateUpperBoundDuration mirrors the lower-bound test
// for MaxSceneDurationSeconds (= 86400).
func TestSubmitJobValidateUpperBoundDuration(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "ub-001",
		Scenes: []SubmitScene{
			{Text: "24h-timelapse", DurationSeconds: MaxSceneDurationSeconds},
		},
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("%.4f s scene must be accepted at boundary, got error: %v",
			MaxSceneDurationSeconds, verr)
	}
}

// TestSubmitJobValidateRejectsLongVideoName locks both the 422 path
// AND the details[].issue == "max_length" diagnostic shape.
func TestSubmitJobValidateRejectsLongVideoName(t *testing.T) {
	t.Parallel()

	longName := strings.Repeat("a", MaxVideoNameBytes+1)
	req := SubmitJobRequest{
		IdempotencyKey: "vn-001",
		VideoName:      longName,
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("video_name with %d bytes should be rejected", len(longName))
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "video_name" && d["issue"] == "max_length" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:video_name, issue:max_length}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateRejectsExcessiveScenes locks the 422 path
// AND the details[].issue == "max_items" diagnostic shape for
// MaxScenes = 10000.
func TestSubmitJobValidateRejectsExcessiveScenes(t *testing.T) {
	t.Parallel()

	scenes := make([]SubmitScene, MaxScenes+1)
	for i := range scenes {
		scenes[i] = SubmitScene{Text: "s", DurationSeconds: 5}
	}
	req := SubmitJobRequest{
		IdempotencyKey: "sc-001",
		Scenes:         scenes,
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("scenes with %d entries should be rejected", len(scenes))
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "scenes" && d["issue"] == "max_items" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:scenes, issue:max_items}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateRejectsEmptySceneText locks the cross-field rule
// that SubmitScene.text must be non-empty after trim.
func TestSubmitJobValidateRejectsEmptySceneText(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "st-001",
		Scenes: []SubmitScene{
			{Text: "", DurationSeconds: 5},
			{Text: "   ", DurationSeconds: 5}, // whitespace-only also rejected (trim policy).
		},
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty scene text must be rejected (bad=%v verr=%v)", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		p, _ := d["path"].(string)
		if len(p) > len("scenes.") && p[:len("scenes.")] == "scenes." && d["issue"] == "empty" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:scenes.<i>.text, issue:empty}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateRejectsEmptyDestinationID locks the cross-field
// rule that SubmitDeliveryPlanEntry.destination_id must be non-empty.
func TestSubmitJobValidateRejectsEmptyDestinationID(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "dd-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "", Priority: 1, RetryBudget: intPtr(3)},
		},
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty destination_id must be rejected (bad=%v verr=%v)", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		p, _ := d["path"].(string)
		if len(p) > len("delivery_plan.") && p[:len("delivery_plan.")] == "delivery_plan." && d["issue"] == "empty" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:delivery_plan.<i>.destination_id, issue:empty}, got: %+v", verr.Details)
	}
}
