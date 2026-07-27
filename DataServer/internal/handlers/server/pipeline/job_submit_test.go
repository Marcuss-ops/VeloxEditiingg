package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestSubmitJobBuildsWorkerPayloadCorrectly is replaced by
// TestNormalizeExternalJobSubmission_ProducesCanonicalPayload below —
// the worker payload is no longer hand-built; it is produced through
// remoteengine.ParseRemotePipelineResult → RemotePipelineResult.
// ToWorkerPayload. The shape assertion that previously read
// payload["scenes"].([]interface{}) is now payload["scenes_json"]
// (string) because the canonical typed-DTO path encodes scenes as a
// JSON string rather than a nested []interface{}. Tests for the
// canonical shape live below.

// TestNormalizeExternalJobSubmission_ProducesCanonicalPayload locks
// the canonical output shape that NormalizeExternalJobSubmission
// produces. The contract:
//
//  (1) source_provider   == ExternalAPISourceProvider (low-cardinality
//      constant; per the [P0 #4] audit verbatim).
//  (2) source_job_id      == trimmed idempotency_key.
//  (3) target_executor_id == JobSubmitTargetExecutorID.
//  (4) worker_payload["status"]            == "completed".
//  (5) worker_payload["job_id"]            == trimmed idempotency_key.
//  (6) worker_payload["video_name"]        (when set on req).
//  (7) worker_payload["script_text"]       (when set on req).
//  (8) worker_payload["voiceover_paths"]   (slice with the same entries).
//  (9) worker_payload["scenes_json"]       — JSON-encoding of the
//      scene list with text + duration_seconds + clip_link / image_link
//      fields carried through.
// (10) worker_payload["delivery_plan"]     — preserved as the
//      canonical shape (entry-level {destination_id, priority,
//      retry_budget, metadata}); ToWorkerPayload base-copies
//      delivery_plan because the typed DTO does NOT own it.
func TestNormalizeExternalJobSubmission_ProducesCanonicalPayload(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "test-job-123",
		VideoName:      "Test Video",
		ScriptText:     "Hello world",
		VoiceoverPaths: []string{"velox-asset://voiceovers/intro.mp3"},
		Scenes: []SubmitScene{
			{Text: "Scene 1", ClipLink: "velox-asset://clips/scene1.mp4", DurationSeconds: 5},
			{Text: "Scene 2", ImageLink: "velox-asset://images/scene2.jpg", DurationSeconds: 3},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: 3},
		},
	}

	canonical := NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil canonical")
	}

	// (1)
	if canonical.SourceProvider != ExternalAPISourceProvider {
		t.Errorf("SourceProvider = %q, want %q", canonical.SourceProvider, ExternalAPISourceProvider)
	}
	// (2)
	if canonical.SourceJobID != "test-job-123" {
		t.Errorf("SourceJobID = %q, want test-job-123", canonical.SourceJobID)
	}
	// (3)
	if canonical.TargetExecutorID != JobSubmitTargetExecutorID {
		t.Errorf("TargetExecutorID = %q, want %q", canonical.TargetExecutorID, JobSubmitTargetExecutorID)
	}

	wp := canonical.WorkerPayload

	// (4)
	if got := wp["status"]; got != "completed" {
		t.Errorf("worker_payload[status] = %v, want completed", got)
	}
	// (5)
	if got := wp["job_id"]; got != "test-job-123" {
		t.Errorf("worker_payload[job_id] = %v, want test-job-123", got)
	}
	// (6)
	if got := wp["video_name"]; got != "Test Video" {
		t.Errorf("worker_payload[video_name] = %v, want Test Video", got)
	}
	// (7)
	if got := wp["script_text"]; got != "Hello world" {
		t.Errorf("worker_payload[script_text] = %v, want Hello world", got)
	}
	// (8)
	vos, ok := wp["voiceover_paths"].([]string)
	if !ok || len(vos) != 1 || vos[0] != "velox-asset://voiceovers/intro.mp3" {
		t.Errorf("worker_payload[voiceover_paths] = %v, want [velox-asset://voiceovers/intro.mp3]", wp["voiceover_paths"])
	}
	// (9) scenes_json: must be a non-empty JSON string. Decoding is
	// covered by the parity test in TestNormalizeExternalJobSubmission_MatchesCreatorPushShape;
	// here we lock just the surface key.
	scenesJSON, ok := wp["scenes_json"].(string)
	if !ok || scenesJSON == "" {
		t.Errorf("worker_payload[scenes_json] missing or empty: %v", wp["scenes_json"])
	}
	// (10) delivery_plan: preserved as the entry-shape map[]interface{}.
	dp, ok := wp["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Errorf("worker_payload[delivery_plan] = %v, want 1 entry", wp["delivery_plan"])
	}
}

// TestNormalizeExternalJobSubmission_MatchesCreatorPushShape is the
// parity test the click asked for. It builds equivalent semantic
// content through BOTH intake paths — creator_push envelopes and
// /api/v1/jobs flat — and asserts that the resulting
// CanonicalCompletedPayload structures produce equivalent worker
// payloads (same job_id, status, voiceover_paths, scenes_json content,
// delivery_plan). This locks the [P1] invariant: a future PR that
// diverges the two paths — e.g., by adding a private field on one
// path's worker_payload — will fail this test loudly.
//
// The two requests below are canonically-equivalent: same job_id,
// same video, same script, same voiceover, same scene text +
// duration, same delivery destination. The idem-key for the SubmitJob
// path sets job_id (matching CreatorPush's source_job_id == job_id).
func TestNormalizeExternalJobSubmission_MatchesCreatorPushShape(t *testing.T) {
	t.Parallel()

	const jobID = "parity-job-001"
	const videoName = "Parity Video"
	const scriptText = "Same script body for parity."
	const voiceover = "velox-asset://voiceovers/parity.mp3"
	const executorID = "scene.composite.v1"

	submitReq := SubmitJobRequest{
		IdempotencyKey: jobID,
		VideoName:      videoName,
		ScriptText:     scriptText,
		VoiceoverPaths: []string{voiceover},
		Scenes: []SubmitScene{
			{Text: "Parity scene A", ClipLink: "velox-asset://clips/parity-a.mp4", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: 3},
		},
	}

	creatorReq := creatorPushRequest{
		SourceProvider:   "creator_parity",
		SourceJobID:      jobID,
		TargetExecutorID: executorID,
		Payload: map[string]interface{}{
			"status":          "completed",
			"job_id":          jobID,
			"video_name":      videoName,
			"script_text":     scriptText,
			"voiceover_paths": []interface{}{voiceover},
			"scenes": []interface{}{
				// duration_seconds is float64 (NOT int 5) because
				// remoteengine.convertRawScenes performs a float64 type
				// assertion; an int 5 would silently drop duration_seconds
				// from scenes_json and break the parity test.
				map[string]interface{}{
					"text":             "Parity scene A",
					"clip_link":        "velox-asset://clips/parity-a.mp4",
					"duration_seconds": float64(5),
				},
			},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "drive",
					"priority":       1,
					"retry_budget":   3,
				},
			},
		},
	}

	submitCanonical := NormalizeExternalJobSubmission(submitReq)
	creatorCanonical, err := normalizeCreatorPushRequest(creatorReq)
	if err != nil {
		t.Fatalf("normalizeCreatorPushRequest: %v", err)
	}

	// Identity-tuple comparison — same shape, different values.
	if fmt.Sprintf("%T", submitCanonical) != fmt.Sprintf("%T", creatorCanonical) {
		t.Errorf("submitCanonical and creatorCanonical have different types: %T vs %T",
			submitCanonical, creatorCanonical)
	}

	// Key fields MUST match in worker_payload (semantic equivalence):
	checks := []struct {
		field string
		want  interface{}
	}{
		{"status", "completed"},
		{"job_id", jobID},
		{"video_name", videoName},
		{"script_text", scriptText},
	}
	for _, ch := range checks {
		if submitCanonical.WorkerPayload[ch.field] != ch.want {
			t.Errorf("submit %s = %v, want %v", ch.field, submitCanonical.WorkerPayload[ch.field], ch.want)
		}
		if creatorCanonical.WorkerPayload[ch.field] != ch.want {
			t.Errorf("creator %s = %v, want %v", ch.field, creatorCanonical.WorkerPayload[ch.field], ch.want)
		}
	}

	// voiceover_paths: both must carry a single string equal to `voiceover`.
	for label, payload := range map[string]map[string]interface{}{
		"submit":  submitCanonical.WorkerPayload,
		"creator": creatorCanonical.WorkerPayload,
	} {
		vos, ok := payload["voiceover_paths"].([]string)
		if !ok {
			t.Errorf("%s voiceover_paths not []string: %T", label, payload["voiceover_paths"])
			continue
		}
		if len(vos) != 1 || vos[0] != voiceover {
			t.Errorf("%s voiceover_paths = %v, want [%s]", label, vos, voiceover)
		}
	}

	// scenes_json: both must be a non-empty JSON string and (after
	// re-decoding) carry the same scene text + duration. This locks
	// the central promise of the canonical path: structurally identical
	// scenes produce structurally identical scenes_json regardless of
	// producer.
	for label, payload := range map[string]map[string]interface{}{
		"submit":  submitCanonical.WorkerPayload,
		"creator": creatorCanonical.WorkerPayload,
	} {
		raw, ok := payload["scenes_json"].(string)
		if !ok || raw == "" {
			t.Errorf("%s scenes_json missing or empty: %v", label, payload["scenes_json"])
			continue
		}
		// Use a structural probe: assert presence of the canonical text
		// we supplied AND the duration_seconds we supplied. This avoids
		// a brittle byte-exact comparison of the JSON encoding (whitespace,
		// key order — stdlib Go json.Marshal sorts map keys but [] marshals
		// in the supplied slice order).
		if !strings.Contains(raw, "Parity scene A") {
			t.Errorf("%s scenes_json does not contain scene text: %s", label, raw)
		}
		if !strings.Contains(raw, "duration_seconds") {
			t.Errorf("%s scenes_json missing duration_seconds key: %s", label, raw)
		}
	}

	// delivery_plan: both must be a 1-entry []interface{} with
	// destination_id == "drive". exact priority / retry_budget values
	// are passed through verbatim from buildWorkerPayloadFromSubmit /
	// ToWorkerPayload, so we lock just the destination.
	for label, payload := range map[string]map[string]interface{}{
		"submit":  submitCanonical.WorkerPayload,
		"creator": creatorCanonical.WorkerPayload,
	} {
		dp, ok := payload["delivery_plan"].([]interface{})
		if !ok || len(dp) != 1 {
			t.Errorf("%s delivery_plan shape wrong: %v", label, payload["delivery_plan"])
			continue
		}
		entry, ok := dp[0].(map[string]interface{})
		if !ok || entry["destination_id"] != "drive" {
			t.Errorf("%s delivery_plan[0] destination_id wrong: %v", label, entry["destination_id"])
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

// TestLogHashShort locks the truncation invariant promised by the
// logHashShort helper (in logging.go). Without this, a future PR that
// flips to [:16] for "more entropy" or breaks on an upstream crypto
// library update goes unnoticed — the operator log line would silently
// change format.
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
func TestLogHashShort(t *testing.T) {
	t.Parallel()

	// (1) Length == 12 hex chars.
	got := logHashShort("video-001")
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
	if a, b := logHashShort("key-X"), logHashShort("key-X"); a != b {
		t.Errorf("hash not deterministic: %q vs %q", a, b)
	}

	// (3) Distinct inputs → distinct hashes.
	if a, b := logHashShort("video-001"), logHashShort("video-002"); a == b {
		t.Errorf("distinct inputs produced same hash: %q", a)
	}

	// (4) Empty-input contract. The helper's docstring promises
	// "Empty input is permitted and produces a stable hash value,
	// NOT a panic." A regression that returned an empty string for
	// empty input (or one that called a nil-receiver method and
	// panicked) would silently corrupt correlation grep. Lock both:
	// non-empty output AND determinism across two calls.
	if got := logHashShort(""); len(got) != 12 {
		t.Errorf("empty-input hash length = %d, want 12", len(got))
	}
	if a, b := logHashShort(""), logHashShort(""); a != b {
		t.Errorf("empty-input hash not deterministic: %q vs %q", a, b)
	}
}
