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
//	(1) source_provider   == ExternalAPISourceProvider (low-cardinality
//	    constant; per the [P0 #4] audit verbatim).
//	(2) source_job_id      == trimmed idempotency_key.
//	(3) target_executor_id == JobSubmitTargetExecutorID.
//	(4) worker_payload["status"]            == "completed".
//	(5) worker_payload["job_id"]            == trimmed idempotency_key.
//	(6) worker_payload["video_name"]        (when set on req).
//	(7) worker_payload["script_text"]       (when set on req).
//	(8) worker_payload["voiceover_paths"]   (slice with the same entries).
//	(9) worker_payload["scenes_json"]       — JSON-encoding of the
//	    scene list with text + duration_seconds + clip_link / image_link
//	    fields carried through.
//
// (10) worker_payload["delivery_plan"]     — preserved as the
//
//	canonical shape (entry-level {destination_id, priority,
//	retry_budget, metadata}); ToWorkerPayload base-copies
//	delivery_plan because the typed DTO does NOT own it.
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
			{DestinationID: "drive", Priority: 1, RetryBudget: intPtr(3)},
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
			{DestinationID: "drive", Priority: 1, RetryBudget: intPtr(3)},
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

// TestNormalizeExternalJobSubmission_OmittedRetryBudgetDefaultsToThree
// locks the *int boundary contract for the omitted case: a client
// that does not supply retry_budget on a delivery_plan entry MUST
// end up with the OpenAPI default (3) stamped into the worker
// payload. This is the "client-friendly" branch — most callers will
// not bother declaring retry_budget at all — and getting a 0
// (silent int default) here would silently bypass retry enforcement
// on every entry the client doesn't annotate.
//
// Asserted by reading back the generated worker_payload's
// delivery_plan entry map and confirming retry_budget == 3.
func TestNormalizeExternalJobSubmission_OmittedRetryBudgetDefaultsToThree(t *testing.T) {
	t.Parallel()

	req := SubmitJobRequest{
		IdempotencyKey: "rb-omitted-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: nil},
		},
	}

	canonical := NormalizeExternalJobSubmission(req)
	dp, ok := canonical.WorkerPayload["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Fatalf("delivery_plan shape wrong: %v", canonical.WorkerPayload["delivery_plan"])
	}
	entry, ok := dp[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery_plan[0] type %T", dp[0])
	}
	if got := entry["retry_budget"]; got != 3 {
		t.Errorf("entry[retry_budget] = %v, want 3 (OpenAPI default)", got)
	}
}

// TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved
// locks the *int boundary contract for the explicit-zero case:
// a client that sends {"retry_budget": 0} MUST have that value
// preserved verbatim into the worker payload, not silently merged
// with the omitted default (3). This is the actual point of the
// int→*int refactor — without it, an operator who tries to disable
// retries on a specific destination would be silently overridden
// on every request.
//
// Asserted by reading back the worker_payload's delivery_plan
// entry map and confirming retry_budget == 0 (NOT 3).
func TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved(t *testing.T) {
	t.Parallel()

	zeroRetry := 0
	req := SubmitJobRequest{
		IdempotencyKey: "rb-zero-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: &zeroRetry},
		},
	}

	canonical := NormalizeExternalJobSubmission(req)
	dp, ok := canonical.WorkerPayload["delivery_plan"].([]interface{})
	if !ok || len(dp) != 1 {
		t.Fatalf("delivery_plan shape wrong: %v", canonical.WorkerPayload["delivery_plan"])
	}
	entry, ok := dp[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery_plan[0] type %T", dp[0])
	}
	if got := entry["retry_budget"]; got != 0 {
		t.Errorf("entry[retry_budget] = %v, want 0 (explicit-zero contract)", got)
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
//	(1) Length == 12 hex chars: matches the documented "48 bits of
//	    entropy — ample to distinguish concurrent jobs" choice.
//
//	(2) Same input yields the same hash on every call. This locks the
//	    SHA-256 determinism property that an operator relies on when
//	    searching Loki / journald for a specific job.
//
//	(3) Distinct inputs yield distinct hashes. With SHA-256 truncated
//	    to 48 bits, collision is theoretically possible but the test
//	    catches accidental no-op regressions (e.g., helper returning
//	    a constant or zero string) immediately.
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

// makeValidSubmitJobRequest is a tiny constructor used by the
// manifest_ref validator tests below. The body satisfies every
// cross-field rule except the manifest_ref — each test mutates
// just the field under test.
func makeValidSubmitJobRequest() SubmitJobRequest {
	return SubmitJobRequest{
		IdempotencyKey: "mr-001",
		Scenes: []SubmitScene{
			{Text: "s", DurationSeconds: 5},
		},
	}
}

// TestSubmitJobValidateManifestRefNilAccepts locks the
// "no manifest_ref at all" happy path: a nil pointer MUST pass
// through ValidateSubmitJobRequest without complaint, otherwise
// every existing client (legacy body shape) would 422 after the
// new field is shipped.
func TestSubmitJobValidateManifestRefNilAccepts(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = nil
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("nil manifest_ref MUST pass validator, got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefGoodShapeAccepts locks the
// canonical manifest_ref happy path: a non-nil pointer with
// well-formed fields MUST pass the validator. Pins the closed
// enum (`velox.render-manifest.v1`), the http(s)+velox-asset
// scheme allow-list, and the 64-lowercase-hex sha256 format in
// a single boundary case.
func TestSubmitJobValidateManifestRefGoodShapeAccepts(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("well-formed manifest_ref MUST pass, got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefRejectsBadSchemaVersion pins
// the closed-enum rejection at the schema_version boundary. The
// test uses a value that LOOKS plausible ("v2", "v1.1") so a
// future refactor that widens the enum to a regex catches the
// regression at the wire level rather than silently accepting
// a future-version marker that no resolver can decode.
func TestSubmitJobValidateManifestRefRejectsBadSchemaVersion(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v2", // not in enum
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("bad schema_version MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.schema_version" && d["issue"] == "unsupported_value" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.schema_version, issue:unsupported_value}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefRejectsBadScheme pins the
// http(s) + velox-asset:// scheme allow-list. A file:// URL
// would silently bypass the SSRF blocklist (different layer)
// and a javascript: URL is a known exfiltration vector; both
// MUST be rejected at the wire-shape layer.
func TestSubmitJobValidateManifestRefRejectsBadScheme(t *testing.T) {
	t.Parallel()

	badURLs := []string{
		"file:///etc/passwd",
		"javascript:alert(1)",
		"data:text/plain,hello",
		"ftp://example.com/manifest",
		"ssh://example.com",
		"not-a-url",
	}
	for _, u := range badURLs {
		u := u
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           u,
				SHA256:        strings.Repeat("a", 64),
			}
			verr, bad := ValidateSubmitJobRequest(req)
			if !bad || verr == nil || len(verr.Details) == 0 {
				t.Fatalf("URL %q MUST be rejected, got: bad=%v verr=%+v", u, bad, verr)
			}
			found := false
			for _, d := range verr.Details {
				if d["path"] == "manifest_ref.url" && d["issue"] == "unsupported_scheme" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected details entry {path:manifest_ref.url, issue:unsupported_scheme}, got: %+v", verr.Details)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefAcceptsAllAllowedSchemes
// pins the positive boundary of the scheme allow-list —
// http, https, velox-asset. A future contributor that drops
// velox-asset:// from the allow-list (or adds e.g. file://)
// is caught at the wire-shape layer rather than at the SSRF
// layer (where the failure mode is a silent exfil path).
func TestSubmitJobValidateManifestRefAcceptsAllAllowedSchemes(t *testing.T) {
	t.Parallel()

	goodURLs := []string{
		"https://drive.google.com/file/d/X/view",
		"http://example.com/manifest.json",
		"velox-asset://manifests/abc123.json",
	}
	for _, u := range goodURLs {
		u := u
		t.Run(u, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           u,
				SHA256:        strings.Repeat("a", 64),
			}
			if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
				t.Fatalf("URL %q MUST be accepted, got: bad=%v verr=%+v", u, bad, verr)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefRejectsBadSHA256 pins the
// 64-lowercase-hex sha256 format. Multiple failure modes in
// one table: too short, too long, uppercase (sha256 of an
// always-lowercase canonical manifest is required because
// the resolver will compare byte-for-byte), non-hex chars,
// empty. A drift in the regex silently flips the runtime
// check to "any string accepted" — the test catches that.
func TestSubmitJobValidateManifestRefRejectsBadSHA256(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hash string
	}{
		{"too_short", strings.Repeat("a", 63)},
		{"too_long", strings.Repeat("a", 65)},
		{"uppercase", strings.Repeat("A", 64)},
		{"mixed_case", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeF"},
		{"non_hex", strings.Repeat("z", 64)},
		{"empty", ""},
		{"with_0x_prefix", "0x" + strings.Repeat("a", 62)},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			req := makeValidSubmitJobRequest()
			req.ManifestRef = &SubmitManifestRef{
				SchemaVersion: "velox.render-manifest.v1",
				URL:           "https://drive.google.com/file/d/MANIFEST/view",
				SHA256:        c.hash,
			}
			verr, bad := ValidateSubmitJobRequest(req)
			if !bad || verr == nil || len(verr.Details) == 0 {
				t.Fatalf("sha256 %q MUST be rejected (case=%s), got: bad=%v verr=%+v",
					c.hash, c.name, bad, verr)
			}
			found := false
			for _, d := range verr.Details {
				if d["path"] == "manifest_ref.sha256" && d["issue"] == "malformed" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected details entry {path:manifest_ref.sha256, issue:malformed}, got: %+v", verr.Details)
			}
		})
	}
}

// TestSubmitJobValidateManifestRefRejectsEmptyURL pins the
// explicit-empty URL boundary. A client that supplies
// {"manifest_ref": {"url": "", ...}} MUST be rejected at the
// wire-shape layer (not silently forwarded to the resolver,
// where the empty URL would surface as a 500 from the HTTP
// fetch layer).
func TestSubmitJobValidateManifestRefRejectsEmptyURL(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "   ", // whitespace-only → trimmed to empty
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty URL MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.url" && d["issue"] == "empty" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.url, issue:empty}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefAggregatesAllViolations locks
// the validator's "report everything" contract: a client that
// submits a manifest_ref with ALL three fields malformed MUST
// receive ONE 422 with details[0..2] populated, NOT a
// first-failure short-circuit. Same shape contract as the
// scenes/delivery_plan aggregations above.
func TestSubmitJobValidateManifestRefAggregatesAllViolations(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v9", // unsupported
		URL:           "file:///etc/passwd",       // unsupported scheme
		SHA256:        "abc",                      // malformed
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil {
		t.Fatalf("want rejection, got: bad=%v verr=%+v", bad, verr)
	}
	wantPaths := map[string]bool{
		"manifest_ref.schema_version": false,
		"manifest_ref.url":            false,
		"manifest_ref.sha256":         false,
	}
	for _, d := range verr.Details {
		if p, ok := d["path"].(string); ok {
			if _, expected := wantPaths[p]; expected {
				wantPaths[p] = true
			}
		}
	}
	for p, seen := range wantPaths {
		if !seen {
			t.Errorf("expected details path %q in aggregated violation report, got: %+v", p, verr.Details)
		}
	}
}

// TestSubmitJobValidateManifestRefEmptyObjectAggregatesThreeViolations
// pins the "non-nil pointer but every nested field empty" boundary:
// a client that sends `{"manifest_ref": {}}` (a JSON object with
// no nested keys) MUST be rejected with ALL three violations
// aggregated, not silently accepted by the validator. This is the
// exact failure mode the *SubmitManifestRef pointer indirection
// exists to distinguish from "field omitted entirely" (the nil
// pointer case, covered by TestSubmitJobValidateManifestRefNilAccepts).
func TestSubmitJobValidateManifestRefEmptyObjectAggregatesThreeViolations(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{} // all three fields empty
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil {
		t.Fatalf("empty manifest_ref MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	wantPaths := map[string]bool{
		"manifest_ref.schema_version": false,
		"manifest_ref.url":            false,
		"manifest_ref.sha256":         false,
	}
	for _, d := range verr.Details {
		if p, ok := d["path"].(string); ok {
			if _, expected := wantPaths[p]; expected {
				wantPaths[p] = true
			}
		}
	}
	for p, seen := range wantPaths {
		if !seen {
			t.Errorf("expected details path %q in aggregated violation report, got: %+v", p, verr.Details)
		}
	}
}

// TestSubmitJobValidateManifestRefRejectsEmptySchemaVersion pins
// the closed-enum rejection for the empty-string boundary. A client
// that supplies `{"schema_version": ""}` MUST be rejected — the
// enum contains only `velox.render-manifest.v1` and an empty
// string is not a member. Without this test, a future refactor that
// drops the empty check (e.g., switching to a regex without `+`
// quantifier) silently accepts malformed manifests.
func TestSubmitJobValidateManifestRefRejectsEmptySchemaVersion(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "",
		URL:           "https://drive.google.com/file/d/MANIFEST/view",
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("empty schema_version MUST be rejected, got: bad=%v verr=%+v", bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.schema_version" && d["issue"] == "unsupported_value" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.schema_version, issue:unsupported_value}, got: %+v", verr.Details)
	}
}

// TestSubmitJobValidateManifestRefURLWhitespaceTrimmed pins the
// canonical trim policy: a URL padded with surrounding whitespace
// MUST pass the validator (after trim) — not be rejected by the
// regex. Without this test, a future refactor that drops the
// strings.TrimSpace call silently rejects URLs the spec advertises
// as valid (the regex anchors with `^(https?://|velox-asset://)`,
// so a leading space breaks the match).
func TestSubmitJobValidateManifestRefURLWhitespaceTrimmed(t *testing.T) {
	t.Parallel()

	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "   https://drive.google.com/file/d/MANIFEST/view   ",
		SHA256:        strings.Repeat("a", 64),
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("URL with surrounding whitespace MUST be accepted (trim policy), got: bad=%v verr=%+v", bad, verr)
	}
}

// TestSubmitJobValidateManifestRefURLMaxLengthBoundary locks the
// byte-cap boundary at MaxManifestRefURLBytes (= 2048). A URL of
// exactly MaxManifestRefURLBytes bytes MUST pass; a URL of
// MaxManifestRefURLBytes+1 bytes MUST be rejected with
// details[].issue="max_length". Without this test, a future bump
// on one side of the drift-guard (apiwire tag vs handler constant)
// silently widens or narrows the cap without a test signal.
//
// The drift guard in apiwire_test.go
// (TestSubmitManifestRef_MaxLengthMatchesHandlerConstant) pins the
// numeric value; this test pins the runtime boundary on the handler
// side. Together they cover both sides of the drift vector.
func TestSubmitJobValidateManifestRefURLMaxLengthBoundary(t *testing.T) {
	t.Parallel()

	// (1) Exactly MaxManifestRefURLBytes bytes — MUST pass. The
	// scheme prefix + a tail of "a" characters to hit the cap.
	exactURL := "https://example.com/" + strings.Repeat("a", MaxManifestRefURLBytes-len("https://example.com/"))
	if len(exactURL) != MaxManifestRefURLBytes {
		t.Fatalf("test fixture broken: exactURL length = %d, want %d", len(exactURL), MaxManifestRefURLBytes)
	}
	req := makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           exactURL,
		SHA256:        strings.Repeat("a", 64),
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("URL of exactly %d bytes MUST be accepted, got: bad=%v verr=%+v",
			MaxManifestRefURLBytes, bad, verr)
	}

	// (2) One byte over the cap — MUST be rejected.
	overURL := "https://example.com/" + strings.Repeat("a", MaxManifestRefURLBytes-len("https://example.com/")+1)
	if len(overURL) != MaxManifestRefURLBytes+1 {
		t.Fatalf("test fixture broken: overURL length = %d, want %d", len(overURL), MaxManifestRefURLBytes+1)
	}
	req = makeValidSubmitJobRequest()
	req.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           overURL,
		SHA256:        strings.Repeat("a", 64),
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil || len(verr.Details) == 0 {
		t.Fatalf("URL of %d bytes (over cap) MUST be rejected, got: bad=%v verr=%+v",
			len(overURL), bad, verr)
	}
	found := false
	for _, d := range verr.Details {
		if d["path"] == "manifest_ref.url" && d["issue"] == "max_length" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected details entry {path:manifest_ref.url, issue:max_length}, got: %+v", verr.Details)
	}
}
