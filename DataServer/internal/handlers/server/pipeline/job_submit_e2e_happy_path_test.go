// Package pipeline — e2e happy-path scenario tests for POST /api/v1/jobs.
//
// This file owns:
//   - the package-level auth fake m2mJobsAuthFake (passthrough for the resolver-layer happy path);
//   - the test stack factory newSubmitJobE2EStack + helpers validSubmitJobBody /
//     expectedSubmitJobID / postSubmitJob / countRowsByJobID (shared across the
//     other e2e files in this package);
//   - scenario 1+2+3 of the submit-job e2e coverage matrix
//     (TestSubmitJobE2E_SuccessAndReplay: happy / identical replay / hash-conflict).
//
// Other e2e scenarios live in:
//   - job_submit_e2e_validation_test.go      (ValidationFailures + NegativeRetryBudgetRejected)
//   - job_submit_e2e_m2m_auth_test.go        (M2MAuthEnvelopes + M2MRateLimitAndQuota)
//   - job_submit_e2e_polling_test.go         (PollingChain_HappyPath + PollingChain_NotFound)
//   - job_submit_e2e_retry_budget_test.go    (RetryBudgetZeroAcceptance)
package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-server/internal/store"
	"velox-shared/contract"
)

func m2mJobsAuthFake(c *gin.Context) { c.Next() }
func newSubmitJobE2EStack(t *testing.T) (*Handlers, *store.SQLiteStore) {
	t.Helper()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	if _, err := db.DB().Exec(
		`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('drive', 'google_drive', 'Drive', 1, '{}', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	testEnqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db.Forwarding(), db, tempDir, filepath.Join(tempDir, "videos"), "")
	h := NewHandlersWithResolver(&config.Config{}, testEnqueuer, nil, resolver, jobRepo, nil, nil).WithStore(db)
	return h, db
}

// validSubmitJobBody returns a known-good SubmitJobRequest that passes
// every validator AND the enqueue-layer completeness guard + SSRF
// policy AND per-request quota. Tests mutate a single field before
// JSON-encoding; sharing a single base fixture ensures the happy path
// and the rejects hit the SAME initial envelope shape, so a drift in
// the fixture itself becomes a single, loud test failure.
//
// IMPORTANT — voiceover_paths is non-nil here. The enqueue-layer
// completeness check requires a non-empty voiceover reference; an
// earlier draft of this fixture omitting this field caused the
// happy-path POST to return 422 payload_incomplete instead of 202.
//
// IMPORTANT — the SSRF URL validator accepts `velox-asset://` as
// always-safe; HTTP/HTTPS public hosts pass when
// cfg.AllowedExternalDomains is empty (blocklist-only mode).
//
// retry_budget is *int(3) here (not nil, not &zero). The handler now
// accepts RetryBudget == 0 (preserved verbatim) per the relaxed
// P0 retry_budget contract. Scenario 5 (TestSubmitJobE2E_
// RetryBudgetZeroAcceptance) below covers the explicit-zero
// round-trip end-to-end.
func validSubmitJobBody(idemKey string) SubmitJobRequest {
	rb := 3
	return SubmitJobRequest{
		IdempotencyKey: idemKey,
		VideoName:      "E2E Submit Test",
		ScriptText:     "Submitted via POST /api/v1/jobs e2e harness.",
		Scenes: []SubmitScene{
			{
				Text:            "Opening scene",
				Clip:            &SubmitClip{URL: "velox-asset://clips/opening.mp4"},
				Voiceover:       &SubmitVoiceover{URL: "velox-asset://voiceovers/opening.mp3"},
				DurationSeconds: 3.5,
			},
		},
		DeliveryPlan: []SubmitDeliveryPlanEntry{
			{DestinationID: "drive", Priority: 1, RetryBudget: &rb},
		},
	}
}

func validProducerOwnedFMP4Plan(t *testing.T) (string, string) {
	t.Helper()
	const durationUS int64 = 1_000_000
	const timelineRevision int64 = 1
	timelineSHA := strings.Repeat("c", 64)
	videoSHA := strings.Repeat("b", 64)
	audioSHA := strings.Repeat("a", 64)
	profile := contract.CanonicalVideoProfileFMP4StreamV1Default

	plan := &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: timelineRevision,
		TimelineSHA256:   timelineSHA,
		DurationUS:       durationUS,
		Output: contract.OutputContractV2{
			Container:    profile.Container,
			VideoCodec:   profile.Codec,
			Width:        profile.Width,
			Height:       profile.Height,
			FPSNum:       profile.FPSNum,
			FPSDen:       profile.FPSDen,
			PixelFormat:  profile.PixelFormat,
			ProfileID:    profile.ProfileID,
			CodecProfile: profile.CodecProfile,
			CodecLevel:   profile.CodecLevel,
			GOPSize:      profile.GOPSize,
			BFrames:      profile.BFrames,
			ClosedGOP:    profile.ClosedGOP,
			TimeBaseNum:  profile.TimeBaseNum,
			TimeBaseDen:  profile.TimeBaseDen,
		},
		FinalAudio: contract.FinalAudioV2{
			Mode:             contract.AudioModeFinalAudioCopy,
			AssetID:          "audio-final",
			SHA256:           audioSHA,
			SizeBytes:        200,
			Codec:            "aac",
			SampleRateHz:     48_000,
			Channels:         2,
			DurationUS:       durationUS,
			TimelineRevision: timelineRevision,
			TimelineSHA256:   timelineSHA,
		},
		VideoTracks: []contract.VideoTrackV2{{
			TrackID: "main",
			Segments: []contract.VideoSegmentV2{{
				SegmentID:          "segment-0",
				AssetID:            "video-source",
				SHA256:             videoSHA,
				TimelineStartFrame: 0,
				FrameCount:         24,
				SourceInUS:         0,
				SourceDurationUS:   durationUS,
			}},
		}},
		Assets: []contract.AssetRefV2{
			{AssetID: "video-source", SHA256: videoSHA, SizeBytes: 100, Kind: "video", DurationUS: durationUS, Width: profile.Width, Height: profile.Height},
			{AssetID: "audio-final", SHA256: audioSHA, SizeBytes: 200, Kind: "final_audio", MIME: "audio/mp4", DurationUS: durationUS},
		},
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("test fMP4 plan failed contract validation: %v", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical fMP4 plan: %v", err)
	}
	sha, err := plan.PlanSHA256()
	if err != nil {
		t.Fatalf("fMP4 plan SHA: %v", err)
	}
	return string(canonical), sha
}

// expectedSubmitJobID derives the canonical job_id the Resolver
// produces for the SubmitJob entry point. The identity tuple is locked
// by job_submit.go's ExternalAPISourceProvider and JobSubmitTargetExecutorID
// constants; re-using them here means a future constant drift is
// caught by this test the same way it is caught by ValidateSubmitJobRequest.
func expectedSubmitJobID(idem string) string {
	return enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey(
			ExternalAPISourceProvider,
			idem,
			JobSubmitTargetExecutorID,
		).String(),
	)
}

// m2mPost is postSubmitJob with the M2M middleware wired. Same
// body-shape helpers, different auth header so the M2M middleware
// does its scope/rate-limit/audit work. Used by scenarios 11 + 12
// tests.
func postSubmitJob(t *testing.T, r *gin.Engine, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer m2mJobsAuthFake-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// countRowsByJobID is a tiny SELECT helper for the success-path
// audit (forwardings / jobs / tasks count by job_id). The
// rejection-path suite uses a baseline-snapshot approach instead —
// per-idem WHERE clauses silently miss rows under non-idem id shapes
// (the Resolver produces hash-based `job_<sha>` IDs).
func countRowsByJobID(t *testing.T, db *store.SQLiteStore, table, jobCol string, jobID string) int {
	t.Helper()
	var n int
	q := "SELECT COUNT(*) FROM " + table + " WHERE " + jobCol + " = ?"
	if err := db.DB().QueryRowContext(context.Background(), q, jobID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// ── Scenarios 1, 2, 3 — happy / replay / hash-conflict ──

// TestSubmitJobE2E_SuccessAndReplay covers three scenarios in a
// single stack so they share the same Resolver + SQLite transactions
// (the replay and hash-conflict cases by definition operate on the
// same idempotency_key as the happy path).
//
//  1. Happy path — POST /api/v1/jobs. Asserts the canonical envelope,
//     exactly 1 forwarding / 1 job / 1 task / 1 task_spec /
//     1 job_delivery_plans row. Asserts retry_budget was preserved
//     through to the SQL row (current behavior: the OpenAPI default
//     3 since the handler rejects explicit 0 — see header).
//
//  2. Identical replay — POST the same body again. Asserts same
//     job_id, created=false, zero new rows in any of the 5 tables.
//
//  3. Same key + different payload — POST a body whose worker_payload
//     hashes differently. Asserts 409 idempotency_key_reused and zero
//     new rows (any forward-write would be a P0 invariant violation).
func TestSubmitJobE2E_SuccessAndReplay(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	const idem = "e2e-success-001"
	body := validSubmitJobBody(idem)

	wantJobID := expectedSubmitJobID(idem)

	// ── Scenario 1 ──
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("first POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	wantFields := map[string]interface{}{
		"ok":              true,
		"accepted_from":   "api_v1_jobs",
		"idempotency_key": idem,
		"job_id":          wantJobID,
		"dispatch_status": "queued_for_workers",
	}
	for key, want := range wantFields {
		if resp[key] != want {
			t.Fatalf("response[%q] = %v, want %v (full body: %s)", key, resp[key], want, w.Body.String())
		}
	}

	// Forwarding: exactly one row matching (external_api, idem, scene.composite.v1).
	var fwdCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		ExternalAPISourceProvider, idem, JobSubmitTargetExecutorID,
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("want exactly 1 creator_forwardings row, got %d", fwdCount)
	}

	// Job: exactly one row, status=PENDING.
	job, err := store.NewSQLiteJobRepository(db).Get(context.Background(), wantJobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job == nil {
		t.Fatal("jobs row not persisted")
	}
	if job.Status != jobs.StatusPending {
		t.Fatalf("job status = %v, want PENDING", job.Status)
	}

	// Tasks + task_specs: exactly one of each. task_specs joins via tasks
	// because task_specs is keyed by task_id (no job_id column).
	if got := countRowsByJobID(t, db, "tasks", "job_id", wantJobID); got != 1 {
		t.Fatalf("tasks rows = %d, want 1", got)
	}
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM task_specs WHERE task_id IN (SELECT task_id FROM tasks WHERE job_id = ?)`,
		wantJobID,
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count task_specs: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("task_specs rows = %d, want 1", fwdCount)
	}

	// retry_budget preservation. The current fixture sends *int(3) and
	// the handler preserves it.
	var gotRetry int
	if err := db.DB().QueryRow(
		`SELECT retry_budget FROM job_delivery_plans WHERE job_id = ? AND destination_id = ?`,
		wantJobID, "drive",
	).Scan(&gotRetry); err != nil {
		t.Fatalf("SELECT job_delivery_plans.retry_budget: %v", err)
	}
	if gotRetry != 3 {
		t.Fatalf("retry_budget = %d, want 3", gotRetry)
	}

	// ── Scenario 2 — identical replay ──
	w2 := postSubmitJob(t, r, body)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("replay POST: want 202, got %d body=%s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("response2 json: %v", err)
	}
	if resp2["job_id"] != wantJobID {
		t.Fatalf("replay job_id = %v, want %s", resp2["job_id"], wantJobID)
	}
	if resp2["accepted_from"] != "api_v1_jobs" {
		t.Fatalf("replay accepted_from = %v, want api_v1_jobs", resp2["accepted_from"])
	}
	if v, ok := resp2["created"]; !ok || v != false {
		t.Fatalf("replay created: want false, got %v (present=%v)", v, ok)
	}
	if resp2["dispatch_status"] != "queued_for_workers" {
		t.Fatalf("replay dispatch_status = %v, want queued_for_workers", resp2["dispatch_status"])
	}

	// Zero new rows.
	if got := countRowsByJobID(t, db, "tasks", "job_id", wantJobID); got != 1 {
		t.Fatalf("after replay tasks = %d, want 1", got)
	}
	if got := countRowsByJobID(t, db, "jobs", "job_id", wantJobID); got != 1 {
		t.Fatalf("after replay jobs = %d, want 1", got)
	}
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		ExternalAPISourceProvider, idem, JobSubmitTargetExecutorID,
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings after replay: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("after replay forwardings = %d, want 1", fwdCount)
	}
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ?`,
		wantJobID,
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count job_delivery_plans after replay: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("after replay job_delivery_plans = %d, want 1", fwdCount)
	}

	// ── Scenario 3 — same key, different payload → 409 ──
	drifted := validSubmitJobBody(idem)
	drifted.VideoName = "Drifted Title"
	w3 := postSubmitJob(t, r, drifted)
	if w3.Code != http.StatusConflict {
		t.Fatalf("hash-conflict POST: want 409, got %d body=%s", w3.Code, w3.Body.String())
	}
	var resp3 map[string]interface{}
	if err := json.Unmarshal(w3.Body.Bytes(), &resp3); err != nil {
		t.Fatalf("response3 json: %v", err)
	}
	if resp3["error"] != "idempotency_key_reused" {
		t.Fatalf("hash-conflict error = %v, want idempotency_key_reused (full body: %s)",
			resp3["error"], w3.Body.String())
	}
	if v, ok := resp3["ok"]; !ok || v != false {
		t.Fatalf("hash-conflict ok = %v, want false", v)
	}

	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		ExternalAPISourceProvider, idem, JobSubmitTargetExecutorID,
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings after 409: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("after 409 forwardings = %d, want 1", fwdCount)
	}
	if got := countRowsByJobID(t, db, "jobs", "job_id", wantJobID); got != 1 {
		t.Fatalf("after 409 jobs = %d, want 1", got)
	}
}

func TestSubmitJobE2E_ProducerOwnedFMP4PlanRoutesAndPersistsCanonically(t *testing.T) {
	t.Setenv("VELOX_FMP4_STREAM_PROFILE", "1")
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	const idem = "e2e-producer-owned-fmp4-001"
	rb := 3
	body := SubmitJobRequest{
		IdempotencyKey:           idem,
		VideoName:                "Producer-owned fMP4 test",
		ScriptText:               "The producer supplies the canonical fMP4 plan.",
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "drive", Priority: 1, RetryBudget: &rb,
		}},
	}

	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("producer-owned fMP4 POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode fMP4 response: %v", err)
	}
	jobID, ok := response["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("fMP4 response missing job_id: %s", w.Body.String())
	}

	var target string
	if err := db.DB().QueryRow(
		`SELECT target_executor_id FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ?`,
		ExternalAPISourceProvider, idem,
	).Scan(&target); err != nil {
		t.Fatalf("read fMP4 forwarding: %v", err)
	}
	if target != compiledPlanTargetExecutorID {
		t.Fatalf("forwarding target = %q, want %q", target, compiledPlanTargetExecutorID)
	}

	var taskSpecJSON string
	if err := db.DB().QueryRow(
		`SELECT s.payload_json FROM tasks t JOIN task_specs s ON s.task_id = t.task_id WHERE t.job_id = ?`,
		jobID,
	).Scan(&taskSpecJSON); err != nil {
		t.Fatalf("read fMP4 TaskSpec: %v", err)
	}
	var taskSpec map[string]interface{}
	if err := json.Unmarshal([]byte(taskSpecJSON), &taskSpec); err != nil {
		t.Fatalf("decode fMP4 TaskSpec: %v", err)
	}
	if got := taskSpec[contract.PayloadKeyCompiledRenderPlanJSON]; got != planJSON {
		t.Fatalf("TaskSpec compiled plan changed: got %#v, want producer bytes", got)
	}
	if got := taskSpec[contract.PayloadKeyCompiledRenderPlanSHA]; got != planSHA {
		t.Fatalf("TaskSpec compiled plan SHA = %#v, want %q", got, planSHA)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(taskSpec); err != nil {
		t.Fatalf("persisted fMP4 TaskSpec failed contract validation: %v", err)
	}
}

// TestSubmitJobE2E_CanonicalRecipePassthrough pins the complete canonical
// InstaEdit producer contract, POST /api/v1/jobs, through
// the persisted Job row and final TaskSpec. This intentionally exercises the
// canonical `/api/v1/jobs` producer contract, not the compatibility BFF
// `/api/v1/instaedit/jobs`, whose old project_id + render_spec adapter is
// retained until this passthrough is certified. These values must remain data
// supplied by the caller rather than being reconstructed from project_id/
// render_spec or replaced by recipe defaults.
func TestSubmitJobE2E_CanonicalRecipePassthrough(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	const idem = "canonical-audit-passthrough-001"
	body := validSubmitJobBody(idem)
	body.JobType = "scene.composite.v1"
	body.TemplateID = "audit.canonical"
	body.TemplateVersion = 17
	body.VideoName = "CANONICAL AUDIT"
	body.Output = &SubmitOutput{Width: 1280, Height: 720, FPS: 24, Format: "mp4"}

	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("canonical POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode canonical response: %v", err)
	}
	jobID, ok := response["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("canonical response missing job_id: %s", w.Body.String())
	}

	var videoName, requestJSON string
	if err := db.DB().QueryRow(
		`SELECT video_name, request_json FROM jobs WHERE job_id = ?`, jobID,
	).Scan(&videoName, &requestJSON); err != nil {
		t.Fatalf("read persisted job: %v", err)
	}
	if videoName != body.VideoName {
		t.Fatalf("jobs.video_name = %q, want %q", videoName, body.VideoName)
	}

	var persistedJob map[string]interface{}
	if err := json.Unmarshal([]byte(requestJSON), &persistedJob); err != nil {
		t.Fatalf("decode jobs.request_json: %v", err)
	}
	assertCanonicalRecipeFields(t, "jobs.request_json", persistedJob)

	var taskID, taskSpecJSON string
	if err := db.DB().QueryRow(
		`SELECT t.task_id, s.payload_json
		 FROM tasks t JOIN task_specs s ON s.task_id = t.task_id
		 WHERE t.job_id = ?`, jobID,
	).Scan(&taskID, &taskSpecJSON); err != nil {
		t.Fatalf("read persisted TaskSpec: %v", err)
	}
	if taskID == "" {
		t.Fatal("persisted TaskSpec has empty task_id")
	}
	var taskSpec map[string]interface{}
	if err := json.Unmarshal([]byte(taskSpecJSON), &taskSpec); err != nil {
		t.Fatalf("decode task_specs.payload_json: %v", err)
	}
	assertCanonicalRecipeFields(t, "task_specs.payload_json", taskSpec)
}

func assertCanonicalRecipeFields(t *testing.T, surface string, payload map[string]interface{}) {
	t.Helper()
	if got := payload["job_type"]; got != "scene.composite.v1" {
		t.Errorf("%s job_type = %v, want scene.composite.v1", surface, got)
	}
	if got := payload["template_id"]; got != "audit.canonical" {
		t.Errorf("%s template_id = %v, want audit.canonical", surface, got)
	}
	if got := payload["template_version"]; got != float64(17) && got != 17 {
		t.Errorf("%s template_version = %v, want 17", surface, got)
	}
	if got := payload["video_name"]; got != "CANONICAL AUDIT" {
		t.Errorf("%s video_name = %v, want CANONICAL AUDIT", surface, got)
	}

	if _, present := payload["spec"]; present {
		t.Errorf("%s spec = %#v, want the spec field fully removed", surface, payload["spec"])
	}
	if _, present := payload["output"]; present {
		t.Errorf("%s output = %#v, want retired top-level output dropped at the boundary", surface, payload["output"])
	}
}

// ── Scenarios 6, 7, 8, 9 — rejection suite ─────────────────────

// TestSubmitJobE2E_ValidationFailures is table-driven.
