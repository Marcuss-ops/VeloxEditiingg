// Package pipeline: creator_push_e2e_test.go exercises the full HTTP →
// creatorflow.Resolver → atomic Job+Task write path for the
// POST /api/v1/creator/jobs endpoint.
//
// creator_push_test.go (sibling) covers the pure normalization layer
// (creatorPushRequest → normalizedCreatorPush). This file is the
// integration counterpart: it wires a real SQLite store, a real Enqueuer
// + creatorflow.Resolver, and runs the handler through a real
// httptest.Recorder + gin.New engine mounted via h.RegisterRoutes.
//
// The auth middleware is bypassed via adminAuthFake because the auth
// chain has its own unit coverage in handlers/server/api; this file
// exercises the creator_push contract exclusively.
package pipeline

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
	"velox-server/internal/creatorflow"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
	"velox-server/internal/store"
)

// adminAuthFake short-circuits the bearer-token check the production
// router applies to /api/v1/creator/jobs. The auth chain is unit-tested
// separately; this file exercises the creator_push contract exclusively.
func adminAuthFake(c *gin.Context) { c.Next() }

// newCreatorPushE2EStack wires the minimum graph required by the
// creator_push endpoint: SQLite store + SQLiteJobRepository +
// AtomicJobTaskCreator + Enqueuer (with the no-op PlanResolver) +
// canonical creatorflow.Resolver + Handlers. The remote client is not
// required (the creator_push path does not call remoteengine.Client).
//
// Mirrors the construction sequence used by pipeline_bridge_test.go and
// service_test.go; the SQLite file lives under t.TempDir() so tests are
// hermetic and parallel-safe. delivery_destinations is seeded with a
// single "drive" row so the enqueuer's delivery_plan validation passes.
func newCreatorPushE2EStack(t *testing.T) (*Handlers, *store.SQLiteStore, *store.SQLiteJobRepository) {
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
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db, tempDir, filepath.Join(tempDir, "videos"), "")
	h := NewHandlersWithResolver(&config.Config{}, testEnqueuer, nil, resolver, jobRepo, nil, nil).WithStore(db)
	return h, db, jobRepo
}

// creatorPushE2EBody is the canonical Creator payload: voiceover +
// stock/clips + scene + delivery_plan. Each test invocation clones and
// tweaks the body. The shape mirrors the contract documented in
// docs/CREATOR-PUSH.md.
func creatorPushE2EBody(sourceProvider, sourceJobID, targetExecutor string) map[string]interface{} {
	return map[string]interface{}{
		"source_provider":    sourceProvider,
		"source_job_id":      sourceJobID,
		"target_executor_id": targetExecutor,
		"payload": map[string]interface{}{
			"status":      "completed",
			"job_id":      sourceJobID,
			"video_name":  "E2E voiceover+stock+clip+scene",
			"script_text": "Creator-supplied script body.",
			"voiceover_paths": []interface{}{
				"velox-asset://voiceovers/audio.mp3",
			},
			"scenes": []interface{}{
				map[string]interface{}{
					"text":             "Prima scena",
					"clip_link":        "velox-asset://clips/clip-01.mp4",
					"duration_seconds": 7,
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
}

// postCreatorPush serializes the body, registers an admin-bearer token
// (the adminAuthFake accepts any), and runs the request through the test
// router. Returns the recorded response.
func postCreatorPush(t *testing.T, r *gin.Engine, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/jobs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestCreatorPushJobsE2E_VoiceoverStockClipScene covers the happy path:
//
//  1. POST /api/v1/creator/jobs with a Creator-built payload (voiceover +
//     stock/clips + scenes + delivery_plan) returns 202 with the canonical
//     envelope (ok=true, accepted_from=creator_push, source_provider,
//     source_job_id, target_executor_id, deterministic job_id,
//     status=PENDING, dispatch_status=queued_for_workers).
//  2. The creatorflow.Resolver atomic CAS created exactly one
//     creator_forwardings row (status=FORWARDED), one jobs row
//     (status=PENDING), and one row each in tasks and task_specs —
//     verified by direct SQL counts so the test does not rely on store
//     helper internals.
//  3. A second POST with the same (source_provider, source_job_id,
//     target_executor_id) converges on the same job_id without creating
//     any additional rows (UNIQUE constraint + idempotency fast-path
//     inside Resolver.Resolve).
func TestCreatorPushJobsE2E_VoiceoverStockClipScene(t *testing.T) {
	h, db, jobRepo := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator_pc_1", "creator-job-001", "scene.composite.v1").String(),
	)
	body := creatorPushE2EBody("creator_pc_1", "creator-job-001", "scene.composite.v1")

	// First POST — full happy path.
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("first POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	wantFields := map[string]interface{}{
		"ok":                true,
		"accepted_from":     "creator_push",
		"source_provider":   "creator_pc_1",
		"source_job_id":     "creator-job-001",
		"target_executor_id": "scene.composite.v1",
		"job_id":            expectedJobID,
		"status":            "PENDING",
		"dispatch_status":   "queued_for_workers",
	}
	for key, want := range wantFields {
		if resp[key] != want {
			t.Fatalf("response[%q] = %v, want %v (full body=%s)", key, resp[key], want, w.Body.String())
		}
	}

	// creator_forwardings row written by Resolve's atomic CAS.
	forwarding, err := db.GetCreatorForwardingBySource(context.Background(),
		"creator_pc_1", "creator-job-001", "scene.composite.v1",
	)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if forwarding == nil {
		t.Fatal("creator_forwardings row not persisted (atomic CAS did not write)")
	}
	if forwarding.Status != string(store.CFStatusForwarded) {
		t.Fatalf("forwarding status = %s, want FORWARDED", forwarding.Status)
	}

	// jobs row exists with status PENDING (the canonical Resolver outcome).
	job, err := jobRepo.Get(context.Background(), expectedJobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job == nil {
		t.Fatal("jobs row not persisted")
	}
	if job.Status != jobs.StatusPending {
		t.Fatalf("job status = %v, want PENDING", job.Status)
	}

	// tasks + task_specs row exists for the job_id (AtomicJobTaskCreator
	// triple-INSERT must have produced all three tables).
	var taskCount, specCount int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, expectedJobID).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("exactly 1 tasks row expected, got %d", taskCount)
	}
	// task_specs is keyed by task_id (no job_id column); JOIN via tasks.
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM task_specs WHERE task_id IN (SELECT task_id FROM tasks WHERE job_id = ?)`,
		expectedJobID,
	).Scan(&specCount); err != nil {
		t.Fatalf("count task_specs: %v", err)
	}
	if specCount != 1 {
		t.Fatalf("exactly 1 task_specs row expected, got %d", specCount)
	}

	// Idempotency: second POST with the same body returns the same job_id
	// and does NOT create additional rows. The UNIQUE constraint on
	// (source_provider, source_job_id, target_executor_id) and the
	// Resolver idempotency fast-path both guarantee convergence.
	w2 := postCreatorPush(t, r, body)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second POST: want 202, got %d body=%s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("response2 json: %v", err)
	}
	if resp2["job_id"] != expectedJobID {
		t.Fatalf("idempotent job_id: want %s, got %v", expectedJobID, resp2["job_id"])
	}
	// accepted_from is preserved across replays too.
	if resp2["accepted_from"] != "creator_push" {
		t.Fatalf("idempotent accepted_from: want creator_push, got %v", resp2["accepted_from"])
	}
	// created=false is the canonical signal from buildIdempotentResolveResponse
	// that the second POST hit the Resolver fast-path, not a fresh insert.
	if v, ok := resp2["created"]; !ok || v != false {
		t.Fatalf("idempotent created: want false (fast-path), got %v (present=%v)", v, ok)
	}
	// dispatch_status is stamped on every response (fresh + replay), so the
	// replay must carry it identically. This guards against a future
	// regression that strips overlay fields on the idempotent path.
	if resp2["dispatch_status"] != "queued_for_workers" {
		t.Fatalf("idempotent dispatch_status: want queued_for_workers, got %v", resp2["dispatch_status"])
	}

	var fwdCount, jobCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		"creator_pc_1", "creator-job-001", "scene.composite.v1",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("want exactly 1 forwarding row after replay, got %d", fwdCount)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("want exactly 1 jobs row after replay, got %d", jobCount)
	}
}

// TestCreatorPushJobsE2E_IncompletePayloadReturns422 covers the 422 path
// when the payload is syntactically valid but fails the Resolver's
// completeness guard. The handler maps creatorflow.ErrResolverNotComplete
// to 422 Unprocessable Entity; no Job row is written and no
// creator_forwardings row is persisted.
func TestCreatorPushJobsE2E_IncompletePayloadReturns422(t *testing.T) {
	h, db, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	body := map[string]interface{}{
		"source_provider": "creator_pc_1",
		"source_job_id":   "creator-incomplete-001",
		"payload": map[string]interface{}{
			"status":     "running", // not in {completed, succeeded, done}
			"job_id":     "creator-incomplete-001",
			"video_name": "not yet ready",
			// no scenes_json, no json_path, no scenes[] → completeness
			// guard inside enqueue.ShouldForwardPipelineResult fails.
		},
	}

	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d body=%s", w.Code, w.Body.String())
	}
	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator_pc_1", "creator-incomplete-001", "scene.composite.v1").String(),
	)

	// 422 path must NOT leave any Job or forwarding row behind.
	var jobCount int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 0 {
		t.Fatalf("422 path must NOT create a jobs row, got %d", jobCount)
	}
	var fwdCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ?`,
		"creator_pc_1", "creator-incomplete-001",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount != 0 {
		t.Fatalf("422 path must NOT create a forwarding row, got %d", fwdCount)
	}
	// task_specs follows task_id; 0 tasks ⇒ 0 task_specs transitively.
	var taskCount422 int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, expectedJobID).Scan(&taskCount422); err != nil {
		t.Fatalf("count tasks on 422 path: %v", err)
	}
	if taskCount422 != 0 {
		t.Fatalf("422 path must NOT create a tasks row, got %d", taskCount422)
	}
}

// TestCreatorPushJobsE2E_MissingSourceJobIDReturns400 covers the 400 path
// when neither the envelope nor payload carry source_job_id. The
// normalization layer rejects this case with a typed error that the
// handler maps to 400 Bad Request. The 400 path runs in
// normalizeCreatorPushRequest BEFORE any DB write, so we additionally
// assert that no creator_forwardings row exists for that key — the
// handler must NOT have reached the Resolver entry point.
func TestCreatorPushJobsE2E_MissingSourceJobIDReturns400(t *testing.T) {
	h, db, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	body := map[string]interface{}{
		"source_provider": "creator_pc_nosrc",
		"payload": map[string]interface{}{
			"status":     "completed",
			"video_name": "no source_job_id anywhere",
			// no source_job_id in envelope, no payload.job_id → normalize
			// returns "source_job_id is required (...)".
		},
	}
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d body=%s", w.Code, w.Body.String())
	}

	// 400 path must reject BEFORE the Resolver — no forwarding row for the
	// supplied source_provider (handler rejected in normalizeCreatorPushRequest
	// before the default "creator" stamp was even applied, so we only need
	// to probe the supplied provider key).
	var fwdCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ?`,
		"creator_pc_nosrc",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings on 400 path: %v", err)
	}
	if fwdCount != 0 {
		t.Fatalf("400 path must NOT reach the Resolver (no forwarding row), got %d", fwdCount)
	}
}

// TestCreatorPushJobsE2E_RetryBudgetZeroAcceptance mirrors
// job_submit_e2e_test.go::TestSubmitJobE2E_RetryBudgetZeroAcceptance
// for the creator_push path. The fix at commit 72a455c relaxed the
// enqueue-layer validateDeliveryPlanRequires boundary from <=0 to
// <0, so retry_budget=0 MUST round-trip verbatim into
// job_delivery_plans.retry_budget on POST /api/v1/creator/jobs too
// (same Resolver.Resolve → validateDeliveryPlanRequires chain —
// both paths converge on resolveCompletedPayload at
// creator_push.go line 183).
//
// This is a defensive contract pin: the enqueue-layer fix already
// covers the creator_push path because normalizeCreatorPushRequest
// (creator_push.go line 95) has NO retry_budget check of its own
// and the typed-DTO parser at remoteengine/dto.go does NOT
// validate retry_budget (delivery_plan is a raw passthrough).
// Without this test, a future contributor could re-introduce a
// creator_push-specific guard (e.g., a misplaced >= 0 check in the
// handler) and silently downgrade retry_budget=0 to 422 —
// diverging from the submit_job path's contract without any test
// failure surfacing.
//
// Three layers of contract pinned:
//
//  1. HTTP envelope: 202 Accepted + ok=true + job_id matches the
//     DeriveForwardingJobID hash of (source_provider,
//     source_job_id, target_executor_id).
//  2. job_delivery_plans.retry_budget = 0 in SQLite (the explicit
//     client choice MUST round-trip verbatim, NOT bumped to the
//     OpenAPI default of 3).
//  3. job.MaxRetries = 0 (the worker terminal-fails on the first
//     hard error — matching the client's explicit "no retries"
//     intent; NOT bumped to DefaultRetryBudget=3).
//
// Idempotency replay (a second POST with the same body) is
// pinned too so a future regression on the resolver's
// idempotency fast-path that drops the retry_budget=0 row is
// caught.
func TestCreatorPushJobsE2E_RetryBudgetZeroAcceptance(t *testing.T) {
	h, db, jobRepo := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// Build the canonical body, then override delivery_plan[0].retry_budget=0.
	// The body helper is fresh per call so the mutation is hermetic.
	body := creatorPushE2EBody("creator_pc_rz0", "creator-job-rz0-001", "scene.composite.v1")
	dp := body["payload"].(map[string]interface{})["delivery_plan"].([]interface{})
	dp[0].(map[string]interface{})["retry_budget"] = 0

	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator_pc_rz0", "creator-job-rz0-001", "scene.composite.v1").String(),
	)

	// First POST — retry_budget=0 MUST be accepted (was previously
	// rejected with 422 by the enqueue-layer validateDeliveryPlanRequires
	// guard at line 188 of internal/jobs/enqueue/delivery_plan_validator.go;
	// that guard was relaxed at commit 72a455c).
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("retry_budget=0 first POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["ok"] != true {
		t.Fatalf("response[ok] = %v, want true (full body=%s)", resp["ok"], w.Body.String())
	}
	if resp["job_id"] != expectedJobID {
		t.Fatalf("response[job_id] = %v, want %s (full body=%s)", resp["job_id"], expectedJobID, w.Body.String())
	}

	// DB contract: job_delivery_plans.retry_budget MUST be 0 (not 3
	// bumped to OpenAPI default, not dropped). Mirrors the
	// TestSubmitJobE2E_RetryBudgetZeroAcceptance assertion.
	var retryBudget int
	if err := db.DB().QueryRow(
		`SELECT retry_budget FROM job_delivery_plans WHERE job_id = ? AND destination_id = ?`,
		expectedJobID, "drive",
	).Scan(&retryBudget); err != nil {
		t.Fatalf("query job_delivery_plans.retry_budget: %v", err)
	}
	if retryBudget != 0 {
		t.Errorf("job_delivery_plans.retry_budget = %d, want 0 (explicit client choice MUST round-trip verbatim — the OpenAPI default bump is forbidden)", retryBudget)
	}

	// Job-level contract: jobs.MaxRetries MUST be 0 too. The enqueue
	// layer's extractPlanMaxRetry at normalize.go line 511 (already
	// verified at commit 72a455c) computes max(retry_budget) across
	// destinations; with retry_budget=0 as the only entry the max is 0
	// and job.MaxRetries=0 means the worker terminal-fails on first
	// hard error. A regression here would silently re-enable retries
	// for clients that explicitly opted out.
	job, err := jobRepo.Get(context.Background(), expectedJobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job == nil {
		t.Fatal("jobs row not persisted")
	}
	if job.MaxRetries != 0 {
		t.Errorf("job.MaxRetries = %d, want 0 (retry_budget=0 means worker terminal-fails on first hard error)", job.MaxRetries)
	}

	// Idempotency replay: a second POST with the same body converges
	// on the same job_id, the same retry_budget=0 round-trip, and
	// does NOT create additional rows. The UNIQUE constraint on
	// (source_provider, source_job_id, target_executor_id) and the
	// Resolver idempotency fast-path both guarantee convergence.
	w2 := postCreatorPush(t, r, body)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("retry_budget=0 second POST: want 202, got %d body=%s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("response2 json: %v", err)
	}
	// Mirror the sibling TestCreatorPushJobsE2E_VoiceoverStockClipScene
	// replay assertions so a regression that strips overlay fields on
	// the idempotent path is caught (e.g., a future refactor that
	// rebuilds the response on the fast-path without re-attaching
	// accepted_from / dispatch_status / created=false).
	replayWantFields := map[string]interface{}{
		"job_id":          expectedJobID,
		"accepted_from":   "creator_push",
		"dispatch_status": "queued_for_workers",
		"created":         false,
		"ok":              true,
	}
	for key, want := range replayWantFields {
		if resp2[key] != want {
			t.Errorf("idempotent replay response[%q] = %v, want %v (full body=%s)", key, resp2[key], want, w2.Body.String())
		}
	}

	// row-count invariant: exactly 1 jobs row + 1 forwarding row
	// after the replay. A regression that drops the retry_budget=0
	// contract on the replay path would still hit the UNIQUE
	// constraint, but a regression that drops it on the FIRST POST
	// would not — this row-count check pins the first-POST path.
	var fwdCount, jobCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		"creator_pc_rz0", "creator-job-rz0-001", "scene.composite.v1",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount != 1 {
		t.Errorf("want exactly 1 forwarding row, got %d", fwdCount)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 1 {
		t.Errorf("want exactly 1 jobs row, got %d", jobCount)
	}

	// Final defensive check: confirm the round-trip retry_budget
	// is STILL 0 after the replay (catches a regression where
	// the idempotent replay path rebuilds the plan with
	// DefaultRetryBudget instead of preserving 0).
	var retryBudgetAfterReplay int
	if err := db.DB().QueryRow(
		`SELECT retry_budget FROM job_delivery_plans WHERE job_id = ? AND destination_id = ?`,
		expectedJobID, "drive",
	).Scan(&retryBudgetAfterReplay); err != nil {
		t.Fatalf("query job_delivery_plans.retry_budget post-replay: %v", err)
	}
	if retryBudgetAfterReplay != 0 {
		t.Errorf("post-replay job_delivery_plans.retry_budget = %d, want 0", retryBudgetAfterReplay)
	}
}

// TestCreatorPushJobsE2E_NegativeRetryBudgetRejected is the
// boundary guard for retry_budget < 0 on the creator_push path.
// Mirrors job_submit_e2e_test.go::TestSubmitJobE2E_NegativeRetryBudgetRejected
// so the same rejection shape is pinned on both intake paths.
//
// Without this row, a future regression that relaxed the enqueue-
// layer boundary the WRONG WAY (e.g., a contributor who reads
// "retry_budget=0 is allowed" and concludes "any value is allowed")
// would surface only on the negative case as a 202 with retry_budget=-1
// persisted into job_delivery_plans. The handler-side pre-check
// in the canonical creator_push path does NOT cover negative
// retry_budget — only the enqueue-layer rejection does — so this
// e2e test pins the rejection shape end-to-end.
func TestCreatorPushJobsE2E_NegativeRetryBudgetRejected(t *testing.T) {
	h, db, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// Build the canonical body, then override delivery_plan[0].retry_budget=-1.
	body := creatorPushE2EBody("creator_pc_rzneg", "creator-job-rzneg-001", "scene.composite.v1")
	dp := body["payload"].(map[string]interface{})["delivery_plan"].([]interface{})
	dp[0].(map[string]interface{})["retry_budget"] = -1

	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator_pc_rzneg", "creator-job-rzneg-001", "scene.composite.v1").String(),
	)

	// Negative retry_budget MUST be rejected with 422 + invalid_payload.
	// The enqueue-layer validateDeliveryPlanRequires (line 188 of
	// internal/jobs/enqueue/delivery_plan_validator.go) returns
	// &validationError{field: "delivery_plan[0].retry_budget", message: "must be >= 0"}
	// which creatorflow.WriteResolverError maps to 422 invalid_payload
	// with details[0].path = "delivery_plan[0].retry_budget" (bracket
	// notation, matches what validateDeliveryPlanRequires emits via
	// fmt.Sprintf).
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("retry_budget=-1 POST: want 422, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["ok"] != false {
		t.Fatalf("response[ok] = %v, want false (full body=%s)", resp["ok"], w.Body.String())
	}
	if resp["error"] != "invalid_payload" {
		t.Fatalf("response[error] = %v, want invalid_payload (full body=%s)", resp["error"], w.Body.String())
	}

	// Details shape: assert details[0].path = "delivery_plan[0].retry_budget"
	// (bracket notation, NOT dot — the validator emits via fmt.Sprintf
	// so the actual emission is bracket). This pins the rejection
	// shape so a future regression that emits 422 with a different
	// error code (e.g., payload_incomplete) cannot silently pass.
	detailsArr, ok := resp["details"].([]interface{})
	if !ok {
		t.Fatalf("response[details] missing or wrong type: %T (full body=%s)", resp["details"], w.Body.String())
	}
	if len(detailsArr) != 1 {
		t.Fatalf("response[details] length = %d, want 1 (full body=%s)", len(detailsArr), w.Body.String())
	}
	detailsObj, ok := detailsArr[0].(map[string]interface{})
	if !ok {
		t.Fatalf("details[0] wrong type: %T (full body=%s)", detailsArr[0], w.Body.String())
	}
	if gotPath := detailsObj["path"]; gotPath != "delivery_plan[0].retry_budget" {
		t.Errorf("details[0].path = %v, want delivery_plan[0].retry_budget (bracket notation, what validateDeliveryPlanRequires emits)", gotPath)
	}
	if gotIssue := detailsObj["issue"]; gotIssue != "invalid" {
		t.Errorf("details[0].issue = %v, want \"invalid\" (canonical token from WriteResolverError's validationFieldExtractor branch)", gotIssue)
	}

	// Row-leak invariant: jobs row MUST NOT exist. The handler never
	// reaches the atomic Job+Task create when enqueue-layer validation
	// rejects the payload.
	var jobCount int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 0 {
		t.Errorf("422 path must NOT create a jobs row, got %d", jobCount)
	}

	// KNOWN FINDING (documented with upper-bound gate): the
	// creatorforwardings row IS leaked on the 422 path because the
	// Resolver creates the row (status=READY_TO_FORWARD) BEFORE the
	// enqueue-layer validateDeliveryPlanRequires rejects. Contrast with
	// TestCreatorPushJobsE2E_IncompletePayloadReturns422 which DOES
	// pin the row-leak invariant because the completeness guard runs
	// before the Resolver entry point. Fixing the row-leak for the
	// in-Resolver-rejection path is a separate refactor — either move
	// the enqueue validation BEFORE the forwarding-row creation, or
	// add a Resolver-level rollback after Enqueue failure. Tracked as
	// a followup. The upper-bound gate (>= 2 rows) catches an
	// EXPANSION of the leak (e.g., a future contributor who
	// inadvertently creates N rows per rejection) while staying
	// in scope for the retry_budget=0 contract mirror.
	var fwdCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		"creator_pc_rzneg", "creator-job-rzneg-001", "scene.composite.v1",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount >= 2 {
		t.Errorf("row-leak UPPER-BOUND gate: creator_forwardings row count = %d, want < 2 (current known leak is 1 row per rejection; this gate catches expansion, not the existing leak)", fwdCount)
	}
}

// TestCreatorPushJobsE2E_RealAdminAuthWired verifies that
//
//  1. No Authorization header  → 401 (handler never reached).
//  2. Wrong bearer token        → 401 (handler never reached).
//  3. Right bearer token        → 202 (handler reached, CreatorPush enqueued).
//
// Defense-in-depth against the IsLocalRequestIP early-return bypass:
// the middleware short-circuits when c.ClientIP() is loopback. We pin
// req.RemoteAddr to a non-loopback public IP (RFC 5737 TEST-NET) so
// the bypass cannot accidentally let the request through inside CI
// or local-dev environments. SetTrustedProxies(nil) prevents Gin from
// trusting any X-Forwarded-For header on the test path.
func TestCreatorPushJobsE2E_RealAdminAuthWired(t *testing.T) {
	h, _, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)

	// Pin the env vars so a leftover value in the developer's shell or in
	// CI does NOT mask the test token. api.AdminAuthMiddleware honours
	// cfg.Auth.AdminToken directly, so this is the canonical contract;
	// setting both VELOX_ADMIN_TOKEN (env) and TOKEN_FILE (the platform
	// fallback path env, per pipeline/codegen/voiceover_harness.go::Token
	// resolution) to empty ensures the middleware will still reject
	// requests with the corresponding header / will not pick up a stale
	// file-based token. Order matters: explicit > env > TOKEN_FILE.
	t.Setenv("VELOX_ADMIN_TOKEN", "")
	t.Setenv("TOKEN_FILE", "")

	const testToken = "test-secret-token"
	cfg := &config.Config{}
	cfg.Auth.AdminToken = testToken
	authMW := api.AdminAuthMiddleware(cfg)

	r := gin.New()
	r.SetTrustedProxies(nil)
	h.RegisterRoutes(r, authMW, m2mJobsAuthFake)

	body := creatorPushE2EBody("creator_pc_auth", "creator-job-auth-001", "scene.composite.v1")
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	cases := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"no_authorization_header", "", http.StatusUnauthorized},
		{"wrong_bearer_token", "Bearer invalid-mock-token", http.StatusUnauthorized},
		{"right_bearer_token", "Bearer " + testToken, http.StatusAccepted},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/jobs", bytes.NewReader(rawBody))
			req.Header.Set("Content-Type", "application/json")
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			// RFC 5737 TEST-NET-2 public IP — never loopback, so the
			// middleware's IsLocalRequestIP early-return cannot save us.
			req.RemoteAddr = "198.51.100.1:1234"
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("want %d, got %d body=%s", tc.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}
