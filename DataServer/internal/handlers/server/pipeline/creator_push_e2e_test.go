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
	h.RegisterRoutes(r, adminAuthFake)

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
	if taskCount < 1 {
		t.Fatalf("at least 1 tasks row expected, got %d", taskCount)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM task_specs WHERE job_id = ?`, expectedJobID).Scan(&specCount); err != nil {
		t.Fatalf("count task_specs: %v", err)
	}
	if specCount < 1 {
		t.Fatalf("at least 1 task_specs row expected, got %d", specCount)
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
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE id = ?`, expectedJobID).Scan(&jobCount); err != nil {
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
	h.RegisterRoutes(r, adminAuthFake)

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
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE id = ?`, expectedJobID).Scan(&jobCount); err != nil {
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
}

// TestCreatorPushJobsE2E_MissingSourceJobIDReturns400 covers the 400 path
// when neither the envelope nor payload carry source_job_id. The
// normalization layer rejects this case with a typed error that the
// handler maps to 400 Bad Request.
func TestCreatorPushJobsE2E_MissingSourceJobIDReturns400(t *testing.T) {
	h, _, _ := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake)

	body := map[string]interface{}{
		"source_provider": "creator_pc_1",
		"payload": map[string]interface{}{
			"status":     "completed",
			"video_name": "no source_job_id anywhere",
			// no source_job_id in envelope, no payload.job_id → normalize
			// returns "source_job_id is required (...)".
		},
	}
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}
