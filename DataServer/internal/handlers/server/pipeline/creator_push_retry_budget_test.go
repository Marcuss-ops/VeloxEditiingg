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
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"testing"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
)

// adminAuthFake short-circuits the bearer-token check the production
// router applies to /api/v1/creator/jobs. The auth chain is unit-tested
// separately; this file exercises the creator_push contract exclusively.
func TestCreatorPushJobsE2E_RetryBudgetZeroAcceptance(t *testing.T) {
	h, db, jobRepo := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// Build the canonical body, then override delivery_plan.0.retry_budget=0.
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
