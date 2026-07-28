// Package pipeline — e2e retry-budget scenario tests for POST /api/v1/jobs.
//
// This file owns:
//   - scenario 5 of the submit-job e2e coverage matrix
//     (TestSubmitJobE2E_RetryBudgetZeroAcceptance): the explicit-zero
//     round-trip contract — a request supplying retry_budget=0 on a
//     delivery_plan entry MUST round-trip into
//     job_delivery_plans.retry_budget=0 (NOT silently default to 3),
//     MUST converge idempotently on replay, and MUST NOT double-write
//     rows. Counterpart: job_submit_e2e_validation_test.go covers the
//     NEGATIVE retry budget boundary.
package pipeline

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
)
func TestSubmitJobE2E_RetryBudgetZeroAcceptance(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	const idem = "e2e-retry-budget-zero-001"
	rb := 0
	body := validSubmitJobBody(idem)
	body.DeliveryPlan[0].RetryBudget = &rb

	wantJobID := expectedSubmitJobID(idem)

	// ── Scenario 5 — explicit-zero POST → 202 + canonical envelope ──
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	if resp["job_id"] != wantJobID {
		t.Fatalf("response.job_id = %v, want %s (explicit-zero acceptance)", resp["job_id"], wantJobID)
	}
	if resp["accepted_from"] != "api_v1_jobs" {
		t.Fatalf("response.accepted_from = %v, want api_v1_jobs", resp["accepted_from"])
	}
	if resp["ok"] != true {
		t.Fatalf("response.ok = %v, want true", resp["ok"])
	}

	// Round-trip invariant: job_delivery_plans.retry_budget MUST
	// equal 0 verbatim. The pre-P0-relaxation bug was that the
	// validator rejected 0 at the enqueue layer, blocking this row
	// entirely. After the relaxation the column round-trips as 0
	// (not the DefaultRetryBudget=3 fallback the test fixture uses
	// in validSubmitJobBody otherwise).
	var gotRetry int
	if err := db.DB().QueryRow(
		`SELECT retry_budget FROM job_delivery_plans WHERE job_id = ? AND destination_id = ?`,
		wantJobID, "drive",
	).Scan(&gotRetry); err != nil {
		t.Fatalf("SELECT job_delivery_plans.retry_budget: %v", err)
	}
	if gotRetry != 0 {
		t.Fatalf("job_delivery_plans.retry_budget = %d, want 0 (explicit-zero contract; NOT DefaultRetryBudget=3 fallback)", gotRetry)
	}

	if got := countRowsByJobID(t, db, "jobs", "job_id", wantJobID); got != 1 {
		t.Fatalf("jobs rows = %d, want 1 (resource-leak invariant)", got)
	}

	// ── Scenario 5b — replay (idempotent convergence) ─────────────────
	// A second POST with the SAME retry_budget=0 + idempotency_key
	// must hit the Resolver fast-path, return 202, and create NO
	// new job_delivery_plans row. created=false in the response is
	// the canonical signal the fast-path was taken.
	w2 := postSubmitJob(t, r, body)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("replay: want 202, got %d body=%s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]interface{}
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	if resp2["job_id"] != wantJobID {
		t.Errorf("replay job_id = %v, want %s", resp2["job_id"], wantJobID)
	}
	if v, ok := resp2["created"]; !ok || v != false {
		t.Errorf("replay created = %v, want false (canonical fast-path signal)", v)
	}

	var ddpCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ?`,
		wantJobID,
	).Scan(&ddpCount); err != nil {
		t.Fatalf("count job_delivery_plans after replay: %v", err)
	}
	if ddpCount != 1 {
		t.Errorf("job_delivery_plans rows after replay = %d, want 1 (idempotent convergence)", ddpCount)
	}
}

// TestSubmitJobE2E_NegativeRetryBudgetRejected pins the relaxed
// rejection boundary (<0) at the HTTP layer: a request that supplies
// retry_budget: -1 MUST be rejected with 422 invalid_payload and
// details[].path pointing at delivery_plan.0.retry_budget. Mirrors the
// down-stack regression guard `TestEnqueue_Precondition_RejectsNegativeRetryBudget`
// at the enqueue layer so a future "intFromAny coercion" (which would
// silently turn -1 into 0) is caught at the very edge instead of
// surfacing as a flaky 202-ok on a negative input.
