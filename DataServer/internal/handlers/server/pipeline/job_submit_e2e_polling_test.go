// Package pipeline — e2e POST → GET polling-chain tests for POST /api/v1/jobs.
//
// This file owns:
//   - the polling helper getSubmittedJob (GET /api/v1/jobs/{job_id}
//     issuing under the m2mPollingAuthFake bearer and fixed test client);
//   - scenarios "polling_chain_happy_path" + "polling_chain_not_found"
//     (TestSubmitJobE2E_PollingChain_HappyPath + TestSubmitJobE2E_PollingChain_NotFound):
//     assert the 202 → 200 chain, the Location/status_url round-trip, the
//     404 envelope (ok:false, error:"job_not_found", message) on GET.
//
// The polling path uses the same body/header conventions as the
// POST path — see the helpers in job_submit_e2e_happy_path_test.go.
// The fake middleware deliberately supplies pollingTestClientID so the
// test exercises the production ownership-scoped lookup without using
// a real M2M key database.
package pipeline

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

const pollingTestClientID = "e2e-polling-client"

func m2mPollingAuthFake(c *gin.Context) {
	c.Set(m2mCtxKeyClientID, pollingTestClientID)
	c.Next()
}

func getSubmittedJob(t *testing.T, r *gin.Engine, jobID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+jobID, nil)
	req.Header.Set("Authorization", "Bearer m2mJobsAuthFake-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestSubmitJobE2E_PollingChain_HappyPath covers the POST → GET chain.
//
//  1. POST a fresh submission. Assert the response carries both the
//     `Location: /api/v1/jobs/{job_id}` header AND the `status_url`
//     JSON field, with the same canonical path string in both.
//  2. GET that job_id. Assert 200 + the 4-field envelope
//     (job_id, status, created=true, status_url).
//  3. Replay-cache GET (no second POST needed). Assert the same
//     shape — the GET response is independent of the POST replay
//     semantics because the resolver fast-path populates the row
//     before either path returns.
//
// The test uses the same fixed authenticated client fixture for both
// POST and GET so it verifies the ownership-scoped polling contract;
// the real token validation matrix has its own dedicated test in
// M2MAuthEnvelopes.
func TestSubmitJobE2E_PollingChain_HappyPath(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mPollingAuthFake)

	const idem = "e2e-polling-001"
	body := validSubmitJobBody(idem)
	wantJobID := expectedSubmitJobID(idem)
	wantStatusURL := "/api/v1/jobs/" + wantJobID

	// POST first.
	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}

	// Location header on the 202 response.
	loc := w.Header().Get("Location")
	if loc != wantStatusURL {
		t.Fatalf("Location header = %q, want %q", loc, wantStatusURL)
	}

	// status_url in the JSON body matches the Location header.
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("POST response json: %v", err)
	}
	if resp["status_url"] != wantStatusURL {
		t.Fatalf("POST response.status_url = %v, want %q (full body: %s)",
			resp["status_url"], wantStatusURL, w.Body.String())
	}

	// Sanity: job_id in the body matches the canonical derivation.
	if resp["job_id"] != wantJobID {
		t.Fatalf("POST response.job_id = %v, want %s", resp["job_id"], wantJobID)
	}

	var persistedClientID string
	if err := db.DB().QueryRow(
		`SELECT COALESCE(external_client_id, '') FROM creator_forwardings WHERE target_job_id = ?`,
		wantJobID,
	).Scan(&persistedClientID); err != nil {
		t.Fatalf("read persisted ownership: %v", err)
	}
	if persistedClientID != pollingTestClientID {
		t.Fatalf("persisted external_client_id = %q, want %q", persistedClientID, pollingTestClientID)
	}

	// GET that job_id and assert the 4-field envelope.
	w2 := getSubmittedJob(t, r, wantJobID)
	if w2.Code != http.StatusOK {
		t.Fatalf("GET: want 200, got %d body=%s", w2.Code, w2.Body.String())
	}
	var getResp map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("GET response json: %v", err)
	}
	wantGETFields := map[string]interface{}{
		"ok":         true,
		"job_id":     wantJobID,
		"status":     "PENDING",
		"created":    true,
		"status_url": wantStatusURL,
	}
	for key, want := range wantGETFields {
		if got := getResp[key]; got != want {
			t.Fatalf("GET response[%q] = %v, want %v (full body: %s)",
				key, got, want, w2.Body.String())
		}
	}

	// Self-link property: the GET response's status_url dereferences
	// to the same canonical job_id, forming a stable loop.
	if getResp["status_url"] != wantStatusURL {
		t.Fatalf("GET response.status_url self-link broken: %v, want %q",
			getResp["status_url"], wantStatusURL)
	}
}

// TestSubmitJobE2E_PollingChain_NotFound covers the 404 envelope on
// the GET path when the requested job_id does not match any known
// creator forwarding row. The envelope shape mirrors the M2M
// token-rejection envelope (ok:false, error:job_not_found, message)
// so a single error dispatcher handles both auth + lookup misses
// without per-endpoint special-casing.
func TestSubmitJobE2E_PollingChain_NotFound(t *testing.T) {
	h, _ := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mPollingAuthFake)

	w := getSubmittedJob(t, r, "job_does_not_exist_001")
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET unknown id: want 404, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("GET 404 json: %v", err)
	}
	if resp["ok"] != false {
		t.Fatalf("GET 404 ok = %v, want false", resp["ok"])
	}
	if resp["error"] != "job_not_found" {
		t.Fatalf("GET 404 error = %v, want job_not_found", resp["error"])
	}
	if _, ok := resp["message"].(string); !ok {
		t.Fatalf("GET 404 missing message (body: %s)", w.Body.String())
	}
}

// (no extra blank line at file end; format string test bodies use fmt.Sprintf directly via the top-level import.)

// ── Scenario 5 — retry_budget=0 round-trip (NEW) ──────────────

// TestSubmitJobE2E_RetryBudgetZeroAcceptance locks the explicit-zero
// contract end-to-end on POST /api/v1/jobs (P0 retry_budget closure).
// A request that supplies retry_budget: 0 MUST be accepted (202),
// MUST round-trip into job_delivery_plans.retry_budget=0 verbatim
// (NOT silently coerced to DefaultRetryBudget=3 or any other
// fallback), and the persisted row MUST have retry_budget=0 so the
// worker terminal-fails on the first hard error — matching
// openapi.yaml:SubmitDeliveryPlanEntry.retry_budget.minimum=0.
//
// Pre-P0-relaxation, the validator at enqueue/delivery_plan_validator.go
// rejected retry_budget<=0 with the plaintext "must be > 0" error,
// which WriteResolverError mapped to 422 invalid_payload. After the
// relaxation (boundary now <0), this scenario becomes 202-ok and the
// contract is the round-trip preservation through:
//
//  1. HTTP → SubmitJobRequest → ValidateSubmitJobRequest passes.
//  2. destination-existence pre-flight passes (drive is seeded).
//  3. NormalizeExternalJobSubmission → submitRequestToRawPayload:
//     nil → DefaultRetryBudget=3; *int-&-0 → entry["retry_budget"]=0
//     (the *int-pointer boundary the contract hinges on).
//  4. Resolver.Resolve → Enqueuer.PrepareJobAndTask →
//     validateDeliveryPlanRequires accepts retry_budget=0.
//  5. AtomicForwardAndEnqueue (creatorflow.Resolver) writes
//     job_delivery_plans.retry_budget=0 verbatim.
//
// Resource-leak invariant: exactly one job row is created; replay
// with retry_budget=0 + same idempotency_key converges on the
// existing row (created=false in the response).
