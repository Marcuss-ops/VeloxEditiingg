// Package pipeline — e2e validation scenario tests for POST /api/v1/jobs.
//
// This file owns:
//   - scenarios 6, 7, 8, 9 of the submit-job e2e coverage matrix
//     (TestSubmitJobE2E_ValidationFailures: missing_destination, sub_min_duration,
//     empty_scene_text, ssrf_loopback_in_voiceover, byte_rejected_idem_key).
//     Note: missing_destination is currently relaxed — the handler returns
//     500 resolver_failure rather than 422 — and is SKIPPED inside the
//     test (see the per-case comment). All other validation subtests assert
//     the 422-aggregation contract.
//   - the negative-retry-budget boundary assert
//     (TestSubmitJobE2E_NegativeRetryBudgetRejected).
//
// Counterpart file: job_submit_e2e_retry_budget_test.go covers the
// matching POSITIVE zero acceptance, scenario 5.
package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSubmitJobE2E_ValidationFailures(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// baseline-delta snapshot of resource-leak tables only.
	type tableCounts struct {
		jobs             int
		tasks            int
		taskSpecs        int
		jobDeliveryPlans int
	}
	snapshot := func() tableCounts {
		var c tableCounts
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&c.jobs); err != nil {
			t.Fatalf("snapshot jobs: %v", err)
		}
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&c.tasks); err != nil {
			t.Fatalf("snapshot tasks: %v", err)
		}
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM task_specs`).Scan(&c.taskSpecs); err != nil {
			t.Fatalf("snapshot task_specs: %v", err)
		}
		if err := db.DB().QueryRow(`SELECT COUNT(*) FROM job_delivery_plans`).Scan(&c.jobDeliveryPlans); err != nil {
			t.Fatalf("snapshot job_delivery_plans: %v", err)
		}
		return c
	}
	baseline := snapshot()

	type wantShape struct {
		detailsObj    bool
		detailsPath   string
		detailsReason string
		detailsLength int
		detailsIssue  string
	}
	cases := []struct {
		name    string
		skipMsg string
		make    func() SubmitJobRequest
		want    wantShape
	}{
		{
			// Scenario 4 — missing destination → 422 invalid_payload +
			// zero writes (handler-side destination-existence pre-flight).
			// Previously skipped because AtomicForwardAndEnqueue would
			// surface a 500 resolver_failure from the plaintext
			// "destination_id %q does not exist" error inside the
			// atomic UoW. Closed by P0 #2 (handler-side destination-
			// existence pre-check at job_submit.go::SubmitJob).
			name: "missing_destination",
			make: func() SubmitJobRequest {
				b := validSubmitJobBody("e2e-missing-dest-001")
				b.DeliveryPlan[0].DestinationID = "missing_drive"
				return b
			},
			want: wantShape{
				detailsPath:  "delivery_plan.0.destination_id",
				detailsIssue: "invalid",
			},
		},
		{
			name: "sub_min_duration",
			make: func() SubmitJobRequest {
				b := validSubmitJobBody("e2e-sub-min-001")
				b.Scenes[0].DurationSeconds = 0.05
				return b
			},
			want: wantShape{
				detailsPath:  "scenes.0.duration_seconds",
				detailsIssue: "out_of_range",
			},
		},
		{
			name: "empty_scene_text",
			make: func() SubmitJobRequest {
				b := validSubmitJobBody("e2e-empty-text-001")
				b.Scenes[0].Text = ""
				return b
			},
			want: wantShape{
				detailsPath:  "scenes.0.text",
				detailsIssue: "empty",
			},
		},
		{
			// Scenario 8 — SSRF URL rejection. Activated by P1 #2.
			name: "ssrf_loopback_in_voiceover",
			make: func() SubmitJobRequest {
				b := validSubmitJobBody("e2e-ssrf-loopback-001")
				b.VoiceoverPaths = []string{"http://127.0.0.1:8000/leak.mp3"}
				return b
			},
			want: wantShape{
				detailsPath:  "voiceover_paths/0",
				detailsIssue: "ssrf_rejected",
			},
		},
		{
			name: "byte_rejected_idem_key",
			make: func() SubmitJobRequest {
				b := validSubmitJobBody(strings.Repeat("k", MaxIdempotencyKeyLen+2))
				return b
			},
			want: wantShape{
				detailsObj:    true,
				detailsPath:   "idempotency_key",
				detailsReason: "length",
				detailsLength: MaxIdempotencyKeyLen + 2,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipMsg != "" {
				t.Skip(tc.skipMsg)
			}
			body := tc.make()
			w := postSubmitJob(t, r, body)
			wantStatus := http.StatusUnprocessableEntity
			if tc.want.detailsObj {
				wantStatus = http.StatusBadRequest
			}
			if w.Code != wantStatus {
				t.Fatalf("status = %d, want %d (full body: %s)", w.Code, wantStatus, w.Body.String())
			}
			var resp map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("response json: %v", err)
			}

			// 422 vs 400 envelope-shape branching.
			if tc.want.detailsObj {
				// 400-byte envelope: details OBJECT {path, reason, length}.
				details, ok := resp["details"].(map[string]interface{})
				if !ok {
					t.Fatalf("details not an object: %T (full body: %s)", resp["details"], w.Body.String())
				}
				if got, _ := details["path"].(string); got != tc.want.detailsPath {
					t.Errorf("details.path = %q, want %q", got, tc.want.detailsPath)
				}
				if got, _ := details["reason"].(string); got != tc.want.detailsReason {
					t.Errorf("details.reason = %q, want %q", got, tc.want.detailsReason)
				}
				if got, ok := details["length"].(float64); !ok || int(got) != tc.want.detailsLength {
					t.Errorf("details.length = %v, want %d", details["length"], tc.want.detailsLength)
				}
			} else {
				// 422-semantic envelope.
				if resp["error"] != "invalid_payload" && resp["error"] != "ssrf_rejected" {
					t.Fatalf("error = %v, want invalid_payload or ssrf_rejected (full body: %s)", resp["error"], w.Body.String())
				}
				if resp["ok"] != false {
					t.Fatalf("ok = %v, want false on rejection path", resp["ok"])
				}

				// SSRF has its own details[] shape: [{path, url, reason}].
				if resp["error"] == "ssrf_rejected" {
					details, ok := resp["details"].([]interface{})
					if !ok || len(details) == 0 {
						t.Fatalf("ssrf details missing: %T (full body: %s)", resp["details"], w.Body.String())
					}
					first, ok := details[0].(map[string]interface{})
					if !ok {
						t.Fatalf("ssrf details[0] not object: %T", details[0])
					}
					if got, _ := first["path"].(string); got != "voiceover_paths/0" {
						t.Errorf("ssrf details[0].path = %q, want voiceover_paths/0", got)
					}
					if got, _ := first["reason"].(string); got != "ip_loopback" {
						t.Errorf("ssrf details[0].reason = %q, want ip_loopback", got)
					}
				}
			}

			// Resource-leak invariant.
			after := snapshot()
			delta := []struct {
				name     string
				observed int
				base     int
			}{
				{"jobs", after.jobs, baseline.jobs},
				{"tasks", after.tasks, baseline.tasks},
				{"task_specs", after.taskSpecs, baseline.taskSpecs},
				{"job_delivery_plans", after.jobDeliveryPlans, baseline.jobDeliveryPlans},
			}
			for _, d := range delta {
				if d.observed != d.base {
					t.Errorf("%s row delta: got=%d want=%d", d.name, d.observed, d.base)
				}
			}
		})
	}
}

// ── Scenario 10 — legacy adminAuth on /api/v1/jobs ─────────────
//
// Removed in P1: adminAuth was the legacy operator-token auth on
// /api/v1/jobs, but the P1 spec retires that surface in favor of
// the dedicated M2M auth (scope=jobs.submit, per-client credentials,
// rate-limit + quota + audit). The auth-tokens surface is now
// exercised by TestSubmitJobE2E_M2MAuthEnvelopes (scenario 11)
// which covers the SAME no/wrong/right-bearer matrix via the new
// M2M middleware backed by a seeded m2m_api_keys row.
//
// The previous test (TestSubmitJobE2E_RealAdminAuthWired) exercised
// adminAuth on this route, but the fail-closed routes.go change in
// P1 makes that mount a hard panic. The auth surface is fully
// covered by the M2M envelopes test; duplicating it under a
// to-be-removed auth path would be wasteful.

// ── Scenario 11 — M2M auth envelopes (NEW for P1 #1) ───────────

// TestSubmitJobE2E_M2MAuthEnvelopes verifies that a REAL M2M
// middleware (backed by a seeded m2m_api_keys row in SQLite) emits
// the canonical error envelope for each rejection path:
//
//   - missing Authorization header   → 401 m2m_token_required
//   - bearer = wrong plaintext       → 401 m2m_token_rejected
//   - bearer = valid plaintext       → 202 accepted (and the
//     audit log gains a row with
//     status_code=202)
//
// This is the regression-detector for the M2M wiring: a future
// change that strips the M2M middleware off /api/v1/jobs, or that
// reverts to legacy adminAuth, MUST fail here loudly.
func TestSubmitJobE2E_NegativeRetryBudgetRejected(t *testing.T) {
	h, db := newSubmitJobE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	// Resource-leak baseline snapshot. The seeded DB has only the
	// "drive" dest row, so any new jobs/tasks/task_specs/job_delivery_plans
	// row is a leak (the rejection path must not write rows).
	type tableCounts struct {
		jobs, tasks, taskSpecs, jobDeliveryPlans int
	}
	snapshot := func() tableCounts {
		var c tableCounts
		_ = db.DB().QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&c.jobs)
		_ = db.DB().QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&c.tasks)
		_ = db.DB().QueryRow(`SELECT COUNT(*) FROM task_specs`).Scan(&c.taskSpecs)
		_ = db.DB().QueryRow(`SELECT COUNT(*) FROM job_delivery_plans`).Scan(&c.jobDeliveryPlans)
		return c
	}
	baseline := snapshot()

	rb := -1
	body := validSubmitJobBody("e2e-retry-budget-negative-001")
	body.DeliveryPlan[0].RetryBudget = &rb

	w := postSubmitJob(t, r, body)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "invalid_payload" {
		t.Fatalf("error = %v, want invalid_payload (negative retry_budget is the only rejected class)", resp["error"])
	}
	if resp["ok"] != false {
		t.Fatalf("ok = %v, want false on rejection path", resp["ok"])
	}

	// Tightened envelope shape check (mirrors the ssrf_loopback_in_voiceover
	// subcase in TestSubmitJobE2E_ValidationFailures): a future regression
	// that emits 422 with a DIFFERENT error code (payload_incomplete,
	// resolver_failure, …) or that drops details.path must NOT silently
	// pass. WriteResolverError emits details:[{path:<field>, issue:"invalid"}]
	// for any *validationError from the enqueue layer; the path is
	// validator-generated via enqueue.ValidationErrorField().
	detailsArr, ok := resp["details"].([]interface{})
	if !ok {
		t.Fatalf("details must be []interface{} for invalid_payload 422, got %T (full body: %s)", resp["details"], w.Body.String())
	}
	if len(detailsArr) != 1 {
		t.Fatalf("details length = %d, want 1 — WriteResolverError must emit exactly one issue per rejection (full body: %s)", len(detailsArr), w.Body.String())
	}
	first, ok := detailsArr[0].(map[string]interface{})
	if !ok {
		t.Fatalf("details[0] not map[string]interface{}: %T (got: %#v)", detailsArr[0], detailsArr[0])
	}
	if gotPath, _ := first["path"].(string); gotPath != "delivery_plan.0.retry_budget" {
		t.Errorf("details[0].path = %q, want \"delivery_plan.0.retry_budget\" (validator-emitted field path; dot-notation per the path-format alignment commit)", gotPath)
	}
	if gotIssue, _ := first["issue"].(string); gotIssue != "invalid" {
		t.Errorf("details[0].issue = %q, want \"invalid\" (canonical issue token from WriteResolverError's validationFieldExtractor branch)", gotIssue)
	}

	// Resource-leak invariant (negative-rejection path must NOT write rows).
	after := snapshot()
	if after.jobs != baseline.jobs {
		t.Errorf("jobs row delta: got=%d want=%d", after.jobs, baseline.jobs)
	}
	if after.tasks != baseline.tasks {
		t.Errorf("tasks row delta: got=%d want=%d", after.tasks, baseline.tasks)
	}
	if after.taskSpecs != baseline.taskSpecs {
		t.Errorf("task_specs row delta: got=%d want=%d", after.taskSpecs, baseline.taskSpecs)
	}
	if after.jobDeliveryPlans != baseline.jobDeliveryPlans {
		t.Errorf("job_delivery_plans row delta: got=%d want=%d", after.jobDeliveryPlans, baseline.jobDeliveryPlans)
	}
}
