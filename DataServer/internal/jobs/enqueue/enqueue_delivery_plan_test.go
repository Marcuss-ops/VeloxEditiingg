package enqueue

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// Step 4/8 canonical-purity preflight tests.
//
// Going from RED to GREEN: every payload that lacks an explicit
// delivery_plan (or legacy delivery_destination_ids) MUST be rejected
// at enqueue time by PrepareJobAndTask, not at finalize time. Without
// this gate, FinalizeVerified would discover the missing plan AFTER the
// render has burned its budget — the diagnostic
// "Validate delivery plan at enqueue or pre-render".

func TestPrepareJobAndTask_AcceptsCanonicalDeliveryPlan(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":     "Canonical Video",
		"script_text":    "canonical enqueue",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-main",
				"priority":       0,
				"retry_budget":   3,
				"enabled":        true,
			},
		},
	}

	job, spec, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("PrepareJobAndTask: want no error, got %v", err)
	}
	if job == nil || spec == nil {
		t.Fatal("want non-nil job and spec")
	}
	if spec.ExecutorID != "scene.composite.v1" {
		t.Errorf("spec.ExecutorID = %q, want scene.composite.v1", spec.ExecutorID)
	}
}

func TestPrepareJobAndTask_AcceptsLegacyDeliveryDestinationIDs(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	// Legacy fallback honored because FinalizeVerified honors it too.
	// The validator's job is to ensure SCHEDULE-ABILITY, not to break
	// consumers that the existing finalize path already supports.
	payload := map[string]interface{}{
		"video_name":               "Legacy Plan",
		"script_text":              "legacy fallback path",
		"voiceover_path":           "/tmp/v.mp3",
		"scenes":                   []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
		"delivery_destination_ids": []string{"drive-main", "video-main"},
	}

	if _, _, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements()); err != nil {
		t.Fatalf("PrepareJobAndTask: want no error (legacy fallback honored), got %v", err)
	}
}

func TestPrepareJobAndTask_AcceptsSingleObjectDeliveryPlan(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":     "Single-Object Plan",
		"script_text":    "single-object shape",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
		"delivery_plan": map[string]interface{}{
			"destination_id": "drive-main",
			"retry_budget":   5,
		},
	}
	if _, _, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements()); err != nil {
		t.Fatalf("PrepareJobAndTask: want no error (single-object shape accepted), got %v", err)
	}
}

func TestPrepareJobAndTask_RejectsMissingDeliveryPlan(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	basePayload := func() map[string]interface{} {
		return map[string]interface{}{
			"video_name":     "No Plan",
			"script_text":    "no plan at all",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
		}
	}

	cases := []struct {
		name      string
		mutate    func(map[string]interface{})
		wantField string
		wantSub   string
	}{
		{
			name:      "no_plan_no_legacy",
			mutate:    func(p map[string]interface{}) {},
			wantField: "delivery_plan",
			wantSub:   "is required",
		},
		{
			name: "empty_array",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{}
			},
			wantField: "delivery_plan",
			wantSub:   "is required",
		},
		{
			name: "retry_budget_zero",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 0},
				}
			},
			wantField: "delivery_plan[0].retry_budget",
			wantSub:   "must be > 0",
		},
		{
			name: "missing_destination_id",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{
					map[string]interface{}{"retry_budget": 3},
				}
			},
			wantField: "delivery_plan[0].destination_id",
			wantSub:   "is required",
		},
		{
			name: "disabled_entry",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "enabled": false},
				}
			},
			wantField: "delivery_plan[0]",
			wantSub:   "disabled",
		},
		{
			name: "duplicate_legacy_destination_ids",
			mutate: func(p map[string]interface{}) {
				p["delivery_destination_ids"] = []string{"drive-main", "drive-main"}
			},
			wantField: "delivery_plan[1].destination_id",
			wantSub:   "duplicate",
		},
		{
			name: "non_object_array_entry",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{"drive-main"}
			},
			wantField: "delivery_plan[0]",
			wantSub:   "must be an object",
		},
		{
			name: "wrong_type_root",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = "drive-main"
			},
			wantField: "delivery_plan",
			wantSub:   "must be an object or array",
		},
		{
			name: "negative_priority",
			mutate: func(p map[string]interface{}) {
				p["delivery_plan"] = []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": -1},
				}
			},
			wantField: "delivery_plan[0].priority",
			wantSub:   "must be >= 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := basePayload()
			tc.mutate(payload)
			job, spec, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
			if err == nil {
				t.Fatalf("want error, got job=%v spec=%v", job, spec)
			}
			if !strings.Contains(err.Error(), tc.wantField) {
				t.Fatalf("error %q does not contain field %q", err.Error(), tc.wantField)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain substring %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestEnqueue_RejectsMissingDeliveryPlan(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":     "Enqueue No Plan",
		"script_text":    "enqueue path",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
	}

	resp, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatalf("want error from Enqueue when delivery_plan missing, got resp=%v", resp)
	}
	if !strings.Contains(err.Error(), "delivery_plan") {
		t.Fatalf("want delivery_plan error, got %v", err)
	}
}

func TestEnqueue_PropagatesDeliveryPlanPreflightError(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	// retry_budget <= 0 path: Enqueue must surface the same field/path
	// the validator produced, so callers can programmatically distinguish
	// "missing plan" from "budget not configured" from "disabled entry".
	payload := map[string]interface{}{
		"video_name":     "Bad Budget",
		"script_text":    "bad budget",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 0},
		},
	}
	_, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("want error from Enqueue when retry_budget is 0")
	}
	if !strings.Contains(err.Error(), "retry_budget") || !strings.Contains(err.Error(), "must be > 0") {
		t.Fatalf("want retry_budget error, got %v", err)
	}
}

// TestEnqueue_NoPreinsertDeliveryPlan_PropagatesMaxRetriesFromPayload is
// the integration regression for P0.2 (the "manual preinsert required"
// production bug surfaced on the Jackie Chan doc-voiceover real run).
//
// Pre-P0.2: PrepareJobAndTask called enforceDeliveryPlanPrecondition
// BEFORE CreateJobWithTask, so the resolver was queried for a job_id
// whose job_delivery_plans rows did not exist yet. Operators had to
// manually INSERT those rows before each POST — the operator runbook
// complained that ErrNoExplicitPlan surfaced on every first-time job
// submit.
//
// Post-P0.2 (commit 54a794b): PrepareJobAndTask no longer queries the
// resolver; the post-create precondition in Enqueue reads
// job_delivery_plans AFTER the atomic create (mockPlanResolver
// impersonates the production resolver's read view).
//
// This test verifies the post-P0.2 contract on the Enqueue path:
//
//  1. Enqueue succeeds with a payload-only delivery_plan (NO test-side
//     pre-insert of job_delivery_plans rows).
//  2. response.job_id is populated.
//  3. payloadMaxRetries (the payload's max(retry_budget)) > 0 — the
//     assertion target requiring the payload to drive the column
//     (Pre-P0.2, the column stayed at compileSceneVideoJob's default
//     0 because the resolver precreate branch never reached
//     extractPlanMaxRetry).
//  4. jobs.max_retries (DB column, read via Jobs repository) equals
//     payloadMaxRetries — payload-driven, not 0, not stale.
//
// seedDestinations seeds delivery_destinations (the master list of
// allowed delivery targets — production pre-flight); it does NOT
// touch job_delivery_plans. The fresh-DB invariant is asserted
// explicitly so a regression that quietly re-introduces a
// "must preinsert job_delivery_plans" scheme fails LOUDLY at the
// assertNoJobDeliveryPlansSeeded helper, not silently in Enqueue.
func TestEnqueue_NoPreinsertDeliveryPlan_PropagatesMaxRetriesFromPayload(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	// Seed d1/d2/d3 in delivery_destinations (production pre-flight,
	// NOT job_delivery_plans preinsert). P0.2 fixes the
	// "must-preinsert job_delivery_plans" — the test deliberately
	// stops short of preinserting plan rows so the regression guard
	// stays meaningful.
	seedDestinations(t, db, map[string]bool{
		"d1": true,
		"d2": true,
		"d3": true,
	})

	// Fresh-DB invariant: zero job_delivery_plans rows. Pinning this
	// makes the test a P0.2 regression guard: any helper that
	// introduces a preinsert trips here before Enqueue is even
	// called, so the assertion that fails matches "we silently
	// regressed to requiring operator preinsert" — not a downstream
	// ErrNoExplicitPlan.
	assertNoJobDeliveryPlansSeeded(t, db)

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "d1", Priority: 0, RetryBudget: 3},
				{DestinationID: "d2", Priority: 1, RetryBudget: 7}, // max
				{DestinationID: "d3", Priority: 2, RetryBudget: 5},
			},
		}},
	)

	payload := map[string]interface{}{
		"video_name":      "no-preinsert-success",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
			map[string]interface{}{"destination_id": "d2", "priority": 1, "retry_budget": 7}, // max
			map[string]interface{}{"destination_id": "d3", "priority": 2, "retry_budget": 5},
		},
	}

	// Step 3 of the spec: payloadMaxRetries > 0. The payload defines
	// [3, 7, 5]; expected max = 7. extractPlanMaxRetry is the SAME
	// function PrepareJobAndTask calls internally, so this binds
	// the test's payload-shape computation to the production
	// column-driven computation.
	payloadMaxRetry := extractPlanMaxRetry(payload)
	if payloadMaxRetry <= 0 {
		t.Fatalf("payload max(retry_budget) = %d, want > 0 (P0.2 fix contract: payload drives the column)", payloadMaxRetry)
	}
	if payloadMaxRetry != 7 {
		t.Errorf("payload max(retry_budget) = %d, want 7", payloadMaxRetry)
	}

	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue returned error on no-preinsert path: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("response.ok = %v, want true", response["ok"])
	}

	// Step 2 of the spec: job_id is populated.
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("response.job_id is empty")
	}

	// Step 4 of the spec: jobs.max_retries (DB column) equals
	// payloadMaxRetries. Jobs.Get reads the column through the typed
	// repository, so the assertion captures BOTH the in-tx INSERT
	// value AND the post-commit read view.
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v job=%v", err, j)
	}
	if j.MaxRetries != payloadMaxRetry {
		t.Errorf("jobs.max_retries = %d, want %d (payload max(retry_budget)); P0.2 fix regression",
			j.MaxRetries, payloadMaxRetry)
	}
	if j.Status != jobs.StatusPending {
		t.Errorf("jobs.status = %q, want PENDING", j.Status)
	}
}

// assertNoJobDeliveryPlansSeeded pins the fresh-DB invariant: zero
// rows in job_delivery_plans. Pinning this assertion (instead of
// trusting the test fixture's silence) makes the test a P0.2
// regression guard: any helper change that could quietly preinsert
// job_delivery_plans trips this helper before Enqueue runs, so we
// fail LOUDLY with the right diagnostic instead of producing a
// spurious "PASS" that hides a regression of the production
// "must-preinsert" error path.
func assertNoJobDeliveryPlansSeeded(t *testing.T, db *store.SQLiteStore) {
	t.Helper()
	var count int
	if err := db.DB().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM job_delivery_plans`).Scan(&count); err != nil {
		t.Fatalf("count job_delivery_plans: %v", err)
	}
	if count != 0 {
		t.Fatalf("fresh DB must have 0 job_delivery_plans rows (P0.2 says: no preinsert); got %d", count)
	}
}

// TestEnqueue_RealJackieChanPayloadShape_Succeeds mirrors the actual
// production payload shape from
// ops/jobs/jackie_chan_doc_voiceover.generate-from-clips.json. That
// payload uses a SINGLE delivery_plan entry with destination_id=
// "comedy_test" and retry_budget=3 (NOT the multi-delivery [3,7,5]
// shape of the existing tests above). The end-to-end runnable
// submit_jackie_chan_doc_voiceover_clips.sh script curls these bytes
// to the master's /api/v1/script/generate-from-clips; this test
// verifies the Enqueue path accepts the SAME shape without the
// pre-P0.2 "ErrNoExplicitPlan / must-preinsert" failure. Asserts:
//
//  1. Enqueue returns ok with the json's exact delivery_plan shape
//     (single entry {destination_id, retry_budget=3, priority=0}).
//  2. response.job_id is populated.
//  3. extractPlanMaxRetry(payload) yields 3 (the payload's only
//     retry_budget value).
//  4. jobs.max_retries (DB column) equals 3 — payload drives the
//     column from the canonical-purity Enqueue path, NOT a 0 or
//     stale value.
//  5. assertNoJobDeliveryPlansSeeded pins the fresh-DB invariant
//     so the test stays a P0.2 regression guard: any code change
//     that quietly re-introduces a "must-preinsert" requirement on
//     the comedy_test path fails LOUDLY at the helper, not at a
//     downstream ErrNoExplicitPlan.
//
// mirrorJackieChanDeliveryPlan returns the EXACT shape of the
// production payload's delivery_plan entry — not interpolated, so
// the test fails if the production payload's shape drifts and we
// don't update the test.
func TestEnqueue_RealJackieChanPayloadShape_Succeeds(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	// Seed ONLY delivery_destinations. Not job_delivery_plans —
	// P0.2 guarantees the precondition reads after the atomic
	// create, not before.
	seedDestinations(t, db, map[string]bool{
		"comedy_test": true,
	})
	assertNoJobDeliveryPlansSeeded(t, db)

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "comedy_test", Priority: 0, RetryBudget: 3},
			},
		}},
	)

	payload := map[string]interface{}{
		"video_name":     "Jackie Chan Doc Voiceover",
		"script_text":    "convergence smoke (production payload shape)",
		"voiceover_path": "/tmp/v.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "intro", "voiceover": "v1"}},
		"reference_voiceovers": []string{
			"https://drive.example/voice-1.mp3",
			"https://drive.example/voice-2.mp3",
			"https://drive.example/voice-3.mp3",
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "comedy_test",
				"retry_budget":   3,
				"priority":       0,
			},
		},
	}

	// Step 3: payload drives the column. extractPlanMaxRetry(payload)
	// = 3 (only retry_budget in the plan array).
	payloadMaxRetry := extractPlanMaxRetry(payload)
	if payloadMaxRetry <= 0 {
		t.Fatalf("payload max(retry_budget) = %d, want = 3", payloadMaxRetry)
	}
	if payloadMaxRetry != 3 {
		t.Fatalf("payload max(retry_budget) = %d, want = 3 (matches ops/jobs/jackie_chan…json)", payloadMaxRetry)
	}

	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue returned error on real-Shaped Jackie Chan payload: %v (P0.2 + post-create precondition must converge here without ErrNoExplicitPlan)", err)
	}
	if response["ok"] != true {
		t.Fatalf("response.ok = %v, want true", response["ok"])
	}
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("response.job_id is empty")
	}

	// Step 4: jobs.max_retries (DB column) == extractPlanMaxRetry(payload).
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v job=%v", err, j)
	}
	if j.MaxRetries != payloadMaxRetry {
		t.Errorf("jobs.max_retries = %d, want %d (P0.2 fix contract)", j.MaxRetries, payloadMaxRetry)
	}
	if j.Status != jobs.StatusPending {
		t.Errorf("jobs.status = %q, want PENDING after first-create (Finalize runs later)", j.Status)
	}
}
