package enqueue

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
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
		"video_name":              "Legacy Plan",
		"script_text":             "legacy fallback path",
		"voiceover_path":          "/tmp/v.mp3",
		"scenes":                  []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
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
