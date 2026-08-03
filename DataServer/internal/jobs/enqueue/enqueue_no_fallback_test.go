package enqueue

import (
	"context"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/store"
)

// TestEnqueue_NoDestinationID_FailsClosed_NoAutoSelect_NoNameMatch_NoCrossWorkspace
// pins the four no-fallback invariants on the enqueue path. The user-facing
// sender contract is: copy the EXACT destination_id returned by
// POST /api/v1/publishing/targets into delivery_plan[].destination_id. Any
// "smart" fallback that picks a similar-looking or different-workspace
// destination would silently route the job to the wrong channel.
//
// The four invariants:
//
//	(a) NO auto-select: missing destination_id (empty plan / empty entry /
//	    empty string) MUST be rejected at the SHAPE layer. The system does
//	    NOT auto-pick the first enabled destination in delivery_destinations.
//
//	(b) NO select-by-name: destination_id pointing to a non-existent row
//	    (even if a similar-named destination exists) MUST be rejected at the
//	    EXISTENCE layer. The system does NOT fuzzy-match, prefix-match, or
//	    substring-match destination_id values.
//
//	(c) NO cross-workspace: destination_id pointing to a destination that
//	    is not in delivery_destinations (the Velox-side proxy for "owned by
//	    another workspace") MUST be rejected at the EXISTENCE layer. The
//	    system does NOT fall back to any other destination in the
//	    delivery_destinations table.
//
//	(d) Fail-closed: all rejections return a typed *validationError with a
//	    precise field path / envelope. NO silent pass-through, NO 500 panic,
//	    NO partial success, NO fallback substitution.
//
// These invariants span both validation layers:
//
//   - Shape layer: validateDeliveryPlanRequires (called by PrepareJobAndTask)
//     pins invariant (a) on the inbound payload.
//   - Existence layer: validateDeliveryDestinationTx (called inside
//     AtomicJobTaskCreator.CreateJobWithTask) pins invariants (b), (c),
//     and (d) on the resolved plan entries.
//   - Both layers must cooperate to pin invariant (d) end-to-end.
func TestEnqueue_NoDestinationID_FailsClosed_NoAutoSelect_NoNameMatch_NoCrossWorkspace(t *testing.T) {
	t.Parallel()

	// ============================================================
	// Layer 1 — SHAPE validator (validateDeliveryPlanRequires via
	// PrepareJobAndTask). Pins invariant (a): NO auto-select.
	// ============================================================

	t.Run("layer1_empty_delivery_plan_rejected_no_default_channel", func(t *testing.T) {
		t.Parallel()
		enq := newTestEnqueuer(t)
		// newTestEnqueuer seeds "drive-main" as an enabled destination.
		// If the validator ever silently substituted the first enabled
		// destination when delivery_plan was empty, this test would
		// surface that as a nil error — it would NOT fail. The assertion
		// therefore nails the contract: empty plan ⇒ hard reject.
		payload := map[string]interface{}{
			"video_name":     "Empty Delivery Plan",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan":  []interface{}{}, // empty array — system MUST NOT auto-pick a channel
		}
		_, _, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (a) violated: empty delivery_plan MUST be rejected (no auto-select); got nil error")
		}
		if !strings.Contains(err.Error(), "explicit delivery plan required") {
			t.Fatalf("invariant (d) violated: error MUST mention 'explicit delivery plan required'; got %v", err)
		}
	})

	t.Run("layer1_missing_destination_id_field_rejected_no_auto_select", func(t *testing.T) {
		t.Parallel()
		enq := newTestEnqueuer(t)
		payload := map[string]interface{}{
			"video_name":     "Missing Destination ID Field",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					// destination_id field ABSENT — system MUST NOT auto-pick
					"priority":     0,
					"retry_budget": 3,
				},
			},
		}
		_, _, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (a) violated: missing destination_id field MUST be rejected (no auto-select); got nil error")
		}
		if !strings.Contains(err.Error(), "delivery_plan.0.destination_id") || !strings.Contains(err.Error(), "is required") {
			t.Fatalf("invariant (d) violated: error MUST pinpoint 'delivery_plan.0.destination_id' + 'is required'; got %v", err)
		}
	})

	t.Run("layer1_empty_string_destination_id_rejected_no_auto_select", func(t *testing.T) {
		t.Parallel()
		enq := newTestEnqueuer(t)
		payload := map[string]interface{}{
			"video_name":     "Empty-String Destination ID",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "", // empty string — system MUST NOT auto-pick
					"priority":       0,
					"retry_budget":   3,
				},
			},
		}
		_, _, _, err := enq.PrepareJobAndTask(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (a) violated: empty-string destination_id MUST be rejected (no auto-select); got nil error")
		}
		if !strings.Contains(err.Error(), "delivery_plan.0.destination_id") {
			t.Fatalf("invariant (d) violated: error MUST pinpoint 'delivery_plan.0.destination_id'; got %v", err)
		}
	})

	// ============================================================
	// Layer 2 — EXISTENCE validator (validateDeliveryDestinationTx
	// inside AtomicJobTaskCreator.CreateJobWithTask via Enqueue +
	// mockPlanResolver). Pins invariants (b), (c), and (d) at the
	// existence layer.
	// ============================================================

	t.Run("layer2_nonexistent_destination_id_rejected_no_name_match_fallback", func(t *testing.T) {
		t.Parallel()
		// The shape layer passes (destination_id field is present and
		// non-empty, retry_budget >= 0). The existence layer MUST
		// reject because 'drive-ghost' is not in delivery_destinations.
		//
		// Crucially: 'drive-ghost' is similar in NAME shape to the
		// seeded 'drive-main' (drive- prefix). If the system did
		// ANY fuzzy / prefix / substring matching, it would substitute
		// 'drive-main'. The error envelope below proves it does not.
		db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
		if err != nil {
			t.Fatalf("sqlite store: %v", err)
		}
		seedDestinations(t, db, map[string]bool{"drive-main": true})

		// Custom mockPlanResolver returns a plan targeting "drive-ghost",
		// which is NOT in delivery_destinations. The atomic creator's
		// validateDeliveryDestinationTx MUST reject this without falling
		// back to the seeded "drive-main".
		badResolver := &mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "drive-ghost", Priority: 0, RetryBudget: 3},
			},
		}}
		enq := NewEnqueuer(
			store.NewAtomicJobTaskCreator(db),
			store.NewSQLiteJobRepository(db),
			nil,
			badResolver,
		)

		payload := map[string]interface{}{
			"video_name":     "Name-Match Fallback Test",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "drive-ghost",
					"priority":       0,
					"retry_budget":   3,
				},
			},
		}
		_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (b) violated: 'drive-ghost' (non-existent, similar NAME shape to seeded 'drive-main') MUST be rejected (no name-match fallback); got nil error")
		}
		if !strings.Contains(err.Error(), `"drive-ghost" does not exist`) {
			t.Fatalf("invariant (d) violated: error MUST be '\"drive-ghost\" does not exist' envelope; got %v", err)
		}
		// Stronger: the error MUST mention the rejected 'drive-ghost'
		// and MUST NOT mention the seeded 'drive-main' (no fallback
		// substitution).
		if !strings.Contains(err.Error(), "drive-ghost") {
			t.Fatalf("invariant (d) violated: error MUST pinpoint the rejected destination_id 'drive-ghost'; got %v", err)
		}
		if strings.Contains(err.Error(), "drive-main") {
			t.Fatalf("invariant (b) violated: error MUST NOT mention the similar-named 'drive-main' (no name-match fallback); got %v", err)
		}
	})

	t.Run("layer2_cross_workspace_destination_id_rejected_no_substitution", func(t *testing.T) {
		t.Parallel()
		// Cross-workspace proxy at the Velox layer: a destination_id
		// that does not exist in delivery_destinations is the same
		// surface as "belongs to another workspace" (the Velox
		// delivery_destinations table is flat; cross-workspace
		// boundaries are enforced upstream in InstaEdit). The test
		// seeds two enabled destinations in this workspace and a
		// submission targeting a destination NOT in delivery_destinations
		// (representing the cross-workspace case). The validator MUST
		// reject WITHOUT substituting any of the seeded destinations.
		db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
		if err != nil {
			t.Fatalf("sqlite store: %v", err)
		}
		seedDestinations(t, db, map[string]bool{
			"drive-workspace-A": true,
			"video-workspace-A": true,
		})

		badResolver := &mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "social-workspace-B", Priority: 0, RetryBudget: 3},
			},
		}}
		enq := NewEnqueuer(
			store.NewAtomicJobTaskCreator(db),
			store.NewSQLiteJobRepository(db),
			nil,
			badResolver,
		)

		payload := map[string]interface{}{
			"video_name":     "Cross-Workspace Destination",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "social-workspace-B", // NOT in this workspace's delivery_destinations
					"priority":       0,
					"retry_budget":   3,
				},
			},
		}
		_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (c) violated: 'social-workspace-B' (not in this workspace's delivery_destinations) MUST be rejected (no cross-workspace substitution); got nil error")
		}
		if !strings.Contains(err.Error(), `"social-workspace-B" does not exist`) {
			t.Fatalf("invariant (d) violated: error MUST be '\"social-workspace-B\" does not exist'; got %v", err)
		}
		// The error MUST NOT mention any of the seeded workspace-A
		// destinations (proves no fallback substitution to a same-
		// workspace channel).
		for _, seeded := range []string{"drive-workspace-A", "video-workspace-A"} {
			if strings.Contains(err.Error(), seeded) {
				t.Fatalf("invariant (c) violated: error MUST NOT mention seeded workspace-A destination %q (no substitution); got %v", seeded, err)
			}
		}
	})

	t.Run("layer2_disabled_destination_id_rejected_no_fallback_to_enabled", func(t *testing.T) {
		t.Parallel()
		// Pin invariant (c) at the disabled-vs-enabled boundary: a
		// destination_id pointing to a disabled row MUST be rejected
		// even when OTHER enabled destinations exist. No fallback
		// substitution to any enabled destination.
		db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
		if err != nil {
			t.Fatalf("sqlite store: %v", err)
		}
		seedDestinations(t, db, map[string]bool{
			"drive-main":    true,
			"video-main":    true,
			"social-mr-off": false, // explicitly disabled
		})

		badResolver := &mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "social-mr-off", Priority: 0, RetryBudget: 3},
			},
		}}
		enq := NewEnqueuer(
			store.NewAtomicJobTaskCreator(db),
			store.NewSQLiteJobRepository(db),
			nil,
			badResolver,
		)

		payload := map[string]interface{}{
			"video_name":     "Disabled Destination With Alternatives",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "social-mr-off", // disabled; enabled alternatives exist
					"priority":       0,
					"retry_budget":   3,
				},
			},
		}
		_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (c) violated: disabled destination_id MUST be rejected even when enabled alternatives exist; got nil error")
		}
		if !strings.Contains(err.Error(), `"social-mr-off" is globally disabled`) {
			t.Fatalf("invariant (d) violated: error MUST be '\"social-mr-off\" is globally disabled'; got %v", err)
		}
		// The error MUST NOT mention any enabled alternative.
		for _, seeded := range []string{"drive-main", "video-main"} {
			if strings.Contains(err.Error(), seeded) {
				t.Fatalf("invariant (c) violated: error MUST NOT mention enabled alternative %q (no fallback); got %v", seeded, err)
			}
		}
	})

	t.Run("layer2_invalid_payload_envelope_typed_not_panic", func(t *testing.T) {
		t.Parallel()
		// Pin invariant (d) at the boundary between Layer 1 (shape)
		// and Layer 2 (existence). A payload with a valid SHAPE but
		// an unselectable destination_id MUST surface as a typed
		// *validationError-style envelope — never as a 500 panic, a
		// silent nil, or a partial-success job row.
		//
		// This sub-test exercises the full Enqueue path (Layer 1 +
		// Layer 2) with a bad destination_id and asserts the resulting
		// error message is structured (contains the destination_id and
		// the "does not exist" / "is globally disabled" envelope).
		db, err := store.NewSQLiteStore(t.TempDir() + "/test.db")
		if err != nil {
			t.Fatalf("sqlite store: %v", err)
		}
		seedDestinations(t, db, map[string]bool{"drive-main": true})

		badResolver := &mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "no-such-channel", Priority: 0, RetryBudget: 3},
			},
		}}
		enq := NewEnqueuer(
			store.NewAtomicJobTaskCreator(db),
			store.NewSQLiteJobRepository(db),
			nil,
			badResolver,
		)

		payload := map[string]interface{}{
			"video_name":     "Fail-Closed Envelope",
			"script_text":    "test",
			"voiceover_path": "/tmp/v.mp3",
			"scenes":         []interface{}{map[string]interface{}{"text": "S1", "image_link": "https://example.com/i.png"}},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "no-such-channel",
					"priority":       0,
					"retry_budget":   3,
				},
			},
		}
		response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
		if err == nil {
			t.Fatal("invariant (d) violated: Enqueue with unselectable destination_id MUST return an error; got nil + response")
		}
		if response != nil {
			// Partial-success is also a fail-open path: an OK
			// response carrying a job_id would mean the job was
			// enqueued despite the bad destination, which would
			// route to no destination downstream.
			if ok, _ := response["ok"].(bool); ok {
				t.Fatalf("invariant (d) violated: Enqueue returned ok=true response=%v despite unselectable destination; fail-open path detected", response)
			}
		}
		if !strings.Contains(err.Error(), "no-such-channel") {
			t.Fatalf("invariant (d) violated: error MUST mention the rejected destination_id 'no-such-channel' (typed envelope); got %v", err)
		}
	})
}
