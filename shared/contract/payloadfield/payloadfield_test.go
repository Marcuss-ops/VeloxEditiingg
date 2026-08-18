package payloadfield

import (
	"sort"
	"testing"

	"velox-shared/contract"
)

// TestJobPayloadV2KeysMatchCanonicalTopLevelKeys pins the generated
// top-level key set against contract.CanonicalTopLevelKeys. The
// contractgen parity check enforces this at generation time; this test
// keeps the invariant visible to the ordinary `go test ./...` suite
// (and to the DataServer/worker modules that consume shared via replace).
func TestJobPayloadV2KeysMatchCanonicalTopLevelKeys(t *testing.T) {
	got := JobPayloadV2Keys()
	want := append([]string(nil), contract.CanonicalTopLevelKeys...)
	sort.Strings(got)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("generated key count = %d, canonical key count = %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("key mismatch at index %d: generated %q != canonical %q", i, got[i], want[i])
		}
	}
}

// TestJobPayloadV2KeysReturnsFreshSlice pins the immutable-snapshot
// behavior: mutating one returned slice must not leak into the next.
func TestJobPayloadV2KeysReturnsFreshSlice(t *testing.T) {
	first := JobPayloadV2Keys()
	second := JobPayloadV2Keys()
	if len(first) == 0 {
		t.Fatal("JobPayloadV2Keys returned an empty slice")
	}
	first[0] = "corrupted"
	if second[0] == "corrupted" {
		t.Fatal("JobPayloadV2Keys returned a shared (mutable) backing array")
	}
}

// TestSpotValues pins a representative subset of generated constants to
// their wire values so a regeneration that renames identifiers or values
// fails loudly instead of silently.
func TestSpotValues(t *testing.T) {
	cases := []struct {
		got  string
		want string
	}{
		{JobID, "job_id"},
		{JobRunID, "job_run_id"},
		{RenderManifest, "render_manifest"},
		{DeliveryPlan, "delivery_plan"},
		{CompiledRenderPlanJSON, "compiled_render_plan_json"},
		{CompiledRenderPlanSHA256, "compiled_render_plan_sha256"},
		{RenderManifestSchema, "schema"},
		{RenderManifestCanvasFPSNum, "fps_num"},
		{RenderManifestAssetsSHA256, "sha256"},
		{RenderManifestTracksEventsTimelineStartMS, "timeline_start_ms"},
		{DeliveryPlanDestinationID, "destination_id"},
		{DeliveryPlanRetryBudget, "retry_budget"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("constant value = %q, want %q", c.got, c.want)
		}
	}
}
