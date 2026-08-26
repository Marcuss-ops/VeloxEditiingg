package observability

import (
	"testing"

	sharedtelemetry "velox-shared/telemetry"
)

func TestBuildAttemptWaterfallPartitionsAttemptWall(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		{Name: sharedtelemetry.MilestoneAllAssetsReady, ElapsedMS: 125},
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	got := BuildAttemptWaterfall("attempt-1", samples, 330)
	if got.AccountedMS != got.WallMS || got.UnaccountedMS != 0 || got.CoveragePct != 100 {
		t.Fatalf("waterfall = %+v, want complete wall-clock coverage", got)
	}
	if len(got.Buckets) != 12 {
		t.Fatalf("bucket count = %d, want 12", len(got.Buckets))
	}
	for i := 1; i < len(got.Buckets); i++ {
		if got.Buckets[i].StartMS != got.Buckets[i-1].EndMS {
			t.Fatalf("bucket %d starts at %d after bucket %d ends at %d", i, got.Buckets[i].StartMS, i-1, got.Buckets[i-1].EndMS)
		}
	}
}

// TestBuildAttemptWaterfall_MissingMilestoneNeverFabricated locks the STEP C
// "unknown instead of inventing" rule: when a mid-timeline boundary milestone
// (here assets.all_ready) is absent, the spanning bucket must be SKIPPED and
// reported as missing — never silently attributed to a neighbouring bucket.
func TestBuildAttemptWaterfall_MissingMilestoneNeverFabricated(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		// assets.all_ready deliberately absent.
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	got := BuildAttemptWaterfall("attempt-missing", samples, 330)

	// The asset_preparation bucket must not exist: it spans the missing
	// milestone and would require inventing assets.all_ready.
	for _, bucket := range got.Buckets {
		if bucket.Name == "asset_preparation" {
			t.Fatalf("asset_preparation bucket was fabricated despite missing assets.all_ready: %+v", bucket)
		}
	}

	// The truly unknown stretch must surface as unaccounted ms and a missing
	// milestone, not be hidden or misattributed.
	if got.UnaccountedMS == 0 {
		t.Fatal("missing milestone produced no unaccounted_ms")
	}
	if !containsStr(got.MissingMilestones, string(sharedtelemetry.MilestoneAllAssetsReady)) {
		t.Fatalf("missing_milestones = %v, want assets.all_ready present", got.MissingMilestones)
	}
	if got.CoveragePct >= 100 {
		t.Fatalf("coverage_pct = %f, want < 100 when a milestone is missing", got.CoveragePct)
	}

	// assets.all_ready is the boundary for BOTH asset_preparation and
	// pre_plan_wait, so neither bucket may exist; the timeline resumes at
	// plan_compile (the first bucket after the missing boundary).
	for _, name := range []string{"asset_preparation", "pre_plan_wait"} {
		for _, bucket := range got.Buckets {
			if bucket.Name == name {
				t.Fatalf("%s bucket fabricated despite missing assets.all_ready: %+v", name, bucket)
			}
		}
	}
	hasPlanCompile := false
	for _, bucket := range got.Buckets {
		if bucket.Name == "plan_compile" {
			hasPlanCompile = true
			break
		}
	}
	if !hasPlanCompile {
		t.Fatal("plan_compile bucket missing after a mid-timeline gap")
	}
}

// TestWaterfallAccountsForAttemptWall locks the STEP C(+test) invariant:
// accounted_ms + unknown_ms ≈ attempt_wall_ms (tolerance ≤100ms or ≤1%),
// with coverage_pct > 98% on a well-formed attempt. The unknown_ms bucket is
// the builder's unaccounted_ms, which must always reconcile the wall even when
// a milestone is missing (never inventing a duration to hide the gap).
func TestWaterfallAccountsForAttemptWall(t *testing.T) {
	full := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		{Name: sharedtelemetry.MilestoneAllAssetsReady, ElapsedMS: 125},
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	t.Run("complete timeline covers the whole wall", func(t *testing.T) {
		got := BuildAttemptWaterfall("a", full, 330)
		assertAccountsWall(t, got, 330)
		if got.CoveragePct != 100 || got.UnaccountedMS != 0 {
			t.Fatalf("complete timeline coverage=%f unaccounted=%d, want 100/0", got.CoveragePct, got.UnaccountedMS)
		}
	})

	t.Run("small unknown gap stays under 2% (coverage > 98%)", func(t *testing.T) {
		// Milestones account for 330ms but the attempt wall is 333ms, leaving a
		// 3ms unknown tail — a realistic cross-machine boundary (master ingest).
		// The invariant must reconcile that and coverage must exceed 98%.
		got := BuildAttemptWaterfall("b", full, 333)
		assertAccountsWall(t, got, 333)
		if got.CoveragePct <= 98 {
			t.Fatalf("coverage_pct = %f, want > 98 for a nearly-complete attempt", got.CoveragePct)
		}
		if got.UnaccountedMS != 3 {
			t.Fatalf("unaccounted_ms = %d, want the 3ms trailing gap", got.UnaccountedMS)
		}
	})

	t.Run("missing milestone surfaces an honest unaccounted region", func(t *testing.T) {
		missing := []sharedtelemetry.AttemptMilestoneSample{
			{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
			{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
			{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
			{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130}, // assets.all_ready absent
			{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
			{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
			{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
			{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
			{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
			{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
			{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
			{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
		}
		got := BuildAttemptWaterfall("c", missing, 330)
		// Even with a missing milestone the sum MUST reconcile the wall (never
		// invent a phantom bucket); the unknown region is where it lands.
		assertAccountsWall(t, got, 330)
		if got.CoveragePct >= 98 {
			t.Fatalf("coverage_pct = %f, want < 98 when a boundary milestone is missing", got.CoveragePct)
		}
		if got.UnaccountedMS == 0 {
			t.Fatal("missing milestone left zero unaccounted_ms")
		}
		if !containsStr(got.MissingMilestones, string(sharedtelemetry.MilestoneAllAssetsReady)) {
			t.Fatalf("missing_milestones = %v, want assets.all_ready", got.MissingMilestones)
		}
	})
}

// assertAccountsWall checks accounted_ms + unaccounted_ms ≈ wall_ms within
// both the absolute (≤100ms) and relative (≤1%) tolerances.
func assertAccountsWall(t *testing.T, got AttemptWaterfall, wallMS int64) {
	t.Helper()
	sum := got.AccountedMS + got.UnaccountedMS
	diff := wallMS - sum
	if diff < 0 {
		diff = -diff
	}
	if diff > 100 {
		t.Fatalf("accounted+unaccounted = %d but wall = %d (Δ %dms > 100ms)", sum, wallMS, diff)
	}
	if wallMS > 0 && float64(diff)/float64(wallMS)*100 > 1.0 {
		t.Fatalf("accounted+unaccounted = %d vs wall = %d exceeds 1%% (Δ %dms)", sum, wallMS, diff)
	}
}

// TestBuildAttemptWaterfall_InvertedPairNeverSilentlyDropped locks the rule that
// an inverted boundary pair (end < start — duplicate/late milestone, clock skew)
// must be reported explicitly instead of silently skipping the bucket: the pair
// lands in inverted_buckets and its span stays in unaccounted_ms as honest UNKNOWN.
func TestBuildAttemptWaterfall_InvertedPairNeverSilentlyDropped(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		// render.started AFTER render.completed: inverted pair for the render bucket.
		{Name: sharedtelemetry.MilestoneAllAssetsReady, ElapsedMS: 125},
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 260},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 330},
	}

	got := BuildAttemptWaterfall("attempt-inverted", samples, 330)

	// The inverted bucket must not be built, and the pair must be reported.
	for _, bucket := range got.Buckets {
		if bucket.Name == "render" {
			t.Fatalf("render bucket was built from an inverted pair: %+v", bucket)
		}
	}
	if !containsStr(got.InvertedBuckets, "render") {
		t.Fatalf("inverted_buckets = %v, want render present", got.InvertedBuckets)
	}
	// The corruption is NOT masked: the inverted span leaks into the preceding
	// bucket (pre_render_wait ends at the impossible render.started=260), so the
	// milestone claims over-cover the 330ms wall. That over-accounting surfaces
	// as NEGATIVE unaccounted_ms and coverage > 100 — never a fake 100% report.
	if got.UnaccountedMS >= 0 {
		t.Fatalf("unaccounted_ms = %d, want negative to surface the inverted-pair corruption", got.UnaccountedMS)
	}
	if got.CoveragePct <= 100 {
		t.Fatalf("coverage_pct = %f, want > 100 when a pair is inverted", got.CoveragePct)
	}
}

// TestBuildAttemptWaterfall_OverlapNeverMasked locks the anti-masking rule: when
// the milestone timeline over-covers the wall (accounted > wall — overlapping
// buckets, duplicated milestones, cross-machine skew), unaccounted_ms must go
// NEGATIVE and coverage_pct must EXCEED 100. The corruption is surfaced, not
// clamped away into a fake "100% covered" report.
func TestBuildAttemptWaterfall_OverlapNeverMasked(t *testing.T) {
	samples := []sharedtelemetry.AttemptMilestoneSample{
		{Name: sharedtelemetry.MilestoneAttemptAccepted, ElapsedMS: 0},
		{Name: sharedtelemetry.MilestoneExecutionStarted, ElapsedMS: 10},
		{Name: sharedtelemetry.MilestoneAssetsRequested, ElapsedMS: 25},
		{Name: sharedtelemetry.MilestoneAllAssetsReady, ElapsedMS: 125},
		{Name: sharedtelemetry.MilestonePlanStarted, ElapsedMS: 130},
		{Name: sharedtelemetry.MilestonePlanCompleted, ElapsedMS: 150},
		{Name: sharedtelemetry.MilestoneRenderStarted, ElapsedMS: 155},
		{Name: sharedtelemetry.MilestoneRenderCompleted, ElapsedMS: 255},
		{Name: sharedtelemetry.MilestoneOutputDurable, ElapsedMS: 270},
		{Name: sharedtelemetry.MilestonePublishStarted, ElapsedMS: 280},
		{Name: sharedtelemetry.MilestonePublishCompleted, ElapsedMS: 320},
		{Name: sharedtelemetry.MilestoneResultSent, ElapsedMS: 325},
		// attempt.completed claims 360ms but the wall is only 330ms: 30ms of
		// over-accounting that must NOT be clamped to zero/100%.
		{Name: sharedtelemetry.MilestoneAttemptCompleted, ElapsedMS: 360},
	}

	got := BuildAttemptWaterfall("attempt-overlap", samples, 330)
	if got.UnaccountedMS >= 0 {
		t.Fatalf("unaccounted_ms = %d, want negative to surface the overlap", got.UnaccountedMS)
	}
	if got.CoveragePct <= 100 {
		t.Fatalf("coverage_pct = %f, want > 100 to surface the overlap", got.CoveragePct)
	}
	if got.AccountedMS != 360 {
		t.Fatalf("accounted_ms = %d, want the honest 360ms over-accounting kept", got.AccountedMS)
	}
	if len(got.MissingMilestones) != 0 || len(got.InvertedBuckets) != 0 {
		t.Fatalf("missing=%v inverted=%v, want no diagnostics for a pure overlap", got.MissingMilestones, got.InvertedBuckets)
	}
}

func containsStr(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func TestDecodeAttemptWaterfallAcceptsProtoJSONMilestones(t *testing.T) {
	raw := `{"milestones":[{"name":"attempt.accepted","sequence":"1","elapsedMs":"0"},{"name":"execution.started","sequence":"2","elapsedMs":"5"},{"name":"attempt.completed","sequence":"3","elapsedMs":"10"}]}`
	got := decodeAttemptWaterfall(raw, "attempt-2", 10)
	if got == nil || got.WallMS != 10 {
		t.Fatalf("decoded waterfall = %+v, want non-nil wall 10", got)
	}
	if len(got.MissingMilestones) == 0 {
		t.Fatal("expected missing milestone diagnostics for incomplete timeline")
	}
}

// realisticAttemptReportJSON is a realistic ~328s attempt timeline as the
// worker emits it in the durable raw report (the design's canonical case:
// a ~680MB job where asset preparation dominates). The milestone elapsed_ms
// values exactly tile the 328041ms attempt wall, so a decode of this report
// must account for 100%% of the wall.
const realisticAttemptReportJSON = `{"milestones":[
{"name":"attempt.accepted","sequence":1,"elapsed_ms":0},
{"name":"execution.started","sequence":2,"elapsed_ms":34},
{"name":"assets.requested","sequence":3,"elapsed_ms":68},
{"name":"assets.all_ready","sequence":4,"elapsed_ms":298314},
{"name":"plan.started","sequence":5,"elapsed_ms":298406},
{"name":"plan.completed","sequence":6,"elapsed_ms":299847},
{"name":"render.started","sequence":7,"elapsed_ms":299939},
{"name":"render.completed","sequence":8,"elapsed_ms":306761},
{"name":"output.durable","sequence":9,"elapsed_ms":310755},
{"name":"publish.started","sequence":10,"elapsed_ms":310766},
{"name":"publish.completed","sequence":11,"elapsed_ms":327510},
{"name":"result.sent","sequence":12,"elapsed_ms":328039},
{"name":"attempt.completed","sequence":13,"elapsed_ms":328041}
]}`

// TestDecodeAttemptWaterfallAccountsForEveryAttemptWall locks the STEP C(+test)
// invariant on the decode path: for every decoded attempt report the waterfall
// must reconcile accounted_ms + unknown_ms ≈ attempt_wall_ms (≤100ms or ≤1%)
// with coverage_pct > 98%% on a well-formed timeline — the same guarantee the
// builder-level test pins, applied to the realistic worker wire shape.
func TestDecodeAttemptWaterfallAccountsForEveryAttemptWall(t *testing.T) {
	t.Run("realistic 328s report covers the whole wall", func(t *testing.T) {
		got := decodeAttemptWaterfall(realisticAttemptReportJSON, "attempt-328s", 328041)
		if got == nil {
			t.Fatal("decode returned nil for a realistic full report")
		}
		assertAccountsWall(t, *got, 328041)
		if got.CoveragePct != 100 || got.UnaccountedMS != 0 || len(got.MissingMilestones) != 0 {
			t.Fatalf("realistic report coverage=%f unaccounted=%d missing=%v, want 100/0/none", got.CoveragePct, got.UnaccountedMS, got.MissingMilestones)
		}
		if len(got.Buckets) != 12 {
			t.Fatalf("bucket count = %d, want 12", len(got.Buckets))
		}
		// asset_preparation must be the dominant bucket: this is the whole point
		// of the drill-down — the ~300s mystery lives here.
		for _, b := range got.Buckets {
			if b.Name == "asset_preparation" && b.DurationMS != 298246 {
				t.Fatalf("asset_preparation = %dms, want 298246ms", b.DurationMS)
			}
		}
	})

	t.Run("cross-machine tail gap stays over 98%% coverage", func(t *testing.T) {
		// Same worker timeline, but the master-local wall is 59ms longer
		// (result sent → attempt completed recorded by the master clock).
		got := decodeAttemptWaterfall(realisticAttemptReportJSON, "attempt-328s-gap", 328100)
		if got == nil {
			t.Fatal("decode returned nil")
		}
		assertAccountsWall(t, *got, 328100)
		if got.CoveragePct <= 98 {
			t.Fatalf("coverage_pct = %f, want > 98 for a nearly-complete attempt", got.CoveragePct)
		}
		if got.UnaccountedMS != 59 {
			t.Fatalf("unaccounted_ms = %d, want the 59ms master/worker boundary gap", got.UnaccountedMS)
		}
	})
}
