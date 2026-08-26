package telemetry

import (
	"testing"
	"time"

	sharedtelemetry "velox-shared/telemetry"
)

// TestAttemptMilestoneRecorder_MarkIsIdempotent verifies that marking the
// same milestone more than once keeps the FIRST recorded sample: elapsed_ms
// and sequence must not move on a duplicate Mark. This is the durability
// guard that makes the canonical waterfall reproducible.
func TestAttemptMilestoneRecorder_MarkIsIdempotent(t *testing.T) {
	start := time.Now().Add(-10 * time.Second)
	r := NewAttemptMilestoneRecorderAt(start)

	r.Mark(sharedtelemetry.MilestoneExecutionStarted)
	first, ok := r.ElapsedMS(sharedtelemetry.MilestoneExecutionStarted)
	if !ok {
		t.Fatal("expected execution.started to be recorded")
	}
	firstSeq := r.Snapshot()[0].Sequence

	// Mark the same milestone again after a delay; elapsed must stay put.
	time.Sleep(5 * time.Millisecond)
	r.Mark(sharedtelemetry.MilestoneExecutionStarted)

	second, ok := r.ElapsedMS(sharedtelemetry.MilestoneExecutionStarted)
	if !ok {
		t.Fatal("expected execution.started to still be recorded")
	}
	if second != first {
		t.Fatalf("duplicate Mark moved elapsed_ms: first=%d second=%d", first, second)
	}
	snap := r.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("duplicate Mark appended a sample: count=%d, want 1", len(snap))
	}
	if snap[0].Sequence != firstSeq {
		t.Fatalf("duplicate Mark changed sequence: %d != %d", snap[0].Sequence, firstSeq)
	}
}

// TestAttemptMilestoneRecorder_ElapsedMSMonotonic verifies that samples grow,
// never shrink, and are measured relative to the recorder's start time — not
// wall-clock UTC deltas that workers/master can't safely subtract.
func TestAttemptMilestoneRecorder_ElapsedMSMonotonic(t *testing.T) {
	start := time.Now().Add(-30 * time.Second)
	r := NewAttemptMilestoneRecorderAt(start)

	r.Mark(sharedtelemetry.MilestoneAttemptAccepted)
	time.Sleep(2 * time.Millisecond)
	r.Mark(sharedtelemetry.MilestoneAssetsRequested)
	time.Sleep(2 * time.Millisecond)
	r.Mark(sharedtelemetry.MilestoneAllAssetsReady)

	snap := r.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("sample count=%d, want 3", len(snap))
	}
	var prev int64 = -1
	for i, s := range snap {
		if s.ElapsedMS < 0 {
			t.Fatalf("sample[%d] %s has negative elapsed_ms=%d", i, s.Name, s.ElapsedMS)
		}
		if s.Sequence != uint64(i+1) {
			t.Fatalf("sample[%d] sequence=%d, want %d", i, s.Sequence, i+1)
		}
		if s.ElapsedMS < prev {
			t.Fatalf("sample[%d] %s elapsed_ms=%d decreased from %d", i, s.Name, s.ElapsedMS, prev)
		}
		prev = s.ElapsedMS
	}
}

// TestAttemptMilestoneRecorder_RejectsUnknownMilestone verifies that
// non-canonical milestone names are silently ignored, keeping the closed
// vocabulary tight and the waterfall builder deterministic.
func TestAttemptMilestoneRecorder_RejectsUnknownMilestone(t *testing.T) {
	r := NewAttemptMilestoneRecorder()
	r.Mark(sharedtelemetry.AttemptMilestone("hypothetical.bogus"))
	r.Mark(sharedtelemetry.AttemptMilestone(""))

	if len(r.Snapshot()) != 0 {
		t.Fatalf("unknown milestone leaked into samples: %d", len(r.Snapshot()))
	}
	if r.Has(sharedtelemetry.AttemptMilestone("hypothetical.bogus")) {
		t.Fatal("Has reported an unknown milestone as recorded")
	}
}

// TestAttemptMilestoneRecorder_SnapshotIsCopy verifies Snapshot returns a
// detached slice: mutating it must not corrupt the recorder's own samples.
func TestAttemptMilestoneRecorder_SnapshotIsCopy(t *testing.T) {
	r := NewAttemptMilestoneRecorderAt(time.Now().Add(-5 * time.Second))
	r.Mark(sharedtelemetry.MilestonePlanStarted)
	r.Mark(sharedtelemetry.MilestonePlanCompleted)

	a := r.Snapshot()
	a[0].ElapsedMS = 999999
	a[0].Name = sharedtelemetry.AttemptMilestone("corrupted")

	b := r.Snapshot()
	if b[0].ElapsedMS > 100000 {
		t.Fatalf("Snapshot shares backing array: elapsed_ms corrupted to %d", b[0].ElapsedMS)
	}
	if b[0].Name != sharedtelemetry.MilestonePlanStarted {
		t.Fatalf("Snapshot shares backing array: name corrupted to %q", b[0].Name)
	}
}

// TestAttemptMilestoneRecorder_HasAndElapsed verifies lookup APIs return the
// correct presence + recorded elapsed for marked milestones and the zero value
// for unmarked ones.
func TestAttemptMilestoneRecorder_HasAndElapsed(t *testing.T) {
	start := time.Now().Add(-8 * time.Second)
	r := NewAttemptMilestoneRecorderAt(start)
	r.Mark(sharedtelemetry.MilestoneRenderStarted)

	if !r.Has(sharedtelemetry.MilestoneRenderStarted) {
		t.Fatal("Has(render.started)=false after Mark")
	}
	if r.Has(sharedtelemetry.MilestoneRenderCompleted) {
		t.Fatal("Has(render.completed)=true before Mark")
	}
	elapsed, ok := r.ElapsedMS(sharedtelemetry.MilestoneRenderStarted)
	if !ok {
		t.Fatal("ElapsedMS(render.started) missing after Mark")
	}
	// Recorder was seeded 8s ago; render.started is the first (only) sample, so
	// its elapsed must be near 8000ms and definitely gap-free positive.
	if elapsed < 7990 || elapsed > 9999 {
		t.Fatalf("ElapsedMS(render.started)=%d, want ~8000", elapsed)
	}
	if _, ok := r.ElapsedMS(sharedtelemetry.MilestoneResultSent); ok {
		t.Fatal("ElapsedMS(result.sent) reported ok for an unmarked milestone")
	}
}

// TestAttemptMilestoneRecorder_NilSafety verifies the recorder APIs tolerate a
// nil receiver so callers on paths where the recorder was never seeded do not
// panic.
func TestAttemptMilestoneRecorder_NilSafety(t *testing.T) {
	var r *AttemptMilestoneRecorder
	r.Mark(sharedtelemetry.MilestoneExecutionStarted) // must not panic
	if got := r.Snapshot(); got != nil {
		t.Fatalf("nil receiver Snapshot=%v, want nil", got)
	}
	if r.Has(sharedtelemetry.MilestoneExecutionStarted) {
		t.Fatal("nil receiver Has=true")
	}
	if _, ok := r.ElapsedMS(sharedtelemetry.MilestoneExecutionStarted); ok {
		t.Fatal("nil receiver ElapsedMS ok=true")
	}
}

// TestCanonicalAttemptMilestones_Acceptance verifies the closed vocabulary is
// fully represented by the identity/validation helpers in the shared catalog.
func TestCanonicalAttemptMilestones_Acceptance(t *testing.T) {
	for _, name := range sharedtelemetry.CanonicalAttemptMilestones() {
		if !sharedtelemetry.IsCanonicalAttemptMilestone(name) {
			t.Fatalf("CanonicalAttemptMilestones() returned non-canonical %q", name)
		}
	}
	if sharedtelemetry.IsCanonicalAttemptMilestone(sharedtelemetry.AttemptMilestone("nope")) {
		t.Fatal("IsCanonicalAttemptMilestone accepted unknown milestone")
	}
}
