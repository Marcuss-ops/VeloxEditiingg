package telemetry

import (
	"testing"
	"time"
)

// TestPhaseTimer_StartEnd_DurationBasic: happy path — start phase A,
// wait, end phase A; PhaseDurations returns non-zero for A.
func TestPhaseTimer_StartEnd_DurationBasic(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	step := 0
	pt := NewPhaseTimerWithClock(func() time.Time {
		t := frozen.Add(time.Duration(step) * 100 * time.Millisecond)
		step++
		return t
	})

	pt.StartPhase(PhaseDecode)
	pt.EndPhase(PhaseDecode)

	d := pt.PhaseDurations()
	if got, want := d[PhaseDecode], 100*time.Millisecond; got != want {
		t.Errorf("PhaseDecode duration = %v, want %v", got, want)
	}
}

// TestPhaseTimer_UnknownIgnored: StartPhase / EndPhase on non-canonical
// names is a noop — does not panic, does not record.
func TestPhaseTimer_UnknownIgnored(t *testing.T) {
	pt := NewPhaseTimer()
	pt.StartPhase("not_a_canonical_phase")
	pt.EndPhase("not_a_canonical_phase")
	d := pt.PhaseDurations()
	if len(d) != len(CanonicalPhaseOrder) {
		t.Errorf("PhaseDurations should always return %d keys, got %d",
			len(CanonicalPhaseOrder), len(d))
	}
	for k, v := range d {
		if v != 0 {
			t.Errorf("phase %q should be zero on unknown input, got %v", k, v)
		}
	}
}

// TestPhaseTimer_EndWithoutStart_IsZero: ending a phase that was
// never started records zero. No negative durations.
func TestPhaseTimer_EndWithoutStart_IsZero(t *testing.T) {
	pt := NewPhaseTimer()
	pt.EndPhase(PhaseRender)
	d := pt.PhaseDurations()
	if d[PhaseRender] != 0 {
		t.Errorf("PhaseRender duration after orphan EndPhase = %v, want 0", d[PhaseRender])
	}
}

// TestPhaseTimer_FreezeBlocksMutations: after Freeze, StartPhase
// and EndPhase are noops; the previously accumulated durations
// remain readable.
func TestPhaseTimer_FreezeBlocksMutations(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	step := 0
	pt := NewPhaseTimerWithClock(func() time.Time {
		t := frozen.Add(time.Duration(step) * 50 * time.Millisecond)
		step++
		return t
	})

	pt.StartPhase(PhaseDownload)
	pt.EndPhase(PhaseDownload)
	d1 := pt.PhaseDurations()[PhaseDownload]
	if d1 == 0 {
		t.Fatalf("setup: expected non-zero Download duration, got 0")
	}
	pt.Freeze()
	if !pt.Frozen() {
		t.Errorf("Frozen() should return true after Freeze()")
	}
	// Attempt to mutate: should not change anything.
	pt.StartPhase(PhaseDownload)
	pt.EndPhase(PhaseDownload)
	d2 := pt.PhaseDurations()[PhaseDownload]
	if d1 != d2 {
		t.Errorf("post-Freeze mutations changed duration: d1=%v d2=%v", d1, d2)
	}
}

// TestPhaseTimer_Reset: returns the timer to a clean state and
// unfreezes it.
func TestPhaseTimer_Reset(t *testing.T) {
	pt := NewPhaseTimerWithClock(func() time.Time {
		return time.Unix(1_700_000_000, 0)
	})
	pt.StartPhase(PhaseEncode)
	pt.EndPhase(PhaseEncode)
	pt.Freeze()
	if !pt.Frozen() {
		t.Fatalf("setup: expected Frozen")
	}
	pt.Reset()
	if pt.Frozen() {
		t.Errorf("Reset should clear Frozen flag")
	}
	for k, v := range pt.PhaseDurations() {
		if v != 0 {
			t.Errorf("post-Reset phase %q should be zero, got %v", k, v)
		}
	}
}

// TestPhaseTimer_PhaseDurationsReturnsAllCanonical: PhaseDurations
// should always yield a complete 12-key map, including phases that
// were never started (zero-valued). Dashboards depend on a stable
// shape.
func TestPhaseTimer_PhaseDurationsReturnsAllCanonical(t *testing.T) {
	pt := NewPhaseTimer()
	d := pt.PhaseDurations()
	if got, want := len(d), len(CanonicalPhaseOrder); got != want {
		t.Errorf("PhaseDurations returned %d keys, want %d", got, want)
	}
	for _, p := range CanonicalPhaseOrder {
		if _, ok := d[p]; !ok {
			t.Errorf("PhaseDurations missing canonical phase %q", p)
		}
	}
}

// TestPhaseTimer_RestartOnSamePhase: calling StartPhase twice on
// the same canonical name without an EndPhase re-stamps the start
// time. Last-write-wins is acceptable for the single-goroutine
// contract.
func TestPhaseTimer_RestartOnSamePhase(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	step := 0
	pt := NewPhaseTimerWithClock(func() time.Time {
		t := frozen.Add(time.Duration(step) * 100 * time.Millisecond)
		step++
		return t
	})
	pt.StartPhase(PhaseRender) // start=0ms
	pt.StartPhase(PhaseRender) // restart at 100ms
	pt.EndPhase(PhaseRender)   // end at 200ms → 100ms duration
	d := pt.PhaseDurations()
	if got, want := d[PhaseRender], 100*time.Millisecond; got != want {
		t.Errorf("PhaseRender after restart: got %v want %v", got, want)
	}
}

// TestPhaseTimer_RepeatedEndPhase_Accumulates: calling EndPhase
// multiple times accumulates durations (useful for sub-phases that
// start/end within a coarse 5-phase boundary).
func TestPhaseTimer_RepeatedEndPhase_Accumulates(t *testing.T) {
	frozen := time.Unix(1_700_000_000, 0)
	step := 0
	pt := NewPhaseTimerWithClock(func() time.Time {
		t := frozen.Add(time.Duration(step) * 50 * time.Millisecond)
		step++
		return t
	})

	// First sub-phase
	pt.StartPhase(PhaseEncode) // step 0 → 0ms
	pt.EndPhase(PhaseEncode)   // step 1 → 50ms; duration = 50ms

	// Second sub-phase
	pt.StartPhase(PhaseEncode) // step 2 → 100ms
	pt.EndPhase(PhaseEncode)   // step 3 → 150ms; duration += 50ms

	d := pt.PhaseDurations()
	if got, want := d[PhaseEncode], 100*time.Millisecond; got != want {
		t.Errorf("accumulated PhaseEncode duration = %v, want %v", got, want)
	}
}

// TestPhaseTimer_NilReceiverSafe: all methods tolerate a nil receiver
// without panic. A nil PhaseTimer has no instance state, so:
//   - Mutations (StartPhase / EndPhase / Freeze / Reset) are no-ops —
//     they cannot panic and cannot mutate anything.
//   - Reads (PhaseDurations / Frozen) return the documented zero:
//     PhaseDurations returns a 12-key zero map (dashboards need a
//     stable shape); Frozen returns false (there is no instance to be
//     frozen — callers should check != nil before inspecting).
func TestPhaseTimer_NilReceiverSafe(t *testing.T) {
	var pt *PhaseTimer
	pt.StartPhase(PhaseDownload) // no-op, no panic
	pt.EndPhase(PhaseDownload)   // no-op, no panic
	pt.Freeze()                  // no-op (cannot mutate nil instance)
	if pt.Frozen() {
		t.Errorf("nil receiver Frozen() = true, want false (no instance state to be frozen)")
	}
	d := pt.PhaseDurations()
	if len(d) != len(CanonicalPhaseOrder) {
		t.Errorf("nil receiver PhaseDurations shape = %d, want %d",
			len(d), len(CanonicalPhaseOrder))
	}
	for k, v := range d {
		if v != 0 {
			t.Errorf("nil receiver PhaseDurations[%q] = %v, want 0", k, v)
		}
	}
	pt.Reset() // no-op, no panic
}

// ── 5→12 mapping tests ─────────────────────────────────────────────────────

// TestMapLegacyPhase_KnownPhases: every legacy 5-phase name maps to a
// non-empty slice of canonical phases whose union covers the documented
// 12-phase shape.
func TestMapLegacyPhase_KnownPhases(t *testing.T) {
	cases := []struct {
		legacy string
		want   []string
	}{
		{"cache_lookup", []string{PhaseCacheLookup}},
		{"prefetch", []string{PhaseAssetWait, PhaseCacheLookup, PhaseDownload}},
		{"execute", []string{PhaseDecode, PhaseCompile, PhaseSimulate, PhaseRender, PhaseComposite, PhaseEncode}},
		{"upload", []string{PhaseUpload, PhaseFinalize}},
		{"report", []string{PhaseFinalize}},
	}
	for _, c := range cases {
		got := MapLegacyPhase(c.legacy)
		if len(got) != len(c.want) {
			t.Errorf("MapLegacyPhase(%q) = %v, want %v", c.legacy, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("MapLegacyPhase(%q)[%d] = %q, want %q",
					c.legacy, i, got[i], c.want[i])
			}
		}
	}
}

// TestMapLegacyPhase_UnknownReturnsNil: an unknown legacy name returns
// nil. Callers must handle nil cleanly.
func TestMapLegacyPhase_UnknownReturnsNil(t *testing.T) {
	got := MapLegacyPhase("not_a_legacy_phase_either")
	if got != nil {
		t.Errorf("MapLegacyPhase(unknown) = %v, want nil", got)
	}
}

// TestMapLegacyPhase_ReturnsDefensiveCopy: callers may mutate the
// returned slice without affecting later calls.
func TestMapLegacyPhase_ReturnsDefensiveCopy(t *testing.T) {
	a := MapLegacyPhase("prefetch")
	if len(a) == 0 {
		t.Fatalf("setup: expected non-empty mapping")
	}
	a[0] = "MUTATED"
	b := MapLegacyPhase("prefetch")
	if b[0] == "MUTATED" {
		t.Errorf("MapLegacyPhase should return a defensive copy")
	}
}

// TestIsCanonical: every canonical phase returns true; a known
// non-canonical name returns false.
func TestIsCanonical(t *testing.T) {
	for _, p := range CanonicalPhaseOrder {
		if !IsCanonical(p) {
			t.Errorf("IsCanonical(%q) = false, want true", p)
		}
	}
	if IsCanonical("not_a_canonical") {
		t.Errorf("IsCanonical on unknown name = true, want false")
	}
}

// TestCanonicalPhaseOrder_Stable: the canonical phase order is stable.
// Dashboards and audit rollups depend on this. Changing this list
// should require a coordinated migration.
func TestCanonicalPhaseOrder_Stable(t *testing.T) {
	want := []string{
		"queue", "asset_wait", "cache_lookup", "download",
		"decode", "compile", "simulate", "render", "composite",
		"encode", "upload", "finalize",
	}
	if len(CanonicalPhaseOrder) != len(want) {
		t.Fatalf("CanonicalPhaseOrder length = %d, want %d", len(CanonicalPhaseOrder), len(want))
	}
	for i := range CanonicalPhaseOrder {
		if CanonicalPhaseOrder[i] != want[i] {
			t.Errorf("CanonicalPhaseOrder[%d] = %q, want %q (canonical order must be stable)",
				i, CanonicalPhaseOrder[i], want[i])
		}
	}
}
