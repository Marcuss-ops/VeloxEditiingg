package telemetry

import "testing"

// TestCanonicalAttemptEventNames pins the closed 9-name attempt lifecycle
// vocabulary. Worker and master both derive their event_name literals from
// these constants; a change here is a wire-contract change and must be
// coordinated across both modules.
func TestCanonicalAttemptEventNames(t *testing.T) {
	want := []string{
		"ATTEMPT_STARTED", "PHASE_CHANGED", "SEGMENT_STARTED",
		"SEGMENT_COMPLETED", "PROGRESS_UPDATED", "ARTIFACT_VERIFY_STARTED",
		"ARTIFACT_VERIFIED", "DELIVERY_STARTED", "ATTEMPT_COMPLETED",
	}
	got := CanonicalAttemptEventNames()
	if len(got) != len(want) {
		t.Fatalf("CanonicalAttemptEventNames() length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("CanonicalAttemptEventNames()[%d] = %q, want %q", i, got[i], want[i])
		}
		if !IsCanonicalAttemptEvent(want[i]) {
			t.Errorf("IsCanonicalAttemptEvent(%q) = false, want true", want[i])
		}
	}
}

// TestIsCanonicalAttemptEventRejectsUnknown pins the fail-closed membership:
// empty and unknown names are never canonical.
func TestIsCanonicalAttemptEventRejectsUnknown(t *testing.T) {
	for _, name := range []string{"", "ATTEMPT_FAILED", "DELIVERY_COMPLETED", "started"} {
		if IsCanonicalAttemptEvent(name) {
			t.Errorf("IsCanonicalAttemptEvent(%q) = true, want false", name)
		}
	}
}

// TestCanonicalAttemptEventNamesDefensive pins the read-only contract:
// mutating the returned slice must not affect later calls.
func TestCanonicalAttemptEventNamesDefensive(t *testing.T) {
	got := CanonicalAttemptEventNames()
	got[0] = "MUTATED"
	again := CanonicalAttemptEventNames()
	if again[0] == "MUTATED" {
		t.Fatal("CanonicalAttemptEventNames() returned a shared slice")
	}
}
