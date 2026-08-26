package pipeline

import (
	"testing"
	"time"
)

func TestAllPhases_ReturnsOrderedNonEmptyList(t *testing.T) {
	phases := AllPhases()
	if len(phases) == 0 {
		t.Fatal("AllPhases() returned empty list")
	}
	seen := make(map[string]bool)
	for _, p := range phases {
		if p == "" {
			t.Error("AllPhases() contains empty string")
		}
		if seen[p] {
			t.Errorf("AllPhases() contains duplicate %q", p)
		}
		seen[p] = true
	}
	// Verify key phases exist in order.
	expected := []string{
		PhasePrefetching, PhaseRendering, PhasePublishing, PhaseDone,
	}
	pi := 0
	for _, p := range phases {
		if pi < len(expected) && p == expected[pi] {
			pi++
		}
	}
	if pi != len(expected) {
		t.Errorf("expected phases %v not found in order, got %v", expected, phases)
	}
}

func TestIsValidPhase(t *testing.T) {
	tests := []struct {
		phase string
		valid bool
	}{
		{PhasePrefetching, true},
		{PhaseRendering, true},
		{PhaseDone, true},
		{"", false},
		{"unknown_phase", false},
		{"RENDERING", false}, // case-sensitive
	}
	for _, tt := range tests {
		if got := IsValidPhase(tt.phase); got != tt.valid {
			t.Errorf("IsValidPhase(%q) = %v, want %v", tt.phase, got, tt.valid)
		}
	}
}

func TestUpdateTaskProgress_CallsCallbackWithValidPhase(t *testing.T) {
	var gotPhase string
	var gotTime time.Time
	called := false

	UpdateTaskProgress("task-1", PhasePrefetching, func(phase string, now time.Time) {
		called = true
		gotPhase = phase
		gotTime = now
	})

	if !called {
		t.Fatal("callback was not called")
	}
	if gotPhase != PhasePrefetching {
		t.Errorf("phase = %q, want %q", gotPhase, PhasePrefetching)
	}
	if gotTime.IsZero() {
		t.Error("timestamp is zero")
	}
}

func TestUpdateTaskProgress_SkipsInvalidPhase(t *testing.T) {
	called := false
	UpdateTaskProgress("task-1", "bogus_phase", func(phase string, now time.Time) {
		called = true
	})
	if called {
		t.Error("callback was called for invalid phase")
	}
}

func TestUpdateTaskProgress_SkipsEmptyTaskID(t *testing.T) {
	called := false
	UpdateTaskProgress("", PhaseRendering, func(phase string, now time.Time) {
		called = true
	})
	if called {
		t.Error("callback was called for empty task ID")
	}
}

func TestUpdateTaskProgress_SkipsNilCallback(t *testing.T) {
	// Should not panic.
	UpdateTaskProgress("task-1", PhaseRendering, nil)
}
