package supervisor

// supervisor_readiness_test.go
//
// Diagnostic-surface tests: Len, Names, Classes, States, Missing.
// Pinned for /ready probes + observability. These tests do NOT drive
// the supervisor goroutines — they exercise the diagnostic surfaces
// in isolation so there is no race between state mutation and
// inspection.
import (
	"context"
	"testing"
	"time"
)

// TestSupervisor_Missing_ReportsNeverStartedRunner verifies the
// structural-bug detection path: a registered runner whose state-map
// entry is missing surfaces in Missing() before Run() starts it.
// This catches wiring bugs at boot rather than at request time.
func TestSupervisor_Missing_ReportsNeverStartedRunner(t *testing.T) {
	sup := New()
	if err := sup.Register(Runner{
		Name: "never-started", Class: ClassRestartable,
		Policy: RestartPolicy{MaxRetries: 0, InitialBackoff: 1 * time.Millisecond, MaxBackoff: 1 * time.Millisecond},
		Run:    func(ctx context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	missing := sup.Missing()
	found := false
	for _, m := range missing {
		if m == "never-started" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected never-started in Missing(), got: %v", missing)
	}
}

// TestSupervisor_NamesClassesStatesConformance verifies that the
// diagnostic surfaces preserve the registration order and stay in
// sync across Len / Names / Classes / States.
func TestSupervisor_NamesClassesStatesConformance(t *testing.T) {
	sup := New()
	runners := []Runner{
		{Name: "alpha", Class: ClassCritical, Run: func(ctx context.Context) error { return nil }},
		{Name: "bravo", Class: ClassRestartable, Run: func(ctx context.Context) error { return nil }},
		{Name: "charlie", Class: ClassOneShot, Run: func(ctx context.Context) error { return nil }},
	}
	for _, r := range runners {
		if err := sup.Register(r); err != nil {
			t.Fatalf("register %s: %v", r.Name, err)
		}
	}
	if sup.Len() != len(runners) {
		t.Errorf("Len=%d, want %d", sup.Len(), len(runners))
	}
	names := sup.Names()
	for i, r := range runners {
		if names[i] != r.Name {
			t.Errorf("Names[%d]=%q, want %q", i, names[i], r.Name)
		}
	}
	classes := sup.Classes()
	for i, r := range runners {
		if classes[i] != r.Class {
			t.Errorf("Classes[%d]=%v, want %v", i, classes[i], r.Class)
		}
	}
	states := sup.States()
	if len(states) != len(runners) {
		t.Errorf("States size=%d, want %d", len(states), len(runners))
	}
	for _, r := range runners {
		if _, ok := states[r.Name]; !ok {
			t.Errorf("States missing %q", r.Name)
		}
	}
}
