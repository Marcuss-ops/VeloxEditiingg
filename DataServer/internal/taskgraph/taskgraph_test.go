package taskgraph

import "testing"

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to Status
		want     bool
	}{
		{StatusPending, StatusReady, true},
		{StatusPending, StatusLeased, true},
		{StatusPending, StatusRunning, true},
		{StatusPending, StatusFailed, true},
		{StatusPending, StatusCancelled, true},
		{StatusPending, StatusSucceeded, false},

		{StatusReady, StatusLeased, true},
		{StatusReady, StatusRunning, true},
		{StatusReady, StatusFailed, true},
		{StatusReady, StatusCancelled, true},
		{StatusReady, StatusSucceeded, false},

		{StatusLeased, StatusRunning, true},
		{StatusLeased, StatusFailed, true},
		{StatusLeased, StatusCancelled, true},
		{StatusLeased, StatusSucceeded, false},
		{StatusLeased, StatusPending, false},

		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusFailed, true},
		{StatusRunning, StatusCancelled, true},
		{StatusRunning, StatusPending, false},
		{StatusRunning, StatusLeased, false},

		{StatusSucceeded, StatusFailed, false},
		{StatusSucceeded, StatusPending, false},
		{StatusFailed, StatusSucceeded, false},
		{StatusFailed, StatusPending, false},
		{StatusCancelled, StatusPending, false},

		// Idempotent
		{StatusPending, StatusPending, true},
		{StatusRunning, StatusRunning, true},
		{StatusSucceeded, StatusSucceeded, true},

		// Empty
		{"", StatusPending, true},
		{StatusPending, "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		got := CanTransition(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("CanTransition(%q, %q) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestStatusIsTerminal(t *testing.T) {
	if StatusSucceeded.IsTerminal() != true {
		t.Error("Succeeded should be terminal")
	}
	if StatusFailed.IsTerminal() != true {
		t.Error("Failed should be terminal")
	}
	if StatusCancelled.IsTerminal() != true {
		t.Error("Cancelled should be terminal")
	}
	if StatusPending.IsTerminal() != false {
		t.Error("Pending should not be terminal")
	}
	if StatusRunning.IsTerminal() != false {
		t.Error("Running should not be terminal")
	}
	// PR-04 / fix/task-expiry-atomic-transition introduced
	// StatusTimedOut as a Task terminal state (the reaper's atomic
	// transition). The ingestion roll-up (Job→AWAITING_ARTIFACT only
	// fires when ALL tasks are terminal) relies on this.
	if StatusTimedOut.IsTerminal() != true {
		t.Errorf("StatusTimedOut.IsTerminal()=%v; want true (PR-04 Task-terminal after reaper atomic)", StatusTimedOut.IsTerminal())
	}
	// AllStatuses must include the new terminal so DB CHECK constraints
	// and validation paths accept it on insert.
	foundTimedOut := false
	for _, st := range AllStatuses() {
		if st == StatusTimedOut {
			foundTimedOut = true
			break
		}
	}
	if !foundTimedOut {
		t.Errorf("AllStatuses() missing StatusTimedOut (PR-04)")
	}
}
