package jobs

import "testing"

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from Status
		to   Status
		want bool
	}{
		// Transitions to empty string must be false (P0 fix)
		{StatusPending, "", false},
		{StatusRunning, "", false},
		{StatusLeased, "", false},
		{"", "", false},

		// Idempotent transitions (from == to) are always true (unless to is "")
		{StatusPending, StatusPending, true},
		{StatusRunning, StatusRunning, true},
		{StatusSucceeded, StatusSucceeded, true},
		{StatusFailed, StatusFailed, true},

		// Valid transitions from Pending/""
		{"", StatusLeased, true},
		{StatusPending, StatusLeased, true},
		{StatusPending, StatusRunning, true},
		{StatusPending, StatusRetryWait, true},
		{StatusPending, StatusFailed, true},
		{StatusPending, StatusCancelled, true},

		// Valid transitions from Leased
		{StatusLeased, StatusRunning, true},
		{StatusLeased, StatusFailed, true},
		{StatusLeased, StatusCancelled, true},
		{StatusLeased, StatusPending, false},

		// Valid transitions from Running
		{StatusRunning, StatusSucceeded, true},
		{StatusRunning, StatusFailed, true},
		{StatusRunning, StatusRetryWait, true},
		{StatusRunning, StatusCancelled, true},
		{StatusRunning, StatusPending, false},

		// Valid transitions from RetryWait
		{StatusRetryWait, StatusPending, true},
		{StatusRetryWait, StatusFailed, true},
		{StatusRetryWait, StatusCancelled, true},
		{StatusRetryWait, StatusSucceeded, false},

		// Terminal states cannot transition to other states
		{StatusSucceeded, StatusPending, false},
		{StatusFailed, StatusPending, false},
		{StatusCancelled, StatusPending, false},
	}

	for _, tt := range tests {
		got := CanTransition(tt.from, tt.to)
		if got != tt.want {
			t.Errorf("CanTransition(%q, %q) = %v; want %v", tt.from, tt.to, got, tt.want)
		}
	}
}
