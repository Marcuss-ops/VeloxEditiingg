package forwardingcontract

import "testing"

func TestCreatorForwardingStatusIsTerminal(t *testing.T) {
	terminal := []CreatorForwardingStatus{
		CFStatusForwarded,
		CFStatusFailed,
		CFStatusCancelled,
		CFStatusBlocked,
	}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%q should be terminal", s)
		}
	}

	nonTerminal := []CreatorForwardingStatus{
		CFStatusPending,
		CFStatusPolling,
		CFStatusReadyToForward,
		CFStatusForwarding,
		CFStatusRetryWait,
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestCreatorForwardingStatusWireSpelling(t *testing.T) {
	cases := map[CreatorForwardingStatus]string{
		CFStatusPending:        "PENDING",
		CFStatusPolling:        "POLLING",
		CFStatusReadyToForward: "READY_TO_FORWARD",
		CFStatusForwarding:     "FORWARDING",
		CFStatusRetryWait:      "RETRY_WAIT",
		CFStatusForwarded:      "FORWARDED",
		CFStatusFailed:         "FAILED",
		CFStatusCancelled:      "CANCELLED",
		CFStatusBlocked:        "BLOCKED",
	}
	for status, want := range cases {
		if string(status) != want {
			t.Errorf("status %q wire spelling = %q, want %q", status, string(status), want)
		}
	}
}
