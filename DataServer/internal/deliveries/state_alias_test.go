package deliveries

import "testing"

func TestDeliveryAttemptStateTerminalSemantics(t *testing.T) {
	for _, state := range []DeliveryAttemptState{DeliverySucceeded, DeliveryFailed, DeliveryBlockedAuth, DeliveryCancelled} {
		if !state.IsTerminal() {
			t.Errorf("%q should be terminal", state)
		}
	}
	for _, state := range []DeliveryAttemptState{DeliveryPending, DeliveryRunning, DeliveryRetryWait} {
		if state.IsTerminal() {
			t.Errorf("%q should not be terminal", state)
		}
	}
}
