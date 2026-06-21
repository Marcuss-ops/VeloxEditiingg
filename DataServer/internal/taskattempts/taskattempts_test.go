package taskattempts

import "testing"

func TestIsCanonicalPhase(t *testing.T) {
	if !IsCanonicalPhase("render") {
		t.Error("render should be canonical")
	}
	if !IsCanonicalPhase("queue") {
		t.Error("queue should be canonical")
	}
	if IsCanonicalPhase("free_form_phase") {
		t.Error("free_form_phase should not be canonical")
	}
	if IsCanonicalPhase("") {
		t.Error("empty string should not be canonical")
	}
}

func TestAttemptStatusIsTerminal(t *testing.T) {
	if !AttemptStatusSucceeded.IsTerminal() {
		t.Error("Succeeded should be terminal")
	}
	if !AttemptStatusFailed.IsTerminal() {
		t.Error("Failed should be terminal")
	}
	if !AttemptStatusCancelled.IsTerminal() {
		t.Error("Cancelled should be terminal")
	}
	if AttemptStatusPending.IsTerminal() {
		t.Error("Pending should not be terminal")
	}
	if AttemptStatusRunning.IsTerminal() {
		t.Error("Running should not be terminal")
	}
}
