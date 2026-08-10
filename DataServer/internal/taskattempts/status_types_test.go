package taskattempts

import (
	"encoding/json"
	"testing"
)

func TestAttemptStatusValidity(t *testing.T) {
	for _, status := range []AttemptStatus{AttemptStatusPending, AttemptStatusRunning, AttemptStatusSucceeded, AttemptStatusFailed, AttemptStatusCancelled, AttemptStatusTimedOut} {
		if !status.Valid() {
			t.Fatalf("status %q should be valid", status)
		}
	}
	if AttemptStatus("COMPLETED").Valid() {
		t.Fatal("COMPLETED is an input-assembly or aggregate alias, not an AttemptStatus")
	}
}

func TestAttemptStatusKeepsWireSpelling(t *testing.T) {
	data, err := json.Marshal(struct {
		Status AttemptStatus `json:"status"`
	}{Status: AttemptStatusTimedOut})
	if err != nil {
		t.Fatalf("marshal attempt status: %v", err)
	}
	if string(data) != `{"status":"TIMED_OUT"}` {
		t.Fatalf("wire attempt status = %s", data)
	}
}
