package contract

import (
	"encoding/json"
	"testing"
)

func TestNewJobPayloadV2CheckedRejectsLifecycleStatuses(t *testing.T) {
	for _, rawStatus := range []string{"SUCCEEDED", "PUBLISHED", "COMPLETED"} {
		if _, err := NewJobPayloadV2Checked(map[string]any{"status": rawStatus}); err == nil {
			t.Fatalf("checked canonical writer accepted lifecycle status %q", rawStatus)
		}
	}
}

func TestJobPayloadV2DirectJSONReadsLegacyOverloadedStatus(t *testing.T) {
	var payload JobPayloadV2
	if err := json.Unmarshal([]byte(`{"job_id":"legacy-direct","status":"SUCCEEDED"}`), &payload); err != nil {
		t.Fatalf("direct legacy JSON should remain readable: %v", err)
	}
	if payload.Status != InputAssemblyStatus("SUCCEEDED") {
		t.Fatalf("direct legacy status = %q, want preserved raw value", payload.Status)
	}

}

func TestJobPayloadV2FromJSONReadsLegacyOverloadedStatus(t *testing.T) {
	payload, err := JobPayloadV2FromJSON([]byte(`{"job_id":"legacy-1","status":"SUCCEEDED"}`))
	if err != nil {
		t.Fatalf("legacy payload should remain readable: %v", err)
	}
	if payload.Status != InputAssemblyStatus("SUCCEEDED") {
		t.Fatalf("legacy status = %q, want preserved raw value", payload.Status)
	}
	if _, err := payload.ToMap(); err == nil {
		t.Fatal("legacy lifecycle status must not be re-emitted by canonical ToMap")
	}
}

func TestInputAssemblyStatusWireCompatibility(t *testing.T) {
	pending, err := InputAssemblyPending.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal pending: %v", err)
	}
	if string(pending) != `"PENDING"` {
		t.Fatalf("pending wire value = %s, want PENDING", pending)
	}
	completed, err := InputAssemblyCompleted.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal completed: %v", err)
	}
	if string(completed) != `"completed"` {
		t.Fatalf("completed wire value = %s, want completed", completed)
	}
}
