package jobs

import (
	"encoding/json"
	"testing"
)

func TestJobStatusIsDistinctDomainTypeAndWireCompatible(t *testing.T) {
	if !JobStatus(StatusSucceeded).Valid() || !JobStatus(StatusSucceeded).IsTerminal() {
		t.Fatal("SUCCEEDED must be a valid terminal JobStatus")
	}
	if JobStatus("COMPLETED").Valid() {
		t.Fatal("COMPLETED is not a canonical JobStatus")
	}
	data, err := json.Marshal(&Job{ID: "job-1", Status: StatusPending})
	if err != nil {
		t.Fatalf("marshal job: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if raw["status"] != "PENDING" {
		t.Fatalf("wire status = %v, want PENDING", raw["status"])
	}
}

func TestJobStatusKeepsWireSpelling(t *testing.T) {
	data, err := json.Marshal(struct {
		Status JobStatus `json:"status"`
	}{Status: StatusAwaitingArtifact})
	if err != nil {
		t.Fatalf("marshal job status: %v", err)
	}
	if string(data) != `{"status":"AWAITING_ARTIFACT"}` {
		t.Fatalf("wire job status = %s", data)
	}
}
