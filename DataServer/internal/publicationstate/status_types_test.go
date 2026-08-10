package publicationstate

import (
	"encoding/json"
	"testing"
)

func TestPublicationStatusValidity(t *testing.T) {
	for _, status := range []PublicationStatus{Pending, WaitingForRender, ArtifactBound, Ready, Scheduled, Uploading, VideoCreated, MetadataApplying, LocalizationsApplying, Verifying, Published, Partial, RetryWait, Failed, Cancelled} {
		if !status.Valid() {
			t.Fatalf("status %q should be valid", status)
		}
	}
	if PublicationStatus("SUCCEEDED").Valid() {
		t.Fatal("SUCCEEDED is a JobStatus, not a PublicationStatus")
	}
}

func TestPublicationStatusKeepsWireSpelling(t *testing.T) {
	data, err := json.Marshal(struct {
		Status PublicationStatus `json:"status"`
	}{Status: Published})
	if err != nil {
		t.Fatalf("marshal publication status: %v", err)
	}
	if string(data) != `{"status":"PUBLISHED"}` {
		t.Fatalf("wire publication status = %s", data)
	}
}
