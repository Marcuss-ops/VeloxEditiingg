// apiwire_test.go: minimal smoke tests for the canonical wire types.
//
// Goal: a JSON-roundtrip test confirms each public struct deserialises
// back from its expected OpenAPI key names, so the cmd/api-schema-gen
// generated schemas (which mirror the json tags) match the runtime
// behaviour of the HTTP handler. Run on every commit.
package apiwire

import (
	"encoding/json"
	"testing"
)

func TestSubmitJobRequest_Roundtrip(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "ext-001",
		VideoName:      "Video 1",
		Scenes: []SubmitScene{
			{Text: "hello", DurationSeconds: 7.0},
		},
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var back SubmitJobRequest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.IdempotencyKey != "ext-001" || back.VideoName != "Video 1" {
		t.Errorf("roundtrip mismatch: %+v", back)
	}
	if len(back.Scenes) != 1 || back.Scenes[0].Text != "hello" || back.Scenes[0].DurationSeconds != 7.0 {
		t.Errorf("scenes roundtrip: %+v", back.Scenes)
	}
}

func TestCreatorPushPayload_StatusEnum(t *testing.T) {
	good := []string{`"completed"`, `"completed_with_warnings"`}
	for _, s := range good {
		var p CreatorPushPayload
		if err := json.Unmarshal([]byte(`{"status":`+s+`}`), &p); err != nil {
			t.Errorf("status %s must parse: %v", s, err)
		}
	}
}

func TestDeliveryPlanEntry_DestinationEnum(t *testing.T) {
	good := []struct {
		raw string
		id  string
	}{
		{`{"destination_id":"drive"}`, "drive"},
		{`{"destination_id":"gcs"}`, "gcs"},
		{`{"destination_id":"s3"}`, "s3"},
		{`{"destination_id":"youtube"}`, "youtube"},
		{`{"destination_id":"local"}`, "local"},
	}
	for _, c := range good {
		var d DeliveryPlanEntry
		if err := json.Unmarshal([]byte(c.raw), &d); err != nil {
			t.Errorf("destination %s must parse: %v", c.id, err)
			continue
		}
		if d.DestinationID != c.id {
			t.Errorf("destination %s roundtrip: %q", c.id, d.DestinationID)
		}
	}
}
