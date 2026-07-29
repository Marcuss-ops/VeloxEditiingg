package pipeline

import (
	"reflect"
	"testing"
)

func TestSubmitRequestToRawPayloadPreservesPublishingMetadata(t *testing.T) {
	retryBudget := 3
	publishingMetadata := map[string]interface{}{
		"contract_version":  "velox.instaedit.publish.v1",
		"title":             "Video title",
		"description":       "Video description",
		"tags":              []string{"wwe", "wrestling"},
		"privacy_status":    "private",
		"final_privacy":     "public",
		"require_thumbnail": true,
	}

	req := &SubmitJobRequest{
		IdempotencyKey: "pipelinegen-job123-a84f927c",
		Scenes: []SubmitScene{{
			SceneID:         "scene-0",
			Text:            "Scene text",
			DurationSeconds: 7.2,
		}},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "instaedit_extdst_01JREADY",
			Priority:      1,
			RetryBudget:   &retryBudget,
			Metadata:      publishingMetadata,
		}},
	}

	raw := submitRequestToRawPayload(req)
	plan, ok := raw["delivery_plan"].([]interface{})
	if !ok || len(plan) != 1 {
		t.Fatalf("delivery_plan shape = %#v", raw["delivery_plan"])
	}
	entry, ok := plan[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery plan entry shape = %#v", plan[0])
	}
	if got := entry["destination_id"]; got != "instaedit_extdst_01JREADY" {
		t.Fatalf("destination_id = %#v", got)
	}
	if !reflect.DeepEqual(entry["metadata"], publishingMetadata) {
		t.Fatalf("publishing metadata changed during normalization:\n got: %#v\nwant: %#v", entry["metadata"], publishingMetadata)
	}
}
