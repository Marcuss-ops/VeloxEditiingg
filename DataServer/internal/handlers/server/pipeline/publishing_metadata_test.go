package pipeline

import (
	"reflect"
	"testing"
)

func TestNormalizeExternalJobSubmissionSeparatesPublicationSpecsFromWorkerPayload(t *testing.T) {
	retryBudget := 3
	madeForKids := true
	req := SubmitJobRequest{
		IdempotencyKey: "pipeline-publication-separation-001",
		VideoName:      "Renderer job name",
		Scenes: []SubmitScene{{
			SceneID:         "scene-0",
			Text:            "Scene text",
			DurationSeconds: 7.2,
		}},
		Publications: []SubmitPublication{{
			PublicationID: "publication-en",
			OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
			Language:      "en",
			Metadata: SubmitPublicationMetadata{
				Title:       "Published title",
				Description: "Published description",
				Tags:        []string{"wwe", "wrestling"},
				Privacy:     "private",
				PublishAt:   "2026-07-20T18:00:00Z",
				MadeForKids: &madeForKids,
			},
			Destinations: []SubmitPublicationDestination{{
				DestinationID: "youtube-en",
				RetryBudget:   &retryBudget,
			}},
		}},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}
	if len(canonical.PublicationSpecs) != 1 {
		t.Fatalf("PublicationSpecs length = %d, want 1", len(canonical.PublicationSpecs))
	}

	spec := canonical.PublicationSpecs[0]
	if spec.Metadata.Title != "Published title" || spec.Metadata.Description != "Published description" {
		t.Fatalf("publication metadata was not retained in PublicationSpecs: %#v", spec.Metadata)
	}
	if !reflect.DeepEqual(spec.Metadata.Tags, []string{"wwe", "wrestling"}) {
		t.Fatalf("publication tags = %#v", spec.Metadata.Tags)
	}
	if spec.Metadata.Privacy != "private" || spec.Metadata.PublishAt != "2026-07-20T18:00:00Z" {
		t.Fatalf("publication privacy/schedule = privacy=%q publish_at=%q", spec.Metadata.Privacy, spec.Metadata.PublishAt)
	}

	workerPayload := canonical.WorkerPayload
	for _, key := range []string{"publications", "publication_specs", "video_metadata", "metadata", "privacy", "privacy_status", "publish_at", "schedule", "scheduling"} {
		if _, present := workerPayload[key]; present {
			t.Fatalf("renderer WorkerPayload contains publication field %q: %#v", key, workerPayload[key])
		}
	}

	plan, ok := workerPayload["delivery_plan"].([]interface{})
	if !ok || len(plan) != 0 {
		if ok {
			t.Fatalf("worker delivery_plan should not be projected for publications-only routing, got %#v", plan)
		}
	}
}

func TestSubmitRequestToRawPayloadStripsLegacyDeliveryMetadata(t *testing.T) {
	retryBudget := 3
	publishingMetadata := map[string]interface{}{
		"title":       "Video title",
		"description": "Video description",
		"tags":        []string{"wwe", "wrestling"},
		"privacy":     "private",
		"publish_at":  "2026-07-20T18:00:00Z",
	}

	req := &SubmitJobRequest{
		IdempotencyKey: "pipeline-legacy-routing-001",
		Scenes: []SubmitScene{{
			SceneID:         "scene-0",
			Text:            "Scene text",
			DurationSeconds: 7.2,
		}},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "youtube-en",
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
	if entry["destination_id"] != "youtube-en" || entry["retry_budget"] != 3 {
		t.Fatalf("legacy routing fields changed: %#v", entry)
	}
	if _, present := entry["metadata"]; present {
		t.Fatalf("legacy delivery metadata leaked into renderer raw payload: %#v", entry["metadata"])
	}
}
