package pipeline

import "testing"

// TestNormalizeExternalJobSubmissionKeepsControlPlaneOutOfWorkerPayload locks
// the intake boundary: delivery routing and publication intent remain on the
// canonical control-plane envelope, while the renderer receives only render
// inputs and technical metadata.
func TestNormalizeExternalJobSubmissionKeepsControlPlaneOutOfWorkerPayload(t *testing.T) {
	retryBudget := 3
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	req := SubmitJobRequest{
		IdempotencyKey:           "renderer-boundary-control-plane-001",
		VideoName:                "Technical renderer name",
		ScriptText:               "Render-only script.",
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "youtube-en",
			Priority:      1,
			RetryBudget:   &retryBudget,
			Metadata: map[string]interface{}{
				"title":       "Delivery title must not leak",
				"description": "Delivery description must not leak",
				"privacy":     "private",
			},
		}},
		Publications: []SubmitPublication{{
			PublicationID: "publication-1",
			OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
			Metadata: SubmitPublicationMetadata{
				Title:       "Publication title must not leak",
				Description: "Publication description must not leak",
				Tags:        []string{"editorial"},
				Privacy:     "private",
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

	if len(canonical.DeliveryPlan) == 0 {
		t.Fatal("delivery plan was lost from the control-plane envelope")
	}
	if canonical.DeliveryPlan["delivery_plan"] == nil {
		t.Fatalf("control-plane delivery plan is empty: %#v", canonical.DeliveryPlan)
	}
	if len(canonical.PublicationSpecs) != 1 {
		t.Fatalf("publication specs = %d, want 1", len(canonical.PublicationSpecs))
	}
	if canonical.PublicationSpecs[0].Destinations[0].DestinationID != "youtube-en" {
		t.Fatalf("control-plane destination was lost: %#v", canonical.PublicationSpecs[0].Destinations)
	}

	worker := canonical.WorkerPayload
	for _, key := range []string{
		"delivery_plan",
		"destination_id",
		"destination_ids",
		"publications",
		"publication_specs",
		"metadata",
		"metadata_override",
		"title",
		"description",
		"tags",
		"privacy",
		"privacy_status",
		"publish_at",
		"schedule",
		"scheduling",
		"localizations",
	} {
		if _, present := worker[key]; present {
			t.Fatalf("control-plane field %q leaked into worker payload: %#v", key, worker[key])
		}
	}
	if worker["video_name"] != "Technical renderer name" {
		t.Fatalf("technical renderer name was lost: %#v", worker["video_name"])
	}
	if worker["compiled_render_plan_json"] != planJSON || worker["compiled_render_plan_sha256"] != planSHA {
		t.Fatal("producer-compiled plan was not passed through to worker payload")
	}
	if _, present := worker["scenes_json"]; present {
		t.Fatal("retired scene projection crossed the worker boundary")
	}
}

func TestNormalizeExternalJobSubmissionRenderOnlyDropsDeliveryPlan(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	req := SubmitJobRequest{
		IdempotencyKey:           "renderer-boundary-render-only-001",
		VideoName:                "Render only",
		ScriptText:               "No delivery route.",
		RenderOnly:               true,
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
		DeliveryPlan:             []SubmitDeliveryPlanEntry{{}},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil {
		t.Fatal("NormalizeExternalJobSubmission returned nil")
	}
	if got := canonical.DeliveryPlan; len(got) != 1 || got["render_only"] != true {
		t.Fatalf("render-only control-plane envelope = %#v, want only render_only=true", got)
	}
	if _, present := canonical.DeliveryPlan["delivery_plan"]; present {
		t.Fatalf("render-only control-plane envelope retained delivery_plan: %#v", canonical.DeliveryPlan)
	}
	if canonical.WorkerPayload["render_only"] != true {
		t.Fatalf("render_only did not reach renderer payload: %#v", canonical.WorkerPayload)
	}
}
