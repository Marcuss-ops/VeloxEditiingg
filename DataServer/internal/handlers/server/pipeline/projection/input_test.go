package projection

import "testing"

func TestBuildRawPayload_PreservesNestedRenderBoundary(t *testing.T) {
	raw := BuildRawPayload(SubmissionInput{
		JobID:     "boundary-job",
		VideoName: "Boundary video",
		Scenes: []SceneInput{{
			Text:            "Scene",
			DurationSeconds: 2,
			Clip:            &ClipInput{URL: "velox-asset://clip.mp4"},
			Voiceover:       &VoiceoverInput{URL: "velox-asset://voice.mp3", Language: "it"},
		}},
		Layers: []LayerInput{{ID: "title", Type: "text", Text: "Title", Position: []float64{0.5, 0.5}}},
	})

	if raw["job_id"] != "boundary-job" {
		t.Fatalf("job_id = %#v", raw["job_id"])
	}
	scenes, ok := raw["scenes"].([]interface{})
	if !ok || len(scenes) != 1 {
		t.Fatalf("scenes = %#v", raw["scenes"])
	}
	scene := scenes[0].(map[string]interface{})
	if scene["clip"].(map[string]interface{})["url"] != "velox-asset://clip.mp4" {
		t.Fatalf("clip projection = %#v", scene["clip"])
	}
	if scene["voiceover"].(map[string]interface{})["language"] != "it" {
		t.Fatalf("voiceover projection = %#v", scene["voiceover"])
	}
	layers, ok := raw["layers"].([]interface{})
	if !ok || layers[0].(map[string]interface{})["id"] != "title" {
		t.Fatalf("layers = %#v", raw["layers"])
	}
}

func TestBuildRawPayload_DefaultsDeliveryRetryBudget(t *testing.T) {
	raw := BuildRawPayload(SubmissionInput{
		JobID: "delivery-boundary-job",
		DeliveryPlan: []RawDeliveryPlanEntry{{
			DestinationID: "drive",
		}},
	})
	plan := raw["delivery_plan"].([]interface{})
	entry := plan[0].(map[string]interface{})
	if entry["retry_budget"] != defaultRetryBudgetValue {
		t.Fatalf("retry_budget = %#v, want %d", entry["retry_budget"], defaultRetryBudgetValue)
	}
}
