package projection

import (
	"reflect"
	"testing"

	"velox-shared/contract"
)

func TestBuildRawPayloadEnvelopePreservesCanonicalWireShape(t *testing.T) {
	zero := 0
	got := BuildRawPayloadEnvelope(RawPayloadInput{
		JobID:            "  job-1  ",
		JobType:          " process_video ",
		TemplateID:       " template-1 ",
		TemplateVersion:  2,
		VideoMode:        " scene ",
		VideoName:        "  Video  ",
		ScriptText:       "verbatim content",
		Spec:             map[string]interface{}{"voiceover_paths": []interface{}{"legacy.mp3"}},
		Output:           map[string]interface{}{"width": 1920},
		RenderManifest:   map[string]interface{}{"schema_version": "v1"},
		ManifestRef:      map[string]interface{}{"url": "https://example.test/manifest"},
		ManifestSHA256:   "sha256",
		PlacementPin:     " worker-1 ",
		LegacyVoiceovers: []string{"legacy.mp3"},
		DeliveryPlan: []RawDeliveryPlanEntry{{
			DestinationID: " drive ",
			Priority:      1,
			RetryBudget:   &zero,
			Metadata:      map[string]interface{}{"folder_id": "folder-1"},
		}},
		RetryBudgetDefault: 3,
	})

	if got["status"] != string(contract.InputAssemblyCompleted) {
		t.Fatalf("status = %v, want %q", got["status"], contract.InputAssemblyCompleted)
	}
	for key, want := range map[string]interface{}{
		"job_id": "job-1", "job_type": "process_video", "template_id": "template-1",
		"video_name": "Video", "script_text": "verbatim content", "video_mode": "scene",
		"_placement_pin_worker_id": "worker-1",
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %v", key, got[key], want)
		}
	}
	if _, ok := got["delivery_plan"]; !ok {
		t.Fatal("delivery_plan missing from raw control-plane payload")
	}
	plan, ok := got["delivery_plan"].([]interface{})
	if !ok || len(plan) != 1 {
		t.Fatalf("delivery_plan = %#v, want one entry", got["delivery_plan"])
	}
	entry := plan[0].(map[string]interface{})
	if entry["destination_id"] != "drive" || entry["retry_budget"] != 0 {
		t.Fatalf("delivery entry = %#v, want trimmed destination and explicit zero retry", entry)
	}
}

func TestBuildRawPayloadEnvelopeUsesDefaultRetryBudgetWhenOmitted(t *testing.T) {
	got := BuildRawPayloadEnvelope(RawPayloadInput{
		JobID: "job-2",
		DeliveryPlan: []RawDeliveryPlanEntry{{
			DestinationID: "drive",
		}},
	})
	plan := got["delivery_plan"].([]interface{})
	entry := plan[0].(map[string]interface{})
	if entry["retry_budget"] != 3 {
		t.Fatalf("retry_budget = %v, want default 3", entry["retry_budget"])
	}
}

func TestBuildRawPayloadEnvelopePreservesControlPlaneEnvelopeFields(t *testing.T) {
	got := BuildRawPayloadEnvelope(RawPayloadInput{
		JobID:          "job-3",
		Output:         map[string]interface{}{"width": 1280},
		RenderManifest: map[string]interface{}{"schema_version": "v1"},
		ManifestRef:    map[string]interface{}{"url": "https://example.test/manifest"},
		ManifestSHA256: "sha256",
	})
	for key, want := range map[string]interface{}{
		"output":          map[string]interface{}{"width": 1280},
		"render_manifest": map[string]interface{}{"schema_version": "v1"},
		"manifest_ref":    map[string]interface{}{"url": "https://example.test/manifest"},
		"manifest_sha256": "sha256",
	} {
		if got[key] == nil {
			t.Errorf("%s missing from envelope", key)
			continue
		}
		if !reflect.DeepEqual(got[key], want) {
			t.Errorf("%s = %#v, want %#v", key, got[key], want)
		}
	}
}

func TestBuildRawPayloadEnvelopeLegacyVoiceoversRemainExplicit(t *testing.T) {
	got := BuildRawPayloadEnvelope(RawPayloadInput{
		JobID:            "job-4",
		LegacyVoiceovers: []string{"legacy-a.mp3", "legacy-b.mp3"},
	})
	tracks, ok := got["audio_tracks"].([]interface{})
	if !ok || len(tracks) != 2 {
		t.Fatalf("audio_tracks = %#v, want two legacy tracks", got["audio_tracks"])
	}
	if tracks[0].(map[string]interface{})["role"] != "voiceover" {
		t.Fatalf("legacy track role = %#v, want voiceover", tracks[0])
	}
}
