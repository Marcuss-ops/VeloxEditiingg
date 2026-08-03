package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestCreatorPushAndExternalSubmitConvergeToTheSameCanonicalContract verifies
// that the two public intake paths produce equivalent render contracts. The
// producer label is intentionally different (it is part of forwarding
// identity), while source job ID, executor, worker payload, and control-plane
// delivery plan must remain equivalent.
func TestCreatorPushAndExternalSubmitConvergeToTheSameCanonicalContract(t *testing.T) {
	t.Parallel()

	const (
		jobID      = "equivalence-job-001"
		executorID = "scene.composite.v1"
		voiceover  = "velox-asset://voiceover/equivalence.mp3"
	)

	external := SubmitJobRequest{
		IdempotencyKey: jobID,
		VideoName:      "Equivalence video",
		ScriptText:     "The same canonical script.",
		Scenes: []SubmitScene{{
			Text:            "Equivalence scene",
			SceneID:         "scene-0",
			Index:           0,
			Kind:            "clip",
			Clip:            &SubmitClip{URL: "velox-asset://clip/equivalence.mp4"},
			DurationSeconds: 5,
			Voiceover:       &SubmitVoiceover{URL: voiceover, Language: "en"},
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://subtitle/equivalence.srt", Format: "srt"},
		}},
		Layers: []SubmitLayer{{
			ID: "title", Type: "text", Role: "title", Text: "Equivalence",
		}},
		AudioTracks: []SubmitAudioTrack{{
			SourceURL: "velox-asset://audio/equivalence.mp3", Role: "background_music", Volume: 0.2,
		}},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "drive-equivalence", Priority: 1, RetryBudget: intPointer(3),
		}},
		Publications: []SubmitPublication{{
			PublicationID: "publication-equivalence",
			OutputRef:     SubmitPublicationOutputRef{ArtifactRole: "final_video"},
			Language:      "en",
			Metadata: SubmitPublicationMetadata{
				Title:       "Published equivalence title",
				Description: "Publication-only description",
				Tags:        []string{"equivalence", "test"},
				Privacy:     "private",
			},
			Destinations: []SubmitPublicationDestination{{
				DestinationID: "drive-equivalence", RetryBudget: intPointer(3),
			}},
		}},
	}

	externalCanonical := (&Handlers{}).NormalizeExternalJobSubmission(external)
	if externalCanonical == nil {
		t.Fatal("external submission returned nil canonical contract")
	}
	if len(externalCanonical.PublicationSpecs) != 1 {
		t.Fatalf("external publication specs = %d, want 1", len(externalCanonical.PublicationSpecs))
	}
	publicationSpec := externalCanonical.PublicationSpecs[0]
	if publicationSpec.Metadata.Title != "Published equivalence title" || publicationSpec.Metadata.Description != "Publication-only description" {
		t.Fatalf("publication metadata was not retained in control plane: %#v", publicationSpec.Metadata)
	}
	if !reflect.DeepEqual(publicationSpec.Metadata.Tags, []string{"equivalence", "test"}) || publicationSpec.Metadata.Privacy != "private" {
		t.Fatalf("publication metadata mismatch: %#v", publicationSpec.Metadata)
	}
	if len(publicationSpec.Destinations) != 1 || publicationSpec.Destinations[0].DestinationID != "drive-equivalence" {
		t.Fatalf("publication destinations mismatch: %#v", publicationSpec.Destinations)
	}

	// Build the creator payload independently from the external request DTO.
	// This is intentionally not submitRequestToRawPayload(&external): the
	// test must detect drift between the two intake adapters rather than
	// compare one adapter's output with itself.
	creatorPayload := map[string]interface{}{
		"status":      "completed",
		"job_id":      jobID,
		"video_name":  "Equivalence video",
		"script_text": "The same canonical script.",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Equivalence scene",
				"scene_id":         "scene-0",
				"kind":             "clip",
				"clip":             map[string]interface{}{"url": "velox-asset://clip/equivalence.mp4"},
				"duration_seconds": float64(5),
				"voiceover":        map[string]interface{}{"url": voiceover, "language": "en"},
				"subtitles":        map[string]interface{}{"url": "velox-asset://subtitle/equivalence.srt", "format": "srt"},
			},
		},
		"layers": []interface{}{
			map[string]interface{}{"id": "title", "type": "text", "role": "title", "text": "Equivalence"},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{"source_url": "velox-asset://audio/equivalence.mp3", "role": "background_music", "volume": 0.2},
		},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-equivalence", "priority": 1, "retry_budget": 3},
		},
	}
	creatorCanonical, err := normalizeCreatorPushRequest(creatorPushRequest{
		SourceProvider:   "creator",
		SourceJobID:      jobID,
		TargetExecutorID: executorID,
		Payload:          creatorPayload,
	})
	if err != nil {
		t.Fatalf("creator push normalization: %v", err)
	}

	if externalCanonical.SourceJobID != creatorCanonical.SourceJobID {
		t.Fatalf("source job IDs differ: external=%q creator=%q", externalCanonical.SourceJobID, creatorCanonical.SourceJobID)
	}
	if externalCanonical.TargetExecutorID != creatorCanonical.TargetExecutorID {
		t.Fatalf("target executors differ: external=%q creator=%q", externalCanonical.TargetExecutorID, creatorCanonical.TargetExecutorID)
	}
	if externalCanonical.SourceProvider == creatorCanonical.SourceProvider {
		t.Fatalf("producer identity unexpectedly collapsed to %q", externalCanonical.SourceProvider)
	}

	// Creator Push retains its historical top-level compatibility aliases;
	// compare only the shared canonical render contract here. The external
	// /api/v1/jobs projection removes these aliases at its own strict boundary.
	creatorWorker := cloneCanonicalWorkerPayload(creatorCanonical.WorkerPayload)
	wantWorker := canonicalJSONValue(t, externalCanonical.WorkerPayload)
	gotWorker := canonicalJSONValue(t, creatorWorker)
	if !reflect.DeepEqual(wantWorker, gotWorker) {
		t.Fatalf("worker payloads diverged:\nexternal=%s\ncreator=%s", mustCanonicalJSON(t, wantWorker), mustCanonicalJSON(t, gotWorker))
	}

	wantDelivery := canonicalJSONValue(t, externalCanonical.DeliveryPlan)
	gotDelivery := canonicalJSONValue(t, creatorCanonical.DeliveryPlan)
	if !reflect.DeepEqual(wantDelivery, gotDelivery) {
		t.Fatalf("delivery plans diverged:\nexternal=%s\ncreator=%s", mustCanonicalJSON(t, wantDelivery), mustCanonicalJSON(t, gotDelivery))
	}

	if len(creatorCanonical.PublicationSpecs) != 0 {
		t.Fatal("creator push unexpectedly produced publication specs from renderer input")
	}
	for label, payload := range map[string]map[string]interface{}{
		"external": externalCanonical.WorkerPayload,
		"creator":  creatorCanonical.WorkerPayload,
	} {
		for _, key := range []string{"publications", "publication_specs", "video_metadata", "metadata", "title", "description", "tags", "privacy", "privacy_status", "publish_at", "schedule", "scheduling", "localizations", "metadata_override"} {
			if _, leaked := payload[key]; leaked {
				t.Fatalf("%s worker payload contains publication field %q: %#v", label, key, payload[key])
			}
		}
	}
}

func cloneCanonicalWorkerPayload(payload map[string]interface{}) map[string]interface{} {
	clone := make(map[string]interface{}, len(payload))
	for key, value := range payload {
		clone[key] = value
	}
	for _, alias := range []string{"voiceover_paths", "subtitle_tracks", "clip_link", "image_link"} {
		delete(clone, alias)
	}
	return clone
}

func intPointer(value int) *int { return &value }

func canonicalJSONValue(t *testing.T, value interface{}) interface{} {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal canonical value: %v", err)
	}
	var normalized interface{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		t.Fatalf("unmarshal canonical value: %v", err)
	}
	return normalized
}

func mustCanonicalJSON(t *testing.T, value interface{}) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal diagnostic value: %v", err)
	}
	return string(encoded)
}
