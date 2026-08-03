package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"

	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
)

// TestCanonicalIngressParity locks the worker-owned payload contract across
// every current producer. The inputs are semantically identical, while the
// adapters are intentionally exercised through their real production seams:
// API request projection, creator push normalization, scene-video
// normalization, pipeline-result building, and remote-engine DTO projection.
//
// Lifecycle identity and producer labels are excluded from the projection;
// render inputs are. Delivery routing is asserted separately on the
// control-plane envelope, so it cannot silently cross the renderer boundary.
// This makes a field silently dropped by one ingress a failing contract test
// rather than a production regression discovered only after a worker claim.
func TestCanonicalIngressParity(t *testing.T) {
	retryBudget := 3
	req := SubmitJobRequest{
		IdempotencyKey: "ingress-parity-001",
		VideoName:      "Parity render name",
		ScriptText:     "The same render script.",
		Scenes: []SubmitScene{{
			Text:            "Parity scene",
			SceneID:         "scene-0",
			Index:           0,
			Kind:            "clip",
			Clip:            &SubmitClip{URL: "velox-asset://clip/parity.mp4"},
			Voiceover:       &SubmitVoiceover{URL: "velox-asset://voiceover/parity.mp3"},
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://subtitles/parity.srt", Format: "srt"},
			DurationSeconds: 5,
		}},
		Layers: []SubmitLayer{{
			ID:              "title-1",
			Type:            "text",
			Role:            "title",
			Text:            "Parity",
			Font:            "Inter",
			FontSize:        48,
			StartSeconds:    0,
			DurationSeconds: 5,
		}},
		AudioTracks: []SubmitAudioTrack{{
			SourceURL:       "velox-asset://audio/parity.mp3",
			Role:            "background_music",
			Volume:          0.12,
			StartTimeOffset: 0,
			DurationSeconds: 5,
		}},
		DeliveryPlan: []SubmitDeliveryPlanEntry{{
			DestinationID: "drive-parity",
			Priority:      1,
			RetryBudget:   &retryBudget,
		}},
	}

	rawAPI := submitRequestToRawPayload(&req)
	canonicalAPI := (&Handlers{}).NormalizeExternalJobSubmission(req)
	apiPayload := canonicalAPI.WorkerPayload
	if apiPayload == nil {
		t.Fatal("API projection returned nil worker payload")
	}
	if canonicalAPI.DeliveryPlan["delivery_plan"] == nil {
		t.Fatalf("API control-plane delivery plan was lost: %#v", canonicalAPI.DeliveryPlan)
	}
	for _, key := range []string{
		"video_name", "script_text", "scenes_json",
		"audio_tracks", "layers",
	} {
		if _, ok := apiPayload[key]; !ok {
			t.Fatalf("API baseline is missing required common worker field %q", key)
		}
	}

	creatorWorkerPayload, err := creatorflow.BuildWorkerPayload(cloneParityMap(rawAPI))
	if err != nil {
		t.Fatalf("creatorflow projection: %v", err)
	}

	normalizedPayload, err := enqueue.NormalizeSceneVideoPayload(cloneParityMap(rawAPI))
	if err != nil {
		t.Fatalf("scene-video normalization: %v", err)
	}

	pipelinePayload, err := enqueue.BuildPipelinePayload(map[string]interface{}{
		"status": "completed",
		"result": cloneParityMap(rawAPI),
	})
	if err != nil {
		t.Fatalf("pipeline builder: %v", err)
	}

	remoteDTO, err := remoteengine.ParseRemotePipelineResult(cloneParityMap(rawAPI))
	if err != nil {
		t.Fatalf("remote-engine parse: %v", err)
	}
	remotePayload, err := remoteDTO.ToWorkerPayloadChecked()
	if err != nil {
		t.Fatalf("remote worker projection: %v", err)
	}

	projections := map[string]map[string]interface{}{
		"api":           apiPayload,
		"creatorflow":   creatorWorkerPayload,
		"normalizer":    normalizedPayload,
		"pipeline":      pipelinePayload,
		"remote_engine": remotePayload,
	}
	want := canonicalWorkerProjection(apiPayload)
	for name, payload := range projections {
		got := canonicalWorkerProjection(payload)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s worker projection drifted:\nwant=%s\n got=%s", name, mustJSON(want), mustJSON(got))
		}
	}
}

// canonicalWorkerProjection retains the functional worker contract and
// canonicalizes JSON-shaped values so []string/[]interface{} and map order do
// not create incidental failures. It deliberately keeps all render timeline,
// asset, metadata, and delivery-routing fields in scope.
func canonicalWorkerProjection(payload map[string]interface{}) map[string]interface{} {
	const (
		videoName       = "video_name"
		scriptText      = "script_text"
		scenesJSON      = "scenes_json"
		audioTracks     = "audio_tracks"
		layers          = "layers"
		videoMetadata   = "video_metadata"
		outputPath      = "output_path"
		driveOutput     = "drive_output_folder"
		audioLanguage   = "audio_language_for_srt"
		videoMode       = "video_mode"
		sceneImagePaths = "scene_image_paths"
		imageSourceMap  = "image_source_map"
		items           = "items"
		clips           = "clips"
		images          = "images"
	)
	projection := make(map[string]interface{})
	for _, key := range []string{
		videoName, scriptText, scenesJSON, audioTracks, layers, videoMetadata, outputPath, driveOutput,

		audioLanguage, videoMode, sceneImagePaths, imageSourceMap,
		clips, images,
	} {
		value, ok := payload[key]
		if !ok {
			continue
		}
		if key == scenesJSON {
			if encoded, ok := value.(string); ok {
				var decoded interface{}
				if json.Unmarshal([]byte(encoded), &decoded) == nil {
					projection[key] = decoded
					continue
				}
			}
		}
		projection[key] = normalizeJSONValue(value)
	}
	return projection
}

func normalizeJSONValue(value interface{}) interface{} {
	encoded, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized interface{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return value
	}
	return normalized
}

func cloneParityMap(value map[string]interface{}) map[string]interface{} {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var clone map[string]interface{}
	if err := json.Unmarshal(encoded, &clone); err != nil {
		panic(err)
	}
	return clone
}

func mustJSON(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "<marshal error>"
	}
	return string(encoded)
}
