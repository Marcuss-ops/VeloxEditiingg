package enqueue

import (
	"encoding/json"
	"fmt"

	"velox-shared/contract"
)

// ProjectLegacyWorkerPayload creates the compatibility payload sent only to
// workers that cannot consume the canonical timeline contract. The legacy
// wire contract intentionally remains hybrid: canonical fields stay present
// for readers that understand them, while items/clips/video_mode and derived
// legacy timeline fields provide the fallback required by older workers. The
// input is never mutated; the persisted task remains canonical.
func ProjectLegacyWorkerPayload(canonical map[string]interface{}) (map[string]interface{}, error) {
	if canonical == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("clone canonical worker payload: %w", err)
	}
	legacy := make(map[string]interface{})
	if err := json.Unmarshal(encoded, &legacy); err != nil {
		return nil, fmt.Errorf("decode canonical worker payload clone: %w", err)
	}

	attachLegacySceneClipTimeline(legacy)
	// The v1 worker adapter reads render inputs from the RenderPlan's
	// `parameters` object. Canonical V2 payloads intentionally keep those
	// fields at the top level, so rebuild the compatibility envelope only
	// on this worker-specific copy. Do not persist this mirror in the
	// canonical task spec.
	legacy["parameters"] = legacyRenderParameters(legacy)
	legacy["payload_contract_version"] = contract.PayloadContractVersionLegacy
	// The worker admission adapter treats the string version as the
	// legacy render-plan discriminator. A canonical payload may carry
	// version="v2"; forwarding that value on the legacy wire path makes
	// a v1-capable worker reject an otherwise valid compatibility payload.
	legacy["version"] = "v1"
	return legacy, nil
}

func legacyRenderParameters(payload map[string]interface{}) map[string]interface{} {
	parameters := make(map[string]interface{})
	for _, key := range []string{
		"audio_language_for_srt",
		"audio_tracks",
		"audio_url",
		"clips",
		"images",
		"items",
		"layers",
		"scene_image_paths",
		"scenes",
		"scenes_json",
		"script_text",
		"subtitle_tracks",
		"video_metadata",
		"video_mode",
		"voiceover_paths",
	} {
		if value, ok := payload[key]; ok && value != nil {
			parameters[key] = value
		}
	}
	return parameters
}
