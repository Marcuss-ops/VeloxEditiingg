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
	legacy["payload_contract_version"] = contract.PayloadContractVersionLegacy
	return legacy, nil
}
