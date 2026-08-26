// Package enqueue - scene normalization helpers.
package enqueue

import (
	"context"
	"encoding/json"
	"strings"

	"velox-server/internal/telemetry"
	"velox-shared/contract"
)

func normalizeScenesContext(ctx context.Context, payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {
	if value, ok := payloadMap["scenes"]; ok && value != nil {
		result, err := contract.ParseSceneMaps(value)
		if err != nil {
			return nil, "", err
		}
		telemetry.RecordEnqueueJSONMarshal(ctx)
		data, err := json.Marshal(result)
		if err != nil {
			return nil, "", err
		}
		return result, string(data), nil
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		telemetry.RecordEnqueueJSONUnmarshal(ctx)
		scenes, err := contract.ParseSceneMapsJSON([]byte(s))
		if err != nil {
			return nil, "", err
		}
		telemetry.RecordEnqueueJSONMarshal(ctx)
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}

func normalizeSceneArray(value interface{}) []map[string]interface{} {
	scenes, err := contract.ParseSceneMaps(value)
	if err != nil {
		return nil
	}
	return scenes
}

func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, err := contract.ParseSceneMaps(payloadMap["scenes"]); err == nil {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		if scenes, err := contract.ParseSceneMapsJSON([]byte(s)); err == nil {
			return len(scenes)
		}
	}
	return 0
}
