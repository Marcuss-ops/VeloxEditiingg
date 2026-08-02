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
	if v, ok := payloadMap["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, contract.NormalizeSceneEntry(m))
			}
			telemetry.RecordEnqueueJSONMarshal(ctx)
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, contract.NormalizeSceneEntry(item))
			}
			telemetry.RecordEnqueueJSONMarshal(ctx)
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		telemetry.RecordEnqueueJSONUnmarshal(ctx)
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = contract.NormalizeSceneEntry(scenes[i])
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
	switch scenes := value.(type) {
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	default:
		return nil
	}
}
func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, ok := payloadMap["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payloadMap["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}
