// Package enqueue - scene normalization helpers.
package enqueue

import (
	"strings"

	"velox-shared/contract"
)

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
