package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ParseSceneMapsJSON is the canonical parser for the scene-array payload
// format. It accepts the JSON representation used by scenes_json, normalizes
// each scene through NormalizeSceneEntry, and rejects malformed or non-array
// documents. Callers must not maintain a second JSON decode loop for scenes.
func ParseSceneMapsJSON(data []byte) ([]map[string]interface{}, error) {
	if strings.TrimSpace(string(data)) == "" {
		return nil, nil
	}
	var raw []map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("contract: parse scenes JSON: %w", err)
	}
	return NormalizeSceneMaps(raw), nil
}

// ParseSceneMaps parses an already decoded scene-array value and applies the
// same canonical normalization as ParseSceneMapsJSON. The accepted Go shapes
// are the two forms produced by encoding/json when decoding into generic
// payload maps.
func ParseSceneMaps(value interface{}) ([]map[string]interface{}, error) {
	switch scenes := value.(type) {
	case []map[string]interface{}:
		return NormalizeSceneMaps(scenes), nil
	case []interface{}:
		result := make([]map[string]interface{}, 0, len(scenes))
		for index, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("contract: scenes[%d] must be an object", index)
			}
			result = append(result, NormalizeSceneEntry(scene))
		}
		return result, nil
	default:
		return nil, fmt.Errorf("contract: scenes must be an array (got %T)", value)
	}
}

// NormalizeSceneMaps applies the canonical scene normalizer without exposing
// a mutable reference to the input maps.
func NormalizeSceneMaps(scenes []map[string]interface{}) []map[string]interface{} {
	if len(scenes) == 0 {
		return nil
	}
	result := make([]map[string]interface{}, 0, len(scenes))
	for _, scene := range scenes {
		result = append(result, NormalizeSceneEntry(scene))
	}
	return result
}
