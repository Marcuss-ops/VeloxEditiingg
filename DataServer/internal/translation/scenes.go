package translation

import (
	"context"
	"fmt"
	"strings"
)

// TranslateScenes translates each scene independently and keeps the source
// text and clip bindings intact. A requested translation failure is returned
// to the API; the job is never reported as successfully translated when it is
// not.
func TranslateScenes(ctx context.Context, raw map[string]interface{}, client Client) (map[string]interface{}, error) {
	target := targetLanguage(raw["translate_to"])
	if target == "" {
		return raw, nil
	}
	input := sceneList(raw["scenes"])
	if len(input) == 0 {
		return nil, fmt.Errorf("translation requires a non-empty scenes array")
	}
	result := cloneMap(raw)
	scenes := make([]interface{}, len(input))
	translations := make([]map[string]interface{}, 0, len(input))
	for i, value := range input {
		scene := value
		text, _ := scene["text"].(string)
		translated, err := client.Translate(ctx, text, target)
		if err != nil {
			return nil, fmt.Errorf("translate scene %d: %w", i, err)
		}
		copyScene := cloneMap(scene)
		byLanguage := map[string]interface{}{}
		if existing, ok := copyScene["translations"].(map[string]interface{}); ok {
			for key, value := range existing {
				byLanguage[key] = value
			}
		}
		byLanguage[target] = translated
		copyScene["translations"] = byLanguage
		copyScene["translated_text"] = translated
		scenes[i] = copyScene
		translations = append(translations, map[string]interface{}{
			"index":    i,
			"language": target,
			"text":     translated,
		})
	}
	result["scenes"] = scenes
	result["translations"] = translations
	result["translation_status"] = "completed"
	result["translation_language"] = target
	return result, nil
}

func sceneList(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, value := range scenes {
			if scene, ok := value.(map[string]interface{}); ok {
				out = append(out, scene)
			}
		}
		return out
	case []map[string]interface{}:
		return scenes
	default:
		return nil
	}
}

func targetLanguage(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []interface{}:
		if len(typed) > 0 {
			if first, ok := typed[0].(string); ok {
				return strings.TrimSpace(first)
			}
		}
	}
	return ""
}

func cloneMap(input map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
