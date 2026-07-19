package translation

import (
	"fmt"
	"strings"
)

// RenderGoogleDocContent renders one deterministic section per scene.
func RenderGoogleDocContent(raw map[string]interface{}) (string, error) {
	items, ok := raw["scenes"].([]interface{})
	if !ok || len(items) == 0 {
		return "", fmt.Errorf("google doc requires a non-empty scenes array")
	}
	var b strings.Builder
	if title := firstString(raw, "video_name", "title", "topic"); title != "" {
		b.WriteString(title + "\n\n")
	}
	for i, value := range items {
		scene, ok := value.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("scene %d is not an object", i)
		}
		text := firstString(scene, "text", "description")
		if text == "" {
			return "", fmt.Errorf("scene %d has no source text", i)
		}
		b.WriteString(fmt.Sprintf("Scene %d\nOriginal: %s", i+1, text))
		if translated := firstString(scene, "translated_text"); translated != "" {
			b.WriteString("\nTranslation: " + translated)
		}
		b.WriteString("\n\n")
	}
	return strings.TrimSpace(b.String()), nil
}

func firstString(values map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
