package enqueue

import (
	"strings"
	"testing"
)

func TestNormalizeRejectsLegacyTimelineInputs(t *testing.T) {
	for key, value := range map[string]interface{}{
		"scenes":              []interface{}{map[string]interface{}{"text": "retired"}},
		"render_manifest":     map[string]interface{}{"schema": "legacy"},
		"layers":              []interface{}{map[string]interface{}{"type": "title"}},
		"visual_replacements": []interface{}{map[string]interface{}{"asset_id": "replacement"}},
	} {
		t.Run(key, func(t *testing.T) {
			_, err := normalizeSceneVideoPayload(map[string]interface{}{"video_name": "legacy", key: value})
			if err == nil || !strings.Contains(err.Error(), "retired") {
				t.Fatalf("legacy %s input error = %v; want explicit retired-input rejection", key, err)
			}
		})
	}
}
