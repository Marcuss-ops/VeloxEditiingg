package enqueue

import (
	"strings"
	"testing"
)

func TestNormalizeRejectsLegacyVisualReplacements(t *testing.T) {
	_, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name": "Legacy replacement",
		"visual_replacements": []interface{}{map[string]interface{}{
			"asset_id": "replacement", "timeline_start_us": 0, "timeline_end_us": 1_000_000,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatalf("visual_replacements error = %v; want explicit retired-input rejection", err)
	}
}
