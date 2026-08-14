package enqueue

import (
	"testing"

	"velox-shared/contract"
)

func TestNormalizeSceneVideoPayload_EmitsCanonicalTimelineOnly(t *testing.T) {
	normalized, err := normalizeSceneVideoPayload(map[string]interface{}{
		"video_name":  "Canonical timeline",
		"script_text": "Canonical timeline body.",
		"copy_only":   true,
		"voiceover_paths": []interface{}{
			"velox-asset://voice-1",
		},
		"scenes": []interface{}{
			map[string]interface{}{
				"clip_link":        "velox-asset://clip-1",
				"duration_seconds": 5.0,
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	if got := normalized["payload_contract_version"]; got != contract.PayloadContractVersionCanonical {
		t.Fatalf("payload_contract_version = %v, want %d", got, contract.PayloadContractVersionCanonical)
	}
	if got, ok := normalized["copy_only"].(bool); !ok || !got {
		t.Fatalf("copy_only = %#v, want true to survive canonical normalization", normalized["copy_only"])
	}
	for _, legacyKey := range []string{"items", "clips", "video_mode"} {
		if _, ok := normalized[legacyKey]; ok {
			t.Fatalf("canonical normalization unexpectedly emitted legacy key %q: %#v", legacyKey, normalized[legacyKey])
		}
	}
}
