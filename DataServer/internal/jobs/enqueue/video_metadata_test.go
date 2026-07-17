package enqueue

import (
	"strings"
	"testing"
)

func TestNormalizeSceneVideoPayloadPreservesAndValidatesVideoMetadata(t *testing.T) {
	payload := map[string]interface{}{
		"video_name": "Test video",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/1.png"},
		},
		"voiceover_paths": []interface{}{"https://example.com/voice.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "social-main", "retry_budget": 3},
		},
		"video_metadata": map[string]interface{}{
			"title":          "Published title",
			"description":    "Published description",
			"tags":           []interface{}{"velox", "test"},
			"privacy_status": "private",
			"publish_at":     "2026-07-20T18:00:00+02:00",
			"channel_id":     "channel-1",
		},
	}

	out, err := normalizeSceneVideoPayload(payload)
	if err != nil {
		t.Fatalf("normalizeSceneVideoPayload: %v", err)
	}
	metadata, ok := out["video_metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("video_metadata type = %T, want map[string]interface{}", out["video_metadata"])
	}
	if metadata["channel_id"] != "channel-1" || metadata["publish_at"] != "2026-07-20T18:00:00+02:00" {
		t.Fatalf("metadata was not preserved: %#v", metadata)
	}
	plan, ok := out["delivery_plan"].([]interface{})
	if !ok || len(plan) != 1 {
		t.Fatalf("delivery_plan = %#v", out["delivery_plan"])
	}
	planItem, ok := plan[0].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery plan item = %#v", plan[0])
	}
	planMetadata, ok := planItem["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("delivery metadata = %#v", planItem["metadata"])
	}
	if _, ok := planMetadata["video_metadata"]; !ok {
		t.Fatalf("delivery metadata does not carry video_metadata: %#v", planMetadata)
	}
}

func TestNormalizeSceneVideoPayloadRejectsInvalidVideoMetadata(t *testing.T) {
	payload := map[string]interface{}{
		"video_name": "Test video",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1"},
		},
		"voiceover_paths": []interface{}{"voice.mp3"},
		"video_metadata": map[string]interface{}{
			"privacy_status": "visible-to-everyone",
		},
	}

	_, err := normalizeSceneVideoPayload(payload)
	if err == nil || !strings.Contains(err.Error(), "video_metadata.privacy_status") {
		t.Fatalf("want privacy validation error, got %v", err)
	}
}
