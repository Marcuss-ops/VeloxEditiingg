package contract

import "testing"

func TestJobPayloadV2PreservesVideoMetadata(t *testing.T) {
	p := NewJobPayloadV2(map[string]any{
		"video_name": "Test video",
		"video_metadata": map[string]any{
			"title":          "Test video",
			"description":    "Description",
			"tags":           []any{"velox", "test"},
			"privacy_status": "private",
			"publish_at":     "2026-07-20T18:00:00+02:00",
			"channel_id":     "channel-1",
		},
	})

	out, err := p.ToMap()
	if err != nil {
		t.Fatalf("ToMap: %v", err)
	}
	metadata, ok := out["video_metadata"].(map[string]any)
	if !ok {
		t.Fatalf("video_metadata type = %T, want map[string]any", out["video_metadata"])
	}
	if metadata["publish_at"] != "2026-07-20T18:00:00+02:00" {
		t.Fatalf("publish_at was not preserved: %v", metadata["publish_at"])
	}
}

func TestValidatePayloadRejectsInvalidVideoMetadataShape(t *testing.T) {
	err := ValidatePayload(map[string]any{"video_metadata": "invalid"})
	if err == nil {
		t.Fatal("expected invalid video_metadata shape to be rejected")
	}
}
