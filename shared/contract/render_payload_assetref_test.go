package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderOnlyPayloadWritesSelfSufficientAssetWire(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip_link":        "https://drive.google.com/file/d/drive-file-123456/view",
				"image_link":       "https://cdn.example.test/image.png",
				"local_asset":      "velox-asset://already-local",
				"duration_seconds": 2.0,
			},
		},
	}
	projected, err := RenderOnlyPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	scenes := projected["scenes"].([]interface{})
	scene := scenes[0].(map[string]interface{})
	clip := scene["clip"].(map[string]interface{})
	// The deferred Drive reference is written as its self-sufficient wire
	// (velox-drive://) — the scheme carries the kind, no sibling annotation.
	if got := clip["url"]; got != "velox-drive://drive-file-123456" {
		t.Fatalf("Drive wire URL = %v", got)
	}
	if got := clip["asset_id"]; got != "drive-file-123456" {
		t.Fatalf("Drive asset_id = %v", got)
	}
	if _, present := clip["asset_ref_kind"]; present {
		t.Fatalf("legacy asset_ref_kind leaked into renderer payload: %#v", clip)
	}
	image := scene["image"].(map[string]interface{})
	if got := image["url"]; got != "https://cdn.example.test/image.png" {
		t.Fatalf("remote wire URL = %v", got)
	}
	if _, present := image["asset_id"]; present {
		t.Fatalf("remote image unexpectedly received asset_id: %#v", image)
	}
	if got := scene["local_asset"]; got != "velox-asset://already-local" {
		t.Fatalf("local asset field changed: %v", got)
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"AssetRef"`) {
		t.Fatal("typed AssetRef leaked into renderer payload")
	}
}
