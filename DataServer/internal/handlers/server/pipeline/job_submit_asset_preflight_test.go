package pipeline

import "testing"

func TestCollectAssetPreflightRequirementsDeduplicatesLocalAndSkipsDeferredDrive(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"clip": map[string]interface{}{
					"asset_id": "asset-1", "url": "velox-asset://asset-1",
					"sha256":     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"size_bytes": float64(123),
				},
			},
			map[string]interface{}{
				"clip": map[string]interface{}{"url": "velox-asset://asset-1"},
			},
			map[string]interface{}{
				"clip": map[string]interface{}{"url": "velox-drive://drive-1"},
			},
		},
	}

	requirements := collectAssetPreflightRequirements(payload)
	if len(requirements) != 1 {
		t.Fatalf("requirements = %#v, want one local asset", requirements)
	}
	if requirements[0].AssetID != "asset-1" || requirements[0].SizeBytes != 123 || requirements[0].SHA256 == "" {
		t.Fatalf("requirement = %#v, want merged metadata", requirements[0])
	}
}
