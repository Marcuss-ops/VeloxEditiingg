package pipeline

import (
	"encoding/json"
	"testing"

	"velox-shared/assetref"
)

func TestClipStockProjectionKeepsCanonicalStockPoolInQueue(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "stock-queue-contract",
		JobType:        "clip.stock.v1",
		Spec: map[string]interface{}{
			"scenes": []interface{}{map[string]interface{}{
				"text":             "Stock scene",
				"duration_seconds": 12.0,
				"clip":             map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 4000},
				"stock": []interface{}{
					map[string]interface{}{"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000},
					map[string]interface{}{"asset_id": "stock-2", "url": "velox-asset://stock-2", "duration_ms": 7000},
				},
				"voiceover": map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 12000},
			}},
		},
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil || canonical.WorkerPayload == nil {
		t.Fatal("stock projection returned no worker payload")
	}
	items, ok := canonical.WorkerPayload["items"].([]map[string]interface{})
	if !ok {
		t.Fatalf("worker items type = %T, want []map[string]interface{}", canonical.WorkerPayload["items"])
	}
	if len(items) != 3 {
		t.Fatalf("queued items = %d, want two stock segments plus clip: %#v", len(items), items)
	}
	seen := map[string]bool{}
	for _, item := range items {
		if id, ok := item["asset_id"].(string); ok {
			seen[id] = true
		}
		if url, ok := item["url"].(string); ok && url == "velox-asset://clip-1" {
			seen["clip-1"] = true
		}
		if _, legacy := item["stock_links"]; legacy {
			t.Fatal("legacy stock_links leaked into the queued item")
		}
	}
	for _, id := range []string{"stock-1", "stock-2", "clip-1"} {
		if !seen[id] {
			t.Fatalf("queued stock/clip asset %q missing: %#v", id, items)
		}
	}
	payload, err := json.Marshal(canonical.WorkerPayload)
	if err != nil {
		t.Fatalf("marshal worker payload: %v", err)
	}
	protected := assetref.ExtractAssetKeys(payload)
	for _, id := range []string{"stock-1", "stock-2"} {
		if _, ok := protected[id]; !ok {
			t.Fatalf("queued stock asset %q is absent from the protected-cache key set: %v", id, protected)
		}
	}
}

func TestNormalizeCanonicalRecipePreservesExplicitStockFallbackFalse(t *testing.T) {
	req := SubmitJobRequest{
		JobType: "clip.stock.v1",
		Spec: map[string]interface{}{
			"scenes": []interface{}{map[string]interface{}{
				"text":             "explicit policy",
				"duration_seconds": 5.0,
				"clip":             map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 5000},
				"stock":            map[string]interface{}{"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000},
				"stock_fallback":   false,
			}},
		},
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if req.Scenes[0].StockFallback {
		t.Fatal("explicit stock_fallback=false was rewritten to true")
	}
}

func TestClipStockProjectionNormalizesSingleStockObjectToArray(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "single-stock-object",
		JobType:        "clip.stock.v1",
		Spec: map[string]interface{}{
			"scenes": []interface{}{map[string]interface{}{
				"text":             "Single stock scene",
				"duration_seconds": 5.0,
				"clip":             map[string]interface{}{"asset_id": "clip-1", "url": "velox-asset://clip-1", "duration_ms": 4000},
				"stock":            map[string]interface{}{"asset_id": "stock-1", "url": "velox-asset://stock-1", "duration_ms": 5000},
				"voiceover":        map[string]interface{}{"asset_id": "voice-1", "url": "velox-asset://voice-1", "duration_ms": 5000},
			}},
		},
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(req)
	if canonical == nil || canonical.WorkerPayload == nil {
		t.Fatal("single-stock projection returned no worker payload")
	}
	items, ok := canonical.WorkerPayload["items"].([]map[string]interface{})
	if !ok {
		t.Fatalf("worker items type = %T, want []map[string]interface{}", canonical.WorkerPayload["items"])
	}
	if len(items) != 2 || items[0]["role"] != "voiceover_bed" || items[1]["role"] != "scene_clip" {
		t.Fatalf("worker timeline = %#v, want stock bed followed by final clip", items)
	}
}
