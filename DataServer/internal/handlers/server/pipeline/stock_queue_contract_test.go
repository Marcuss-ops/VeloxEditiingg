package pipeline

import (
	"testing"
)

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
