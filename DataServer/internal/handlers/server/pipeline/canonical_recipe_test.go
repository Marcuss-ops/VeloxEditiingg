package pipeline

import "testing"

func TestRecipeRegistry_RejectsRemovedLegacyRenderType(t *testing.T) {
	if _, ok := ResolveRecipe("legacy.render.v1"); ok {
		t.Fatal("legacy.render.v1 must not be registered")
	}
}

func TestNormalizeCanonicalRecipe_SpecSceneBindings(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "recipe-1",
		JobType:        "scene.composite.v1",
		TemplateID:     "documentary.clip-stock",
		Spec: map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{
					"id":               "scene-0",
					"kind":             "intro",
					"text":             "Intro",
					"duration_seconds": 5.0,
					"clip":             map[string]interface{}{"asset_id": "clip-0", "url": "velox-asset://clip-0", "duration_ms": float64(5000)},
					"voiceover":        map[string]interface{}{"asset_id": "voice-0", "url": "velox-asset://voice-0", "duration_ms": float64(5000)},
				},
			},
		},
	}

	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if len(req.Scenes) != 1 {
		t.Fatalf("scenes = %d, want 1", len(req.Scenes))
	}
	scene := req.Scenes[0]
	if scene.DurationSeconds != 5 {
		t.Fatalf("duration_seconds = %v, want 5", scene.DurationSeconds)
	}
	if scene.Clip == nil || scene.Clip.URL != "velox-asset://clip-0" {
		t.Fatalf("clip = %#v, want velox asset clip", scene.Clip)
	}
	if scene.Voiceover == nil || scene.Voiceover.URL != "velox-asset://voice-0" {
		t.Fatalf("voiceover = %#v, want velox asset voiceover", scene.Voiceover)
	}
}

func TestNormalizeCanonicalRecipe_RejectsUnknownJobType(t *testing.T) {
	req := SubmitJobRequest{JobType: "boxing.v9"}
	if err := NormalizeCanonicalRecipe(&req); err == nil {
		t.Fatal("unknown job_type accepted")
	}
}

func TestNormalizeCanonicalRecipe_ProjectsMultipleBindingStocks(t *testing.T) {
	req := SubmitJobRequest{
		IdempotencyKey: "recipe-stocks",
		JobType:        "scene.composite.v1", Spec: map[string]interface{}{
			"scenes": []interface{}{
				map[string]interface{}{
					"text":             "Portrait",
					"duration_seconds": 5.0,
					"stock": []interface{}{
						map[string]interface{}{"url": "https://stock/a.mp4"},
						map[string]interface{}{"asset_id": "stock-b"},
					},
				},
			},
		},
	}

	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if got := req.Scenes[0].StockLinks; len(got) != 2 || got[0] != "https://stock/a.mp4" || got[1] != "velox-asset://stock-b" {
		t.Fatalf("stock_links = %#v, want both canonical links", got)
	}
	if !req.Scenes[0].StockFallback {
		t.Fatal("multiple stocks must enable stock fallback mode")
	}
}
