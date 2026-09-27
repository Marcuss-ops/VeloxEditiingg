package pipeline

import "testing"

func TestRecipeRegistry_RejectsRemovedLegacyRenderType(t *testing.T) {
	if _, ok := ResolveRecipe("legacy.render.v1"); ok {
		t.Fatal("legacy.render.v1 must not be registered")
	}
}

func TestRecipeRegistry_OnlySceneCompositeAndStockClip(t *testing.T) {
	for _, jobType := range []string{"scene.composite.v1", "clip.stock.v1"} {
		if _, ok := ResolveRecipe(jobType); !ok {
			t.Errorf("%s must remain registered", jobType)
		}
	}
	for _, jobType := range []string{"scene.image.v1", "slideshow.v1"} {
		if _, ok := ResolveRecipe(jobType); ok {
			t.Errorf("%s must not be registered", jobType)
		}
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

func TestNormalizeCanonicalRecipe_PropagatesCopyOnlyFromSpec(t *testing.T) {
	req := SubmitJobRequest{
		JobType: "clip.stock.v1",
		Spec: map[string]interface{}{
			"copy_only": true,
			"scenes": []interface{}{
				map[string]interface{}{"id": "scene-0", "text": "clip", "duration_seconds": 1},
			},
		},
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if !req.CopyOnly {
		t.Fatal("CopyOnly = false, want true from spec.copy_only")
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
	if got := req.Scenes[0].StockAssets; len(got) != 2 || got[0].URL != "https://stock/a.mp4" || got[1].URL != "velox-asset://stock-b" {
		t.Fatalf("stock assets = %#v, want both canonical assets", got)
	}
	if !req.Scenes[0].StockFallback {
		t.Fatal("multiple stocks must enable stock fallback mode")
	}
}

func TestNormalizeCanonicalRecipePreservesDeferredDriveStockMetadata(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	req := SubmitJobRequest{
		JobType: "scene.composite.v1",
		Spec: map[string]interface{}{"scenes": []interface{}{map[string]interface{}{
			"kind": "clip", "text": "Narration", "duration_seconds": 5.0,
			"stock": []interface{}{map[string]interface{}{
				"drive_file_id": "stock-drive-1",
				"drive_link":    "https://drive.google.com/file/d/stock-drive-1/view",
				"url":           "velox-drive://stock-drive-1",
				"source_uri":    "https://drive.google.com/uc?export=download&id=stock-drive-1",
				"sha256":        sha,
				"size_bytes":    float64(4096),
				"duration_ms":   float64(5000),
			}},
		}}},
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if req.Scenes[0].Kind != "clip" || len(req.Scenes[0].StockAssets) != 1 {
		t.Fatalf("normalized scene = %+v", req.Scenes[0])
	}
	stock := req.Scenes[0].StockAssets[0]
	if stock.DriveFileID != "stock-drive-1" || stock.SourceURI == "" || stock.SHA256 != sha || stock.SizeBytes != 4096 || stock.DurationMS != 5000 {
		t.Fatalf("Drive stock metadata was not preserved: %+v", stock)
	}
}

func TestNormalizeCanonicalRecipe_AllowsCompiledPlanWithoutInlineScenes(t *testing.T) {
	req := SubmitJobRequest{
		JobType:                  "scene.composite.v1",
		CompiledRenderPlanJSON:   `{"plan_version":2}`,
		CompiledRenderPlanSHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}

	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if len(req.Scenes) != 0 {
		t.Fatalf("scenes = %d, want no inline scenes for compiled plan", len(req.Scenes))
	}
	if verr, bad := ValidateSubmitJobRequest(req); bad || verr != nil {
		t.Fatalf("compiled plan without scenes rejected: %#v", verr)
	}
}

func TestValidateSubmitJobRequestRejectsPartialCompiledPlan(t *testing.T) {
	req := SubmitJobRequest{
		Scenes:                 nil,
		CompiledRenderPlanJSON: `{"plan_version":2}`,
	}
	verr, bad := ValidateSubmitJobRequest(req)
	if !bad || verr == nil {
		t.Fatal("partial compiled plan must be rejected")
	}
	for _, detail := range verr.Details {
		if detail["path"] == "compiled_render_plan_sha256" && detail["issue"] == "required_with_compiled_render_plan_json" {
			return
		}
	}
	t.Fatalf("missing compiled plan SHA detail: %#v", verr.Details)
}
