package pipeline

import (
	"fmt"
	"strings"
)

// RecipeDefinition is the registry entry for one technical rendering recipe.
// Editorial differences belong in template_id, not in new HTTP routes.
type RecipeDefinition struct {
	JobType      string
	RendererMode string
}

var recipeRegistry = map[string]RecipeDefinition{
	"scene.composite.v1": {JobType: "scene.composite.v1"},
	"clip.stock.v1":      {JobType: "clip.stock.v1", RendererMode: "clip_stock"},
	"scene.image.v1":     {JobType: "scene.image.v1", RendererMode: "scene_image"},
	"slideshow.v1":       {JobType: "slideshow.v1", RendererMode: "slideshow"},
}

func ResolveRecipe(jobType string) (RecipeDefinition, bool) {
	recipe, ok := recipeRegistry[jobType]
	return recipe, ok
}

// NormalizeCanonicalRecipe projects the recipe envelope into the typed
// SubmitJobRequest used by the existing validation and enqueue path.
// Keeping this projection at the HTTP boundary lets all producers share one
// validator and one resolver while recipe compilers evolve independently.
func NormalizeCanonicalRecipe(req *SubmitJobRequest) error {
	if req == nil {
		return fmt.Errorf("job request is nil")
	}
	if strings.TrimSpace(req.JobType) == "" {
		req.JobType = "scene.composite.v1"
	}
	req.JobType = strings.TrimSpace(req.JobType)
	if _, ok := ResolveRecipe(req.JobType); !ok {
		return fmt.Errorf("unsupported job_type %q", req.JobType)
	}
	if req.TemplateVersion == 0 {
		req.TemplateVersion = 1
	}
	if req.TemplateVersion < 1 {
		return fmt.Errorf("template_version must be positive")
	}
	if len(req.Spec) == 0 {
		return nil
	}

	if req.VideoName == "" {
		req.VideoName = firstRecipeString(req.Spec, "video_name", "title")
	}
	if req.ScriptText == "" {
		req.ScriptText = firstRecipeString(req.Spec, "script_text", "script")
	}
	if len(req.DeliveryPlan) == 0 {
		// DeliveryPlan is intentionally typed at the public boundary. Recipe
		// producers should use the same canonical field, not a second route.
		if raw, ok := req.Spec["delivery_plan"].([]interface{}); ok {
			for _, item := range raw {
				if object, ok := item.(map[string]interface{}); ok {
					if destination, _ := object["destination_id"].(string); destination != "" {
						req.DeliveryPlan = append(req.DeliveryPlan, SubmitDeliveryPlanEntry{DestinationID: destination})
					}
				}
			}
		}
	}
	if len(req.Scenes) == 0 {
		rawScenes, ok := req.Spec["scenes"].([]interface{})
		if !ok {
			return fmt.Errorf("spec.scenes must be an array")
		}
		for index, raw := range rawScenes {
			scene, ok := raw.(map[string]interface{})
			if !ok {
				return fmt.Errorf("spec.scenes[%d] must be an object", index)
			}
			converted, err := canonicalRecipeScene(scene, index)
			if err != nil {
				return err
			}
			req.Scenes = append(req.Scenes, converted)
		}
	}
	return nil
}

func canonicalRecipeScene(raw map[string]interface{}, index int) (SubmitScene, error) {
	scene := SubmitScene{Index: int64(index)}
	stockFallbackExplicit := false
	scene.SceneID = firstRecipeString(raw, "scene_id", "id")
	scene.Kind = firstRecipeString(raw, "kind")
	scene.Text = firstRecipeString(raw, "text", "description")
	scene.DurationSeconds = recipeFloat(raw["duration_seconds"])
	if scene.DurationSeconds <= 0 {
		return scene, fmt.Errorf("spec.scenes[%d].duration_seconds must be positive", index)
	}
	if rawClip, ok := raw["clip"].(map[string]interface{}); ok {
		scene.Clip = recipeClip(rawClip)
	}
	if rawVoiceover, ok := raw["voiceover"].(map[string]interface{}); ok {
		scene.Voiceover = recipeVoiceover(rawVoiceover)
	}
	if rawSubtitles, ok := raw["subtitles"].(map[string]interface{}); ok {
		scene.Subtitles = recipeSubtitles(rawSubtitles)
	}
	if rawStock, ok := raw["stock"]; ok {
		applyRecipeStocks(&scene, rawStock)
		if explicit, ok := raw["stock_fallback"].(bool); ok {
			scene.StockFallback = explicit
			stockFallbackExplicit = true
		}
	}
	if scene.Stock != nil && !stockFallbackExplicit && !scene.StockFallback {
		scene.StockFallback = true
	}
	return scene, nil
}

func applyRecipeStocks(scene *SubmitScene, raw interface{}) {
	if scene == nil {
		return
	}
	switch value := raw.(type) {
	case map[string]interface{}:
		if scene.Stock == nil {
			scene.Stock = recipeClip(value)
		}
	case []interface{}:
		for _, item := range value {
			if object, ok := item.(map[string]interface{}); ok {
				if clip := recipeClip(object); clip != nil && clip.URL != "" {
					scene.StockAssets = append(scene.StockAssets, *clip)
				}
			} else if link, ok := item.(string); ok && strings.TrimSpace(link) != "" {
				trimmed := strings.TrimSpace(link)
				if clip := recipeClip(map[string]interface{}{"url": trimmed}); clip != nil {
					scene.StockAssets = append(scene.StockAssets, *clip)
				}
			}
		}
	}
	if len(scene.StockAssets) > 0 {
		scene.StockFallback = true
	}
}

func recipeClip(raw map[string]interface{}) *SubmitClip {
	if raw == nil {
		return nil
	}
	clip := &SubmitClip{
		AssetID:     firstRecipeString(raw, "asset_id", "clip_id"),
		DriveFileID: firstRecipeString(raw, "drive_file_id"),
		StartMS:     int64(recipeFloat(raw["start_ms"])),
		EndMS:       int64(recipeFloat(raw["end_ms"])),
		DurationMS:  int64(recipeFloat(raw["duration_ms"])),
	}
	clip.URL = firstRecipeString(raw, "url", "link", "drive_link", "folder_link")
	if clip.URL == "" && clip.AssetID != "" {
		clip.URL = "velox-asset://" + clip.AssetID
	}
	if clip.AssetID == "" && strings.HasPrefix(strings.ToLower(clip.URL), "velox-asset://") {
		clip.AssetID = strings.TrimPrefix(clip.URL, "velox-asset://")
	}
	return clip
}

func recipeSubtitles(raw map[string]interface{}) *SubmitSubtitles {
	if raw == nil {
		return nil
	}
	return &SubmitSubtitles{
		AssetID:  firstRecipeString(raw, "asset_id"),
		Format:   firstRecipeString(raw, "format"),
		URL:      firstRecipeString(raw, "url"),
		SHA256:   firstRecipeString(raw, "sha256"),
		Language: firstRecipeString(raw, "language"),
	}
}

func recipeVoiceover(raw map[string]interface{}) *SubmitVoiceover {
	if raw == nil {
		return nil
	}
	voiceover := &SubmitVoiceover{
		AssetID:     firstRecipeString(raw, "asset_id", "voiceover_id"),
		DriveFileID: firstRecipeString(raw, "drive_file_id"),
		SHA256:      firstRecipeString(raw, "sha256"),
		SizeBytes:   int64(recipeFloat(raw["size_bytes"])),
		DurationMS:  int64(recipeFloat(raw["duration_ms"])),
		Language:    firstRecipeString(raw, "language"),
	}
	voiceover.URL = firstRecipeString(raw, "url", "link", "drive_link", "local_path")
	if voiceover.URL == "" && voiceover.AssetID != "" {
		voiceover.URL = "velox-asset://" + voiceover.AssetID
	}
	return voiceover
}

func firstRecipeString(raw map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func recipeFloat(raw interface{}) float64 {
	switch value := raw.(type) {
	case float64:
		return value
	case int:
		return float64(value)
	case int64:
		return float64(value)
	}
	return 0
}
