package pipeline

import (
	"fmt"
	"strings"
)

// RecipeDefinition is the single supported video-render contract. Producers
// compile editorial timelines before submission; the Master only validates
// and forwards CompiledRenderPlanV2.
type RecipeDefinition struct {
	JobType      string
	RendererMode string
}

var recipeRegistry = map[string]RecipeDefinition{
	"video.render.v2": {JobType: "video.render.v2"},
}

func ResolveRecipe(jobType string) (RecipeDefinition, bool) {
	recipe, ok := recipeRegistry[jobType]
	return recipe, ok
}

// NormalizeCanonicalRecipe applies recipe defaults without translating scenes
// or compiling a manifest on the Master.
func NormalizeCanonicalRecipe(req *SubmitJobRequest) error {
	if req == nil {
		return fmt.Errorf("job request is nil")
	}
	if strings.TrimSpace(req.JobType) == "" {
		req.JobType = "video.render.v2"
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
	if len(req.Spec) > 0 || len(req.Scenes) > 0 {
		return fmt.Errorf("inline spec/scenes are retired; submit the producer-compiled CompiledRenderPlanV2")
	}
	return nil
}
