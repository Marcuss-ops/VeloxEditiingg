package pipeline

import "testing"

func TestRecipeRegistryOnlySupportsCompiledV2(t *testing.T) {
	if _, ok := ResolveRecipe("video.render.v2"); !ok {
		t.Fatal("video.render.v2 must be registered")
	}
	for _, retired := range []string{"scene.composite.v1", "clip.stock.v1", "scene.image.v1", "slideshow.v1", "legacy.render.v1"} {
		if _, ok := ResolveRecipe(retired); ok {
			t.Errorf("retired recipe %s is still registered", retired)
		}
	}
}

func TestNormalizeCanonicalRecipeRequiresProducerPlanBoundary(t *testing.T) {
	req := SubmitJobRequest{JobType: "video.render.v2", TemplateVersion: 1}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		t.Fatalf("NormalizeCanonicalRecipe: %v", err)
	}
	if req.JobType != "video.render.v2" {
		t.Fatalf("job type = %q", req.JobType)
	}
	legacy := SubmitJobRequest{JobType: "video.render.v2", Spec: map[string]interface{}{"scenes": []interface{}{}}}
	if err := NormalizeCanonicalRecipe(&legacy); err == nil {
		t.Fatal("inline scene spec was accepted")
	}
}

func TestValidateSubmitJobRequestRejectsPartialCompiledPlan(t *testing.T) {
	req := SubmitJobRequest{CompiledRenderPlanJSON: `{"plan_version":2}`}
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
