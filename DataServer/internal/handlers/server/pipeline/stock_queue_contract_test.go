package pipeline

import "testing"

func TestNormalizeCanonicalRecipeRejectsInlineSceneSpec(t *testing.T) {
	req := SubmitJobRequest{
		JobType: "video.render.v2",
		Spec:    map[string]interface{}{"scenes": []interface{}{map[string]interface{}{"text": "retired"}}},
	}
	if err := NormalizeCanonicalRecipe(&req); err == nil {
		t.Fatal("inline scene spec was accepted; producer must submit CompiledRenderPlanV2")
	}
}
