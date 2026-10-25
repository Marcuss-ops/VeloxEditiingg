package pipeline

import "testing"

func TestSubmitJobRejectsLegacyManifestRef(t *testing.T) {
	body := validSubmitJobBody(t, "manifest-ref-retired")
	body.ManifestRef = &SubmitManifestRef{
		SchemaVersion: "velox.render-manifest.v1",
		URL:           "https://assets.example.test/render-manifest.json",
		SHA256:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	validation, bad := ValidateSubmitJobRequest(body)
	if !bad || validation == nil {
		t.Fatal("legacy manifest_ref was accepted; clients must submit CompiledRenderPlanV2")
	}
	for _, detail := range validation.Details {
		if detail["path"] == "manifest_ref" && detail["issue"] == "legacy_manifest_input_removed; submit compiled_render_plan_json and compiled_render_plan_sha256" {
			return
		}
	}
	t.Fatalf("validation details do not identify retired manifest_ref: %#v", validation.Details)
}
