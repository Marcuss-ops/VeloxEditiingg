package pipeline

import "testing"

func TestValidateSubmitRejectsLegacyOverlays(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	req := SubmitJobRequest{
		IdempotencyKey:           "overlay-end-only",
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
		Overlays: []SubmitOverlay{{
			ID: "overlay", AssetID: "asset", StartFrame: 24, EndFrame: 144,
			Mode: "replace", AudioMode: "preserve_final_audio",
		}},
	}
	if err, bad := ValidateSubmitJobRequest(req); !bad || err == nil {
		t.Fatal("legacy overlay was accepted alongside a V2 plan")
	}
}
