package pipeline

import (
	"testing"

	"velox-server/internal/jobs/enqueue"
)

func TestRenderOnlyV2RequestSurvivesIntoControlPlanePayload(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	req := SubmitJobRequest{
		IdempotencyKey:           "render-only-v2-001",
		VideoName:                "Render only",
		ScriptText:               "No external delivery",
		JobType:                  "video.render.v2",
		RenderOnly:               true,
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
	}
	raw := submitRequestToRawPayload(&req)
	if raw["render_only"] != true {
		t.Fatalf("request projection render_only = %#v, want true", raw["render_only"])
	}
	normalized, err := enqueue.NormalizeSceneVideoPayload(raw)
	if err != nil {
		t.Fatalf("normalize render-only payload: %v", err)
	}
	if normalized["render_only"] != true {
		t.Fatalf("normalized render_only = %#v, want true", normalized["render_only"])
	}
	worker, err := projectWorkerPayload(&req)
	if err != nil {
		t.Fatalf("project worker payload: %v", err)
	}
	if worker["render_only"] != true {
		t.Fatalf("worker payload render_only = %#v, want true", worker["render_only"])
	}
}
