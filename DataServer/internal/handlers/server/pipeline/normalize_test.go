package pipeline

import "testing"

func TestNormalizeExternalJobSubmissionPassesProducerV2Plan(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	request := SubmitJobRequest{
		IdempotencyKey:           "producer-v2-001",
		VideoName:                "Producer timeline",
		ScriptText:               "Compiled by producer",
		CompiledRenderPlanJSON:   planJSON,
		CompiledRenderPlanSHA256: planSHA,
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(request)
	if canonical == nil || canonical.WorkerPayload == nil {
		t.Fatal("canonical submission returned no worker payload")
	}
	if canonical.WorkerPayload["compiled_render_plan_json"] != planJSON || canonical.WorkerPayload["compiled_render_plan_sha256"] != planSHA {
		t.Fatal("Master changed producer-owned V2 plan bytes")
	}
	if _, present := canonical.WorkerPayload["scenes_json"]; present {
		t.Fatal("Master reconstructed legacy scenes_json")
	}
}
