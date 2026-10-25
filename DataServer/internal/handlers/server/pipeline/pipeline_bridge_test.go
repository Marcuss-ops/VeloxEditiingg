package pipeline

import (
	"testing"

	"velox-server/internal/jobs/enqueue"
	"velox-shared/contract"
)

func TestBuildSceneVideoPayloadFromPipelineResultPassesThroughV2(t *testing.T) {
	planJSON, planSHA := validProducerOwnedFMP4Plan(t)
	result := map[string]interface{}{
		"status": contract.InputAssemblyCompleted,
		"result": map[string]interface{}{
			contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
			contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
		},
	}

	payload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		t.Fatalf("build V2 worker payload: %v", err)
	}
	if payload[contract.PayloadKeyCompiledRenderPlanJSON] != planJSON {
		t.Fatal("compiled render plan bytes changed during Master handoff")
	}
	if payload[contract.PayloadKeyCompiledRenderPlanSHA] != planSHA {
		t.Fatal("compiled render plan hash changed during Master handoff")
	}
	if _, present := payload["scenes_json"]; present {
		t.Fatal("legacy scene projection crossed the worker boundary")
	}
}
