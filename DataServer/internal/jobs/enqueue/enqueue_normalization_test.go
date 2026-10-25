package enqueue

import (
	"encoding/json"
	"strings"
	"testing"

	"velox-shared/contract"
)

func TestNormalizeCompiledV2PayloadIsIdempotent(t *testing.T) {
	planJSON, planSHA := testCompiledV2Plan()
	input := map[string]interface{}{
		"job_id": "idempotent-job", "job_run_id": "idempotent-run",
		"correlation_id": "idempotent-correlation", "video_name": "V2 idempotent",
		"script_text": "Producer-compiled plan.",
		contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
		contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
	}
	first, err := normalizeSceneVideoPayload(input)
	if err != nil {
		t.Fatalf("first normalization: %v", err)
	}
	before, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeSceneVideoPayload(first)
	if err != nil {
		t.Fatalf("second normalization: %v", err)
	}
	after, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || string(before) != string(secondJSON) {
		t.Fatalf("V2 normalization is not stable:\nfirst=%s\nsecond=%s", before, secondJSON)
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(second); err != nil {
		t.Fatalf("normalized V2 plan invalid: %v", err)
	}
}

func TestBuildPipelinePayloadPassesProducerPlanThrough(t *testing.T) {
	planJSON, planSHA := testCompiledV2Plan()
	input := map[string]interface{}{
		"status": contract.InputAssemblyCompleted,
		"result": map[string]interface{}{
			"video_name": "Pipeline V2", "script_text": "Plan bytes stay canonical.",
			contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
			contract.PayloadKeyCompiledRenderPlanSHA:  planSHA,
		},
	}
	payload, err := BuildPipelinePayload(input)
	if err != nil {
		t.Fatalf("BuildPipelinePayload: %v", err)
	}
	if payload[contract.PayloadKeyCompiledRenderPlanJSON] != planJSON || payload[contract.PayloadKeyCompiledRenderPlanSHA] != planSHA {
		t.Fatal("BuildPipelinePayload changed producer plan bytes or hash")
	}
	if _, present := payload["scenes_json"]; present {
		t.Fatal("legacy scenes_json crossed the worker boundary")
	}
	if _, err := BuildPipelinePayload(map[string]interface{}{"scenes": []interface{}{map[string]interface{}{"text": "legacy"}}}); err == nil {
		t.Fatal("BuildPipelinePayload accepted a legacy scene payload without CompiledRenderPlanV2")
	}
}

func TestFlattenPipelineResult(t *testing.T) {
	t.Parallel()
	nested := map[string]interface{}{"ok": true, "result": map[string]interface{}{"title": "T", "text": "X"}}
	flat := FlattenPipelineResult(nested)
	if flat["title"] != "T" || flat["ok"] != true {
		t.Errorf("unexpected: %v", flat)
	}
	plain := map[string]interface{}{"ok": true, "title": "Flat"}
	if FlattenPipelineResult(plain)["title"] != "Flat" {
		t.Error("flat result mismatch")
	}
}

func TestShouldForwardPipelineResult(t *testing.T) {
	t.Parallel()
	plan := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version":2}`,
		contract.PayloadKeyCompiledRenderPlanSHA:  strings.Repeat("a", 64),
	}
	valid := map[string]interface{}{"status": "completed", "result": plan}
	if !ShouldForwardPipelineResult(valid) {
		t.Error("want true for completed producer-owned V2 plan")
	}
	if ShouldForwardPipelineResult(nil) {
		t.Error("want false for nil")
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"scenes_json": `[]`}) {
		t.Error("legacy scene payload without a producer V2 plan must not be forwarded")
	}
	if ShouldForwardPipelineResult(map[string]interface{}{"status": "failed"}) {
		t.Error("want false for failed")
	}
	for _, status := range []string{"succeeded", "done", "published"} {
		result := map[string]interface{}{"status": status}
		for key, value := range plan {
			result[key] = value
		}
		if ShouldForwardPipelineResult(result) {
			t.Errorf("status %q is a lifecycle/publication alias and must not be accepted as input-assembly completion", status)
		}
	}
	partialPlan := map[string]interface{}{contract.PayloadKeyCompiledRenderPlanJSON: `{"plan_version":2}`}
	if readiness := InspectPipelineResultReadiness(partialPlan); readiness.Complete || readiness.Reason != "compiled_plan_incomplete" {
		t.Errorf("incomplete V2 plan readiness = %+v", readiness)
	}
}
