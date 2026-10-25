// Package enqueue — pipeline payload builder (remote engine → worker handoff).
package enqueue

import (
	"fmt"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

// =============================================================================
// Pipeline payload builder (remote engine → worker handoff)
// =============================================================================

// BuildPipelinePayload validates and forwards a producer-compiled V2 plan.
// The Master does not compile scenes, bind media, or reconstruct timelines.
func BuildPipelinePayload(result map[string]interface{}) (map[string]interface{}, error) {
	if result == nil {
		return nil, fmt.Errorf("pipeline result is empty")
	}
	flat := FlattenPipelineResult(result)
	if !compiledRenderPlanV2Present(flat) {
		return nil, fmt.Errorf("compiled_render_plan_v2: producer plan and SHA are required")
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(flat); err != nil {
		return nil, fmt.Errorf("compiled_render_plan_v2: %w", err)
	}
	return contract.RenderOnlyPayload(flat)
}

// FlattenPipelineResult flattens a nested pipeline result by merging top-level
// keys with any nested "result" map.
func FlattenPipelineResult(result map[string]interface{}) map[string]interface{} {
	flat := make(map[string]interface{}, len(result)+8)
	for k, v := range result {
		flat[k] = v
	}
	if nested, ok := result["result"].(map[string]interface{}); ok {
		for k, v := range nested {
			flat[k] = v
		}
	}
	return flat
}

// PipelineResultReadiness is a payload-safe summary of the resolver's
// forwardability check. It deliberately contains no URLs, scene text, or
// asset identifiers so it can be written to operational logs.
type PipelineResultReadiness struct {
	Complete        bool
	Reason          string
	HasCompiledPlan bool
}

// InspectPipelineResultReadiness explains whether a pipeline result is
// complete enough to be forwarded to a worker without exposing payload data.
func InspectPipelineResultReadiness(result map[string]interface{}) PipelineResultReadiness {
	readiness := PipelineResultReadiness{Reason: "payload_missing"}
	if result == nil {
		return readiness
	}
	flat := FlattenPipelineResult(result)
	status := strings.ToLower(strings.TrimSpace(payload.FirstString(flat, "status")))
	// The remote-engine/creator handoff uses InputAssemblyCompleted. Do not
	// accept job-lifecycle aliases here: SUCCEEDED belongs to the Velox job
	// domain and DONE is not part of the remote-engine contract. Keeping this
	// gate strict prevents an already-terminal job status from being mistaken
	// for a producer-side completed input handoff.
	if status != "" && status != string(contract.InputAssemblyCompleted) {
		readiness.Reason = "input_assembly_not_completed"
		return readiness
	}
	// A producer-owned CompiledRenderPlanV2 is the only complete worker input.
	// Scene lists and positional audio belong to the retired scene compositor;
	// the Master no longer reconstructs timelines from them.
	planJSON := strings.TrimSpace(payload.FirstString(flat, contract.PayloadKeyCompiledRenderPlanJSON))
	planSHA := strings.TrimSpace(payload.FirstString(flat, contract.PayloadKeyCompiledRenderPlanSHA))
	readiness.HasCompiledPlan = planJSON != "" && planSHA != ""
	if readiness.HasCompiledPlan {
		readiness.Complete = true
		readiness.Reason = "ready"
		return readiness
	}
	if planJSON != "" || planSHA != "" {
		readiness.Reason = "compiled_plan_incomplete"
	} else {
		readiness.Reason = "compiled_render_plan_v2_missing"
	}
	return readiness
}

// ShouldForwardPipelineResult checks whether a pipeline result is complete
// enough to be forwarded to a worker for video rendering.
func ShouldForwardPipelineResult(result map[string]interface{}) bool {
	return InspectPipelineResultReadiness(result).Complete
}
