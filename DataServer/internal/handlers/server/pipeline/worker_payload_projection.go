// Package pipeline — worker_payload_projection.go creates the dispatch
// envelope around a producer-compiled V2 plan. It does not rebuild timelines.
package pipeline

import (
	"velox-shared/contract"
)

func projectWorkerPayload(req *SubmitJobRequest) (map[string]interface{}, error) {
	return contract.RenderOnlyPayload(submitRequestToRawPayload(req))
}

// submitRequestToRawPayload projects only the V2 execution bytes and fields
// needed by enqueue. Publication and routing intents remain control-plane data.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	if req == nil {
		return map[string]interface{}{}
	}
	raw := map[string]interface{}{
		"job_id":      req.IdempotencyKey,
		"video_name":  req.VideoName,
		"script_text": req.ScriptText,
		contract.PayloadKeyCompiledRenderPlanJSON: req.CompiledRenderPlanJSON,
		contract.PayloadKeyCompiledRenderPlanSHA:  req.CompiledRenderPlanSHA256,
	}
	if req.JobType != "" {
		raw["job_type"] = req.JobType
	}
	if req.RenderOnly {
		raw["render_only"] = true
	}
	if req.RuntimeAssets != nil {
		raw["runtime_assets"] = req.RuntimeAssets
	}
	if req.RuntimePayload != nil {
		raw["runtime_payload"] = req.RuntimePayload
	}
	if req.RuntimeAssetsPending {
		raw["runtime_assets_pending"] = true
	}
	if pin := req.PlacementPinWorkerID; pin != "" {
		raw["_placement_pin_worker_id"] = pin
	}
	if len(req.DeliveryPlan) > 0 {
		entries := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, entry := range req.DeliveryPlan {
			retryBudget := DefaultRetryBudget
			if entry.RetryBudget != nil {
				retryBudget = *entry.RetryBudget
			}
			entries = append(entries, map[string]interface{}{
				"destination_id": entry.DestinationID,
				"priority":       entry.Priority,
				"retry_budget":   retryBudget,
				"metadata":       entry.Metadata,
			})
		}
		raw["delivery_plan"] = entries
	}
	return raw
}
