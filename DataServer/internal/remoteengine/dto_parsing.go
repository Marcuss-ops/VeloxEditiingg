package remoteengine

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

// ── Initial response validation ──────────────────────────────────────────────

// ValidateInitialResponse validates that the raw remote response contains
// at least a job_id (with fallback to trace_id / id) and a known status.
// Returns a typed *RemoteError (PERMANENT, contract violation) on failure.
//
// The raw map is preserved in InitialResponse.RawResult so the caller can
// pass it to the polling path or to ParseRemotePipelineResult when the
// job is completed.
func ValidateInitialResponse(raw map[string]interface{}) (*InitialResponse, error) {
	if raw == nil {
		return nil, &RemoteError{
			Class:   RemoteErrorMalformed,
			Code:    "CONTRACT_NIL_RESPONSE",
			Message: "remote response is nil",
		}
	}

	jobID := payload.FirstString(raw, "job_id", "trace_id", "id")
	if jobID == "" {
		return nil, ErrContractMissingJobID
	}

	statusRaw, _ := raw["status"].(string)
	status := strings.ToLower(strings.TrimSpace(statusRaw))
	if status == "" {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "CONTRACT_MISSING_STATUS",
			Message: "remote response missing status",
		}
	}

	if !knownRemoteStatuses[status] {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "CONTRACT_UNKNOWN_STATUS",
			Message: fmt.Sprintf("remote response has unknown status %q (known: queued, running, completed, failed, cancelled)", status),
			Body:    truncateBody(jsonString(raw), 4096),
			Cause:   ErrContractUnknownStatus,
		}
	}

	return &InitialResponse{
		JobID:     jobID,
		Status:    status,
		RawResult: raw,
	}, nil
}

// ParseRemotePipelineResult converts a raw remote engine response map into
// the typed RemotePipelineResult DTO. It flattens the nested "result"
// envelope and extracts each sub-component with validation.
//
// This function does NOT reject incomplete results — it extracts whatever
// fields are present and leaves the rest zero-valued. The caller should
// use enqueue.ShouldForwardPipelineResult to check completeness before
// forwarding to the worker.
func ParseRemotePipelineResult(raw map[string]interface{}) (*RemotePipelineResult, error) {
	if raw == nil {
		return nil, fmt.Errorf("remoteengine: ParseRemotePipelineResult: raw map is nil")
	}
	flat := flattenResult(raw)
	planJSON, jsonOK := flat[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	planSHA, shaOK := flat[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	if !jsonOK || !shaOK || strings.TrimSpace(planJSON) == "" || strings.TrimSpace(planSHA) == "" {
		return nil, fmt.Errorf("remoteengine: producer-compiled CompiledRenderPlanV2 is required")
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(flat); err != nil {
		return nil, fmt.Errorf("remoteengine: invalid CompiledRenderPlanV2: %w", err)
	}
	return &RemotePipelineResult{RemoteJobID: payload.FirstString(flat, "job_id", "trace_id", "id"), Raw: flat}, nil
}

// ToWorkerPayloadChecked converts the typed DTO into a map[string]interface{}
// that enqueue.BuildPipelinePayload can consume. This ensures the worker
// receives a payload DERIVED from the typed DTO, not the raw remote map
// passed through unchecked.
//
// The merge strategy preserves render-only fields from the flattened raw map
// (for example output_path) while overlaying the typed DTO fields on top —
// the typed values take precedence, having been validated and normalized by
// ParseRemotePipelineResult. Control-plane publication and delivery fields
// are removed at the renderer boundary below.
//
// Overlaid fields include:
//   - job_id / trace_id / job_run_id / correlation_id (from RemoteJobID)
//   - video_name (from Script.Title)
//   - script_text (from Script.Text)
//   - scenes_json (serialized from Scenes)
//   - canonical nested scene assets (from Scenes)
//   - technical video metadata (from Metadata)
//   - json_path / markdown_path (from Script, for on-disk fallback)
func (r *RemotePipelineResult) ToWorkerPayloadChecked() (map[string]interface{}, error) {
	if r == nil {
		return nil, fmt.Errorf("remoteengine: pipeline result is required")
	}
	flat := flattenResult(r.Raw)
	planJSON, jsonOK := flat[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	planSHA, shaOK := flat[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	if !jsonOK || !shaOK || strings.TrimSpace(planJSON) == "" || strings.TrimSpace(planSHA) == "" {
		return nil, fmt.Errorf("remoteengine: producer-compiled CompiledRenderPlanV2 is required")
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(flat); err != nil {
		return nil, fmt.Errorf("remoteengine: invalid CompiledRenderPlanV2: %w", err)
	}
	projected, err := contract.RenderOnlyPayload(flat)
	if err != nil {
		return nil, fmt.Errorf("project renderer payload: %w", err)
	}
	return projected, nil
}

// flattenResult merges top-level keys with the nested "result" map.
// Mirrors enqueue.FlattenPipelineResult but kept local to avoid a cross-
// package dependency.
func flattenResult(result map[string]interface{}) map[string]interface{} {
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

// jsonString serializes a map to a JSON string, returning "{}" on error.
func jsonString(m map[string]interface{}) string {
	if m == nil {
		return "{}"
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
