package renderplan

import "fmt"

// ValidateTaskPayload is the worker admission router. New tasks must carry
// render_plan_version=v2. The only compatibility path is an explicitly
// versioned legacy RenderPlan v1 payload; unversioned or unknown payloads
// fail closed instead of being interpreted by guessing among old keys.
func ValidateTaskPayload(raw map[string]interface{}) error {
	if raw == nil {
		return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "payload is required")
	}
	if _, present := raw["render_plan_version"]; present {
		return ValidateVersionedRenderPlan(raw)
	}
	// The current master emits payload_contract_version while the fleet
	// migrates to the compiled RenderPlan envelope. This is still an
	// explicitly versioned compatibility path and is temporary by design.
	if version, ok := numericInt(raw["payload_contract_version"]); ok && version > 0 {
		return validateLegacyPayloadContract(raw, version)
	}
	// Check the explicit payload contract before the historical `version`
	// field. Canonical V2 payloads intentionally carry both
	// payload_contract_version=2 and version="v2"; treating the latter as
	// a legacy RenderPlan version would reject every current master task.
	if version, ok := raw["version"].(string); ok {
		if version == LegacyRenderPlanVersion {
			return validateLegacyV1Payload(raw)
		}
		return planError(ERR_PLAN_UNSUPPORTED_VERSION, "version", fmt.Sprintf("unsupported legacy version %q", version))
	}
	return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "must be declared; legacy payloads require an explicit version")
}

// validateLegacyV1Payload is the temporary, versioned compatibility adapter.
// It deliberately delegates to the pre-existing legacy rules and contains
// no independent key-discovery logic. Remove this function when v1 workers
// have drained from the fleet.
func validateLegacyV1Payload(raw map[string]interface{}) error {
	plan := FromMap(raw)
	if err := ValidateRenderPlan(plan); err != nil {
		return fmt.Errorf("legacy render plan v1: %w", err)
	}
	return nil
}

func validateLegacyPayloadContract(raw map[string]interface{}, version int) error {
	if version < 1 {
		return planError(ERR_PLAN_UNSUPPORTED_VERSION, "payload_contract_version", fmt.Sprintf("unsupported version %d", version))
	}
	if raw["job_id"] == nil || raw["job_type"] == nil || raw["created_at"] == nil {
		return planError(ERR_PLAN_REQUIRED_FIELD, "job_id", "legacy payload is missing required identity fields")
	}
	return nil
}
