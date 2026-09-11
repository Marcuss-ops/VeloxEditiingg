package renderplan

import (
	"fmt"
	"strings"

	"velox-shared/contract"
)

// ValidateTaskPayload is the worker admission router. New tasks must carry
// render_plan_version=v2. The only compatibility path is an explicitly
// versioned legacy RenderPlan v1 payload; unversioned or unknown payloads
// fail closed instead of being interpreted by guessing among old keys.
func ValidateTaskPayload(raw map[string]interface{}) error {
	if raw == nil {
		return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "payload is required")
	}
	// The native packet-copy executor has its own strict, producer-compiled
	// contract. Keep it out of the legacy RenderPlan adapter: otherwise the
	// shared CompiledRenderPlanV2 document can be misread as the older mirror
	// and rejected (or, worse, interpreted through a second timeline shape).
	if isNativePacketCopyExecutor(raw["executor_id"]) {
		for _, legacyKey := range []string{"render_plan_version", "render_plan", "render_plan_json"} {
			if _, present := raw[legacyKey]; present {
				return planError(ERR_PLAN_SCHEMA, legacyKey, "legacy render-plan envelope is not allowed for the native packet-copy executor")
			}
		}
		if _, present := raw[contract.PayloadKeyCompiledRenderPlanJSON]; !present {
			return planError(ERR_PLAN_REQUIRED_FIELD, contract.PayloadKeyCompiledRenderPlanJSON, "is required for the native packet-copy executor")
		}
		if _, present := raw[contract.PayloadKeyCompiledRenderPlanSHA]; !present {
			return planError(ERR_PLAN_REQUIRED_FIELD, contract.PayloadKeyCompiledRenderPlanSHA, "is required for the native packet-copy executor")
		}
		if err := contract.ValidateCompiledRenderPlanV2Payload(raw); err != nil {
			return fmt.Errorf("native packet-copy render plan v2: %w", err)
		}
		return nil
	}
	if _, present := raw["render_plan_version"]; present {
		return ValidateVersionedRenderPlan(raw)
	}
	// Master-compiled render plan (Fase D): when the TaskOffer payload
	// carries the canonical CompiledRenderPlan document, validate it
	// strictly. Additive to the legacy payload_contract_version envelope
	// below — payloads without the compiled plan are untouched.
	if err := ValidateCompiledRenderPlan(raw); err != nil {
		return err
	}
	// The current master emits payload_contract_version while the fleet
	// migrates to the compiled RenderPlan envelope. This is still an
	// explicitly versioned compatibility path and is temporary by design.
	if version, ok := numericInt(raw["payload_contract_version"]); ok && version > 0 {
		return validateLegacyPayloadContract(raw, version)
	}
	if version, ok := raw["version"].(string); ok {
		if version == LegacyRenderPlanVersion {
			return validateLegacyV1Payload(raw)
		}
		return planError(ERR_PLAN_UNSUPPORTED_VERSION, "version", fmt.Sprintf("unsupported legacy version %q", version))
	}
	return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "must be declared; legacy payloads require an explicit version")
}

func isNativePacketCopyExecutor(value interface{}) bool {
	id, ok := value.(string)
	if !ok {
		return false
	}
	return id == "video.assemble.copy.v1" || strings.HasPrefix(id, "video.assemble.copy.v1@")
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
