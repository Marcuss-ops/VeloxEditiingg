package renderplan

import (
	"fmt"
	"strings"
)

const (
	// LegacyRenderPlanVersion is the only version handled by the temporary
	// compatibility adapter.
	LegacyRenderPlanVersion = "v1"
	// CanonicalRenderPlanVersion is the strict worker execution contract.
	CanonicalRenderPlanVersion = "v2"
)

// VersionedRenderPlan is the worker-facing, already-compiled execution plan.
// The worker must not infer a timeline from arbitrary legacy keys once this
// contract is selected.
type VersionedRenderPlan struct {
	RenderPlanVersion string                 `json:"render_plan_version"`
	ExecutorID        string                 `json:"executor_id"`
	ExecutorVersion   int                    `json:"executor_version"`
	Assets            []interface{}          `json:"assets"`
	Timeline          []interface{}          `json:"timeline"`
	OutputContract    map[string]interface{} `json:"output_contract"`
}

// ValidateVersionedRenderPlan validates the strict v2 worker contract.
// It intentionally checks the boundary shape only; asset hashes, timeline
// references, and output details are validated by the master compiler and
// executor-specific validation after admission.
func ValidateVersionedRenderPlan(raw map[string]interface{}) error {
	if raw == nil {
		return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "payload is required")
	}

	version, ok := raw["render_plan_version"].(string)
	if !ok || strings.TrimSpace(version) == "" {
		return planError(ERR_PLAN_REQUIRED_FIELD, "render_plan_version", "is required")
	}
	if version != CanonicalRenderPlanVersion {
		return planError(ERR_PLAN_UNSUPPORTED_VERSION, "render_plan_version", fmt.Sprintf("unsupported version %q", version))
	}

	executorID, ok := raw["executor_id"].(string)
	if !ok || strings.TrimSpace(executorID) == "" {
		return planError(ERR_PLAN_REQUIRED_FIELD, "executor_id", "is required")
	}

	executorVersion, ok := numericInt(raw["executor_version"])
	if !ok || executorVersion <= 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "executor_version", "must be a positive integer")
	}

	assets, ok := raw["assets"].([]interface{})
	if !ok || len(assets) == 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "assets", "must be a non-empty array")
	}

	timeline, ok := raw["timeline"].([]interface{})
	if !ok || len(timeline) == 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "timeline", "must be a non-empty array")
	}

	outputContract, ok := raw["output_contract"].(map[string]interface{})
	if !ok || len(outputContract) == 0 {
		return planError(ERR_PLAN_REQUIRED_FIELD, "output_contract", "must be a non-empty object")
	}

	return nil
}

func numericInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), v == float64(int(v))
	case float32:
		return int(v), v == float32(int(v))
	default:
		return 0, false
	}
}

func planError(code ErrorCode, field, message string) *PlanError {
	return &PlanError{Code: code, Field: field, Message: message}
}
