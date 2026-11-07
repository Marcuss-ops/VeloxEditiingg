package pipeline

import (
	"encoding/json"
	"fmt"

	voiceoverassets "velox-server/internal/assets"
)

func compiledPlanHasDriveStock(planJSON string, runtimeAssets ...[]map[string]interface{}) bool {
	var bindings []map[string]interface{}
	for _, group := range runtimeAssets {
		bindings = append(bindings, group...)
	}
	return voiceoverassets.PlanHasDriveStock(planJSON, bindings)
}

func appendRuntimeAssetBindings(assets []map[string]interface{}, raw interface{}) ([]map[string]interface{}, error) {
	if raw == nil {
		return assets, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("runtime_assets cannot be inspected: %w", err)
	}
	var nested []map[string]interface{}
	if err := json.Unmarshal(encoded, &nested); err != nil {
		return nil, fmt.Errorf("runtime_assets must be an array of bindings: %w", err)
	}
	return append(assets, nested...), nil
}

func retainPlanSourceBindings(planJSON string, assets []map[string]interface{}) ([]map[string]interface{}, error) {
	return voiceoverassets.RetainPlanSourceBindings(planJSON, assets)
}
