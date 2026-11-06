package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

func compiledPlanHasDriveStock(planJSON string, runtimeAssets ...[]map[string]interface{}) bool {
	var plan contract.CompiledRenderPlanV2
	if strings.TrimSpace(planJSON) == "" || json.Unmarshal([]byte(planJSON), &plan) != nil || plan.StockPool != nil {
		return false
	}
	for _, asset := range plan.Assets {
		if asset.Kind == "video" && (strings.EqualFold(strings.TrimSpace(asset.SourceKind), "drive") || strings.TrimSpace(asset.DriveFileID) != "" || strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:")) {
			return true
		}
	}
	planAssetIDs := make(map[string]struct{}, len(plan.Assets)*3)
	for _, asset := range plan.Assets {
		for _, id := range []string{asset.AssetID, asset.AssetKey, asset.SHA256} {
			if id = strings.TrimSpace(id); id != "" {
				planAssetIDs[id] = struct{}{}
			}
		}
	}
	for _, bindings := range runtimeAssets {
		for _, binding := range bindings {
			role, _ := binding["role"].(string)
			if !strings.EqualFold(strings.TrimSpace(role), "stock") || !driveRuntimeBindingMap(binding) {
				continue
			}
			for _, key := range []string{"asset_id", "asset_key", "sha256", "asset_sha256"} {
				if _, found := planAssetIDs[strings.TrimSpace(fmt.Sprint(binding[key]))]; found {
					return true
				}
			}
		}
	}
	return false
}

func driveRuntimeBindingMap(binding map[string]interface{}) bool {
	if binding == nil {
		return false
	}
	if value, _ := binding["drive_file_id"].(string); strings.TrimSpace(value) != "" {
		return true
	}
	if value, _ := binding["source_kind"].(string); strings.EqualFold(strings.TrimSpace(value), "drive") {
		return true
	}
	for _, key := range []string{"source_uri", "url", "drive_link"} {
		value, _ := binding[key].(string)
		lower := strings.ToLower(strings.TrimSpace(value))
		if strings.Contains(lower, "drive.google.com/") || strings.Contains(lower, "drive.usercontent.google.com/") || strings.HasPrefix(lower, "velox-drive://") {
			return true
		}
	}
	return false
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

func removePreparedStockBindings(assets []map[string]interface{}) []map[string]interface{} {
	filtered := make([]map[string]interface{}, 0, len(assets))
	for _, asset := range assets {
		role, _ := asset["role"].(string)
		driveFileID, _ := asset["drive_file_id"].(string)
		sourceKind, _ := asset["source_kind"].(string)
		sourceURI, _ := asset["source_uri"].(string)
		driveLink, _ := asset["drive_link"].(string)
		url, _ := asset["url"].(string)
		driveBacked := strings.TrimSpace(driveFileID) != "" || strings.EqualFold(strings.TrimSpace(sourceKind), "drive") ||
			strings.Contains(strings.ToLower(sourceURI), "drive.google.com/") || strings.Contains(strings.ToLower(sourceURI), "drive.usercontent.google.com/") ||
			strings.Contains(strings.ToLower(driveLink), "drive.google.com/") || strings.HasPrefix(strings.ToLower(strings.TrimSpace(url)), "velox-drive://")
		if strings.EqualFold(strings.TrimSpace(role), "stock") && driveBacked {
			continue
		}
		filtered = append(filtered, asset)
	}
	return filtered
}
