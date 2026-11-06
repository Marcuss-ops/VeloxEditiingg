package contract

import (
	"encoding/json"
	"strings"
)

const (
	VideoAssemblyCopyExecutorID  = "video.assemble.copy.v1"
	VideoAssemblySliceExecutorID = "video.assemble.slice.v1"
)

// RequiredVideoAssemblyExecutor is the single routing rule for compiled video
// plans. Drive-backed video and stock-pool plans use the worker's source
// resolver; plans containing only already-certified assets use packet-copy.
func RequiredVideoAssemblyExecutor(payload map[string]interface{}) string {
	planJSON, _ := payload[PayloadKeyCompiledRenderPlanJSON].(string)
	var plan CompiledRenderPlanV2
	if strings.TrimSpace(planJSON) == "" || json.Unmarshal([]byte(planJSON), &plan) != nil {
		return ""
	}
	if plan.StockPool != nil {
		return VideoAssemblySliceExecutorID
	}
	// Two-stage submissions choose an executor before runtime asset bindings
	// arrive. Keep them on the source-aware path so FINALIZE can provide Drive
	// stock without leaving the task pinned to copy-only.
	if pending, _ := payload["runtime_assets_pending"].(bool); pending || compiledRuntimeHasStockRole(payload["runtime_assets"]) {
		return VideoAssemblySliceExecutorID
	}
	runtimeDriveIDs := compiledRuntimeDriveIDs(payload["runtime_assets"])
	for _, asset := range plan.Assets {
		if asset.Kind != "video" {
			continue
		}
		if asset.SourceKind == "drive" || strings.TrimSpace(asset.DriveFileID) != "" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:") {
			return VideoAssemblySliceExecutorID
		}
		if runtimeDriveIDs[asset.AssetID] != "" || runtimeDriveIDs[asset.AssetKey] != "" {
			return VideoAssemblySliceExecutorID
		}
	}
	return VideoAssemblyCopyExecutorID
}

func compiledRuntimeHasStockRole(raw interface{}) bool {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return false
	}
	var assets []struct {
		Role string `json:"role"`
	}
	if json.Unmarshal(encoded, &assets) != nil {
		return false
	}
	for _, asset := range assets {
		if strings.EqualFold(strings.TrimSpace(asset.Role), "stock") {
			return true
		}
	}
	return false
}

func compiledRuntimeDriveIDs(raw interface{}) map[string]string {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return nil
	}
	var assets []struct {
		AssetID     string `json:"asset_id"`
		AssetKey    string `json:"asset_key"`
		DriveFileID string `json:"drive_file_id"`
	}
	if json.Unmarshal(encoded, &assets) != nil {
		return nil
	}
	byIdentity := make(map[string]string, len(assets)*2)
	for _, asset := range assets {
		fileID := strings.TrimSpace(asset.DriveFileID)
		if fileID == "" {
			continue
		}
		for _, key := range []string{asset.AssetID, asset.AssetKey} {
			if key = strings.TrimSpace(key); key != "" {
				byIdentity[key] = fileID
			}
		}
	}
	return byIdentity
}
