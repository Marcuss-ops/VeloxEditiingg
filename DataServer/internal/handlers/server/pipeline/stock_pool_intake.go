package pipeline

import (
	"context"
	"fmt"

	"velox-shared/assetref"
	"velox-shared/contract"
)

// resolveStockPoolRuntimeAssets is the one V2 folder-resolution boundary.
// It lists metadata only; media bytes remain worker-owned transfers.
func (h *Handlers) resolveStockPoolRuntimeAssets(ctx context.Context, req *SubmitJobRequest) error {
	if req == nil || req.CompiledRenderPlanJSON == "" {
		return nil
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(req.CompiledRenderPlanJSON))
	if err != nil {
		return nil // Canonical plan validation returns the structured V2 error later.
	}
	if plan.StockPool == nil {
		return nil
	}
	if h.stockFolderLister == nil {
		return fmt.Errorf("stock_pool requires the Master Drive folder-listing capability")
	}
	folderURL := "https://drive.google.com/drive/folders/" + plan.StockPool.DriveFolderID
	folderID, err := assetref.ParseDriveFolderID(folderURL)
	if err != nil {
		return err
	}
	files, err := listStockFolderVideos(ctx, h.stockFolderLister, folderID.String(), 0, make(map[string]struct{}))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("stock_pool Drive folder contains no video files")
	}
	runtimeAssets := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	seen := make(map[string]struct{}, len(runtimeAssets)+len(files))
	for _, asset := range runtimeAssets {
		if id, ok := asset["asset_id"].(string); ok {
			seen[id] = struct{}{}
		}
	}
	for _, file := range files {
		if _, exists := seen[file.ID]; exists {
			continue
		}
		asset := map[string]interface{}{
			"asset_id": file.ID, "asset_key": file.ID, "drive_file_id": file.ID,
			"url":        "velox-drive://" + file.ID,
			"source_uri": "https://drive.google.com/uc?export=download&id=" + file.ID,
			"kind":       "video", "stock_pool_member": true,
		}
		if file.Size > 0 {
			asset["size_bytes"] = file.Size
		}
		if file.VideoMediaMetadata.DurationMillis > 0 {
			asset["duration_ms"] = file.VideoMediaMetadata.DurationMillis
		}
		runtimeAssets = append(runtimeAssets, asset)
		seen[file.ID] = struct{}{}
	}
	req.RuntimeAssets = runtimeAssets
	// Ensure downstream payload projection sees the stable snapshot. The plan
	// remains byte-for-byte hash-bound; retries reuse the accepted runtime list.
	return nil
}
