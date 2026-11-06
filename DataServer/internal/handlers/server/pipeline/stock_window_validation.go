package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// validateStockPacketCopyWindows rejects Drive stock segments that ask the
// packet-copy worker to take only part of a source file. The worker does not
// seek inside stock clips or encode them, so accepting such a plan only defers
// a deterministic validation failure until after placement.
func validateStockPacketCopyWindows(planJSON string, runtimeAssets []map[string]interface{}, runtimePayload map[string]interface{}) error {
	if strings.TrimSpace(planJSON) == "" {
		return nil
	}
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return nil // The canonical V2 validator returns the structured decode error.
	}
	if plan.StockPool != nil {
		return nil // Stock-pool slots are whole-source selections by contract.
	}

	assets := append([]map[string]interface{}(nil), runtimeAssets...)
	if raw, ok := runtimePayload["runtime_assets"]; ok {
		encoded, err := json.Marshal(raw)
		if err != nil {
			return fmt.Errorf("runtime_assets cannot be inspected: %w", err)
		}
		var nested []map[string]interface{}
		if err := json.Unmarshal(encoded, &nested); err != nil {
			return fmt.Errorf("runtime_assets must be an array of bindings: %w", err)
		}
		assets = append(assets, nested...)
	}

	stockIDs := make(map[string]struct{}, len(assets)*3)
	durationUSByID := make(map[string]int64, len(assets)*3)
	for _, asset := range assets {
		role, _ := asset["role"].(string)
		if !strings.EqualFold(strings.TrimSpace(role), "stock") {
			continue
		}
		durationUS := runtimeAssetDurationUS(asset)
		for _, key := range []string{"asset_id", "asset_key", "drive_file_id", "sha256"} {
			id, _ := asset[key].(string)
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			stockIDs[id] = struct{}{}
			if durationUS > 0 {
				durationUSByID[id] = durationUS
			}
		}
	}

	planAssets := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		planAssets[asset.AssetID] = asset
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if segment.StockPoolSlot {
				continue
			}
			asset, ok := planAssets[segment.AssetID]
			if !ok {
				continue // Canonical plan validation reports the missing asset.
			}
			_, runtimeStock := stockIDs[segment.AssetID]
			isDriveStock := asset.Kind == "video" && (runtimeStock || strings.EqualFold(asset.SourceKind, "drive") || strings.TrimSpace(asset.DriveFileID) != "" || strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:"))
			if !isDriveStock {
				continue
			}

			durationUS := asset.DurationUS
			for _, identity := range []string{segment.AssetID, asset.AssetKey, asset.DriveFileID, asset.SHA256} {
				if known := durationUSByID[strings.TrimSpace(identity)]; known > 0 {
					durationUS = known
					break
				}
			}
			switch {
			case segment.SourceInUS != 0:
				return fmt.Errorf("stock segment %q is not a whole-file packet-copy window: source_in_us=%d; expected 0", segment.SegmentID, segment.SourceInUS)
			case durationUS <= 0:
				return fmt.Errorf("stock segment %q cannot be certified for packet-copy: source duration metadata is missing", segment.SegmentID)
			case segment.SourceDurationUS != durationUS:
				return fmt.Errorf("stock segment %q is not a whole-file packet-copy window: source_duration_us=%d, file_duration_us=%d", segment.SegmentID, segment.SourceDurationUS, durationUS)
			case asset.FrameCount <= 0 || segment.FrameCount != asset.FrameCount:
				return fmt.Errorf("stock segment %q is not a whole-file packet-copy window: segment frame_count=%d, certified file frame_count=%d", segment.SegmentID, segment.FrameCount, asset.FrameCount)
			case asset.ProfileID == "" || asset.ProfileID != plan.Output.ProfileID:
				return fmt.Errorf("stock segment %q has a divergent certified profile: asset=%q output=%q", segment.SegmentID, asset.ProfileID, plan.Output.ProfileID)
			case !asset.FirstFrameKeyframe || !asset.ClosedGOP:
				return fmt.Errorf("stock segment %q is not certified for packet-copy: first_frame_keyframe=%t closed_gop=%t", segment.SegmentID, asset.FirstFrameKeyframe, asset.ClosedGOP)
			}
		}
	}
	return nil
}

func runtimeAssetDurationUS(asset map[string]interface{}) int64 {
	if value := runtimeAssetInt64(asset["duration_us"]); value > 0 {
		return value
	}
	if value := runtimeAssetInt64(asset["duration_ms"]); value > 0 {
		return value * 1000
	}
	return 0
}

func runtimeAssetInt64(value interface{}) int64 {
	switch n := value.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case json.Number:
		parsed, _ := n.Int64()
		return parsed
	case string:
		var parsed int64
		_, _ = fmt.Sscan(strings.TrimSpace(n), &parsed)
		return parsed
	default:
		return 0
	}
}
