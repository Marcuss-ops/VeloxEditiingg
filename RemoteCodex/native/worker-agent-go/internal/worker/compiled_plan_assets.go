package worker

import (
	"context"
	"fmt"
	"os"
	"strings"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/runtimeassets"
)

// resolveCompiledRenderPlanAssets is an adapter/orchestrator around the
// existing common asset resolver. It deliberately does not download bytes or
// write cache rows itself: resolveCommonAssetPayload owns that verified path.
//
// The synthetic payload exists only in memory so the common resolver can
// consume the typed V2 references. Local paths returned by that resolver are
// projected into runtimeassets.Bindings and never written back into the
// canonical CompiledRenderPlanV2 JSON.
func (w *Worker) resolveCompiledRenderPlanAssets(ctx context.Context, payload map[string]interface{}) (runtimeassets.Bindings, error) {
	if payload == nil {
		return nil, fmt.Errorf("compiled render plan v2: payload is required")
	}
	raw, ok := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("compiled render plan v2: %q is required", contract.PayloadKeyCompiledRenderPlanJSON)
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: decode: %w", err)
	}

	// This structure is an adapter input, not a replacement plan. The
	// resolver understands the existing asset envelope and returns verified
	// worker-local paths for each uri. AssetKey is the canonical cache/transfer
	// identity when supplied; AssetID remains the stable plan/binding identity.
	assetEnvelopes := make([]interface{}, 0, len(plan.Assets))
	seenIdentities := make(map[string]string, len(plan.Assets))
	for _, asset := range plan.Assets {
		identity := compiledAssetIdentity(asset)
		if identity == "" {
			return nil, fmt.Errorf("compiled render plan v2: asset %q has no canonical identity", asset.AssetID)
		}
		if previous, exists := seenIdentities[identity]; exists && previous != asset.AssetID {
			return nil, fmt.Errorf("compiled render plan v2: assets %q and %q share canonical identity %q", previous, asset.AssetID, identity)
		}
		seenIdentities[identity] = asset.AssetID
		assetEnvelopes = append(assetEnvelopes, map[string]interface{}{
			"asset_id":   identity,
			"uri":        compiledAssetReference(asset),
			"kind":       asset.Kind,
			"sha256":     asset.SHA256,
			"size_bytes": asset.SizeBytes,
		})
	}
	adapterPayload := map[string]interface{}{"assets": assetEnvelopes}
	resolvedPayload, err := w.resolveCommonAssetPayload(ctx, adapterPayload)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: resolve assets: %w", err)
	}

	resolvedAssets, ok := resolvedPayload["assets"].([]interface{})
	if !ok || len(resolvedAssets) != len(plan.Assets) {
		return nil, fmt.Errorf("compiled render plan v2: resolver returned %d assets, want %d", len(resolvedAssets), len(plan.Assets))
	}
	bindings := make(runtimeassets.Bindings, len(plan.Assets))
	for index, asset := range plan.Assets {
		resolved, ok := resolvedAssets[index].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("compiled render plan v2: resolved assets[%d] is %T, want object", index, resolvedAssets[index])
		}
		path, ok := resolved["uri"].(string)
		if !ok || strings.TrimSpace(path) == "" || strings.HasPrefix(path, "velox-") {
			return nil, fmt.Errorf("compiled render plan v2: assets[%d] did not resolve to a local path", index)
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			if err == nil {
				err = fmt.Errorf("file is empty or not regular")
			}
			return nil, fmt.Errorf("compiled render plan v2: assets[%d] local path invalid: %w", index, err)
		}
		bindings[asset.AssetID] = runtimeassets.Binding{
			AssetID: asset.AssetID,
			Path:    path,
			SHA256:  asset.SHA256,
			Size:    asset.SizeBytes,
		}
	}
	return bindings, nil
}

// compiledAssetIdentity is the one V2-to-cache identity rule. AssetKey is
// optional for backwards-compatible plans; when omitted, AssetID is the
// canonical cache key as well as the runtime binding key.
func compiledAssetIdentity(asset contract.AssetRefV2) string {
	identity := strings.TrimSpace(asset.AssetKey)
	if identity == "" {
		identity = strings.TrimSpace(asset.AssetID)
	}
	if wireID, ok := assetref.WireAssetID(identity); ok {
		return wireID
	}
	return identity
}

func compiledAssetReference(asset contract.AssetRefV2) string {
	raw := strings.TrimSpace(asset.AssetKey)
	if raw == "" {
		raw = strings.TrimSpace(asset.AssetID)
	}
	if _, ok := assetref.WireAssetID(raw); ok {
		return raw
	}
	return assetref.SchemeVeloxAsset + "://" + compiledAssetIdentity(asset)
}
