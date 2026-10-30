package grpcserver

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-shared/futureasset"
	"velox-shared/paths"
)

// futureAssetManifestsWithCatalog upgrades source-only Drive references with
// identities learned from an earlier verified prefetch. The payload remains
// authoritative when it already contains SHA-256 metadata.
func (h *Handler) futureAssetManifestsWithCatalog(ctx context.Context, payload []byte) []futureasset.AssetManifest {
	assets := futureAssetManifests(payload)
	if h == nil || h.dbStore == nil {
		return assets
	}
	for i := range assets {
		if assets[i].SHA256 != "" || assets[i].AssetID == "" {
			continue
		}
		identity, found, err := h.dbStore.LookupAssetIdentity(ctx, assets[i].AssetID)
		if err != nil || !found {
			continue
		}
		assets[i].SHA256 = identity.SHA256
		assets[i].SizeBytes = identity.SizeBytes
	}
	return assets
}

// futureAssetManifests extracts all referenced asset manifests from a task
// payload, walking nested JSON to collect {asset_key, sha256, size_bytes}
// triples. The returned list is sorted deterministically by AssetKey for
// stable placement decisions and cache comparisons.
func futureAssetManifests(payload []byte) []futureasset.AssetManifest {
	var root interface{}
	if len(payload) == 0 || json.Unmarshal(payload, &root) != nil {
		return nil
	}
	seen := make(map[string]futureasset.AssetManifest)
	sourceURIs, planAssetKeys := futureAssetRuntimeBindings(root)
	var walk func(interface{})
	walk = func(value interface{}) {
		switch node := value.(type) {
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		case map[string]interface{}:
			if rawPlan, ok := node[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
				appendCompiledPlanAssetManifests(rawPlan, seen, sourceURIs)
			}
			// The canonical worker payload persists typed scenes in
			// scenes_json.  Keep manifest discovery recursive across that
			// boundary: otherwise stock assets survive in the task payload but
			// disappear from the future plan and are downloaded at runtime.
			if rawScenes, ok := node["scenes_json"].(string); ok && strings.TrimSpace(rawScenes) != "" {
				var scenes interface{}
				if err := json.Unmarshal([]byte(rawScenes), &scenes); err == nil {
					walk(scenes)
				}
			}
			key, _ := node["asset_key"].(string)
			assetID, _ := node["asset_id"].(string)
			if assetID == "" {
				assetID, _ = node["drive_file_id"].(string)
			}
			rawURL, _ := node["url"].(string)
			if strings.TrimSpace(rawURL) == "" {
				rawURL, _ = node["drive_link"].(string)
			}
			if assetID == "" {
				if driveID, err := assetref.ParseDriveFileID(rawURL); err == nil {
					assetID = driveID.String()
				}
			}
			sha, _ := node["sha256"].(string)
			if sha == "" {
				sha, _ = node["asset_sha256"].(string)
			}
			var size int64
			switch n := node["size_bytes"].(type) {
			case float64:
				size = int64(n)
			case int64:
				size = n
			}
			if key == "" {
				key = planAssetKeys[assetID]
			}
			if key == "" {
				key = assetID
			}
			sourceURI, _ := node["source_uri"].(string)
			if sourceURI == "" && (strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawURL)), "https://") || strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawURL)), "http://")) {
				sourceURI = strings.TrimSpace(rawURL)
			}
			if sourceURI == "" {
				if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawURL)), "velox-drive://") {
					sourceURI = "https://drive.google.com/uc?export=download&id=" + strings.TrimSpace(rawURL[len("velox-drive://"):])
				} else if driveID, err := assetref.ParseDriveFileID(rawURL); err == nil {
					sourceURI = "https://drive.google.com/uc?export=download&id=" + driveID.String()
				}
			}
			// Producers may provide Drive's human-facing /file/d/.../view
			// locator in source_uri. FutureAssetPlan must carry a byte endpoint;
			// otherwise the worker receives HTML and can never certify the asset.
			if sourceURI != "" {
				sourceURI = paths.NormalizeDriveURL(sourceURI)
			}
			if key != "" && assetID == "" {
				assetID = key
			}
			// A deferred Drive manifest is intentionally not content-addressed
			// yet: the worker owns the direct Drive transfer and computes the
			// immutable SHA-256 before PREPARED. It is admitted only with a
			// trusted source locator and a positive Drive-reported size.
			complete := sha != "" && size > 0
			deferred := sourceURI != "" && assetID != "" && size > 0
			if key != "" && (complete || deferred) {
				role, _ := node["role"].(string)
				seen[key] = futureasset.AssetManifest{AssetKey: key, AssetID: assetID, SHA256: sha, SizeBytes: size, Role: role, SourceURI: sourceURI}
			}
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(root)
	out := make([]futureasset.AssetManifest, 0, len(seen))
	for _, asset := range seen {
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AssetKey < out[j].AssetKey
	})
	return out
}

// futureAssetRuntimeBindings joins FINALIZE's direct byte locators to the
// producer-compiled plan by asset_id. The plan intentionally carries only
// immutable identity; worker-only source URIs remain in runtime_assets.
func futureAssetRuntimeBindings(root interface{}) (map[string]string, map[string]string) {
	sourceURIs := make(map[string]string)
	planAssetKeys := make(map[string]string)
	var walk func(interface{})
	walk = func(value interface{}) {
		switch node := value.(type) {
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		case map[string]interface{}:
			if rawPlan, ok := node[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
				if plan, err := contract.DecodeCompiledRenderPlanV2([]byte(rawPlan)); err == nil && plan != nil {
					for _, asset := range plan.Assets {
						key := asset.AssetKey
						if key == "" {
							key = asset.AssetID
						}
						if asset.AssetID != "" && key != "" {
							planAssetKeys[asset.AssetID] = key
						}
					}
				}
			}
			if runtimeAssets, ok := node["runtime_assets"].([]interface{}); ok {
				for _, raw := range runtimeAssets {
					asset, ok := raw.(map[string]interface{})
					if !ok {
						continue
					}
					id, _ := asset["asset_id"].(string)
					uri, _ := asset["source_uri"].(string)
					if strings.TrimSpace(uri) == "" {
						uri, _ = asset["url"].(string)
						lowerURI := strings.ToLower(strings.TrimSpace(uri))
						if !strings.HasPrefix(lowerURI, "https://") && !strings.HasPrefix(lowerURI, "http://") {
							uri = ""
						}
					}
					if strings.TrimSpace(id) != "" && strings.TrimSpace(uri) != "" {
						sourceURIs[strings.TrimSpace(id)] = strings.TrimSpace(uri)
					}
				}
			}
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(root)
	return sourceURIs, planAssetKeys
}

func appendCompiledPlanAssetManifests(raw string, seen map[string]futureasset.AssetManifest, sourceURIs map[string]string) {
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil || plan == nil {
		return
	}
	for _, asset := range plan.Assets {
		key := asset.AssetKey
		if key == "" {
			key = asset.AssetID
		}
		if key == "" || asset.SHA256 == "" || asset.SizeBytes <= 0 {
			continue
		}
		seen[key] = futureasset.AssetManifest{
			AssetKey: key, AssetID: asset.AssetID, SHA256: asset.SHA256,
			SizeBytes: asset.SizeBytes, MIMEType: asset.MIME, Role: asset.Kind,
			SourceURI: sourceURIs[asset.AssetID],
		}
	}
}
