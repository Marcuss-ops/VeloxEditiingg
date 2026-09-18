package grpcserver

import (
	"encoding/json"
	"sort"
	"strings"

	"velox-shared/contract"
	"velox-shared/futureasset"
)

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
	var walk func(interface{})
	walk = func(value interface{}) {
		switch node := value.(type) {
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		case map[string]interface{}:
			if rawPlan, ok := node[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
				appendCompiledPlanAssetManifests(rawPlan, seen)
			}
			key, _ := node["asset_key"].(string)
			if key == "" {
				key, _ = node["asset_id"].(string)
			}
			assetID, _ := node["asset_id"].(string)
			if assetID == "" {
				assetID, _ = node["drive_file_id"].(string)
			}
			if key == "" {
				key = assetID
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
			sourceURI, _ := node["source_uri"].(string)
			if sourceURI == "" {
				if rawURL, _ := node["url"].(string); strings.HasPrefix(strings.ToLower(strings.TrimSpace(rawURL)), "velox-drive://") {
					sourceURI = "https://drive.google.com/uc?export=download&id=" + strings.TrimSpace(rawURL[len("velox-drive://"):])
				}
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

func appendCompiledPlanAssetManifests(raw string, seen map[string]futureasset.AssetManifest) {
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
		}
	}
}
