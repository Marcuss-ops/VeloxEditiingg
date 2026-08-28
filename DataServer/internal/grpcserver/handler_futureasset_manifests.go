package grpcserver

import (
	"encoding/json"
	"sort"

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
			if key != "" && sha != "" && size > 0 {
				role, _ := node["role"].(string)
				seen[key] = futureasset.AssetManifest{AssetKey: key, AssetID: key, SHA256: sha, SizeBytes: size, Role: role}
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
