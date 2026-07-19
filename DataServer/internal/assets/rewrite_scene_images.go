package assets

import (
	"encoding/json"

	"velox-shared/payload"
)

// rewrite_scene_images.go owns the scene-image-specific payload
// collector and applicator. Pure payload navigators — no DB, no
// BlobStore, no SHA-256, no resolver registry. The shared applyRewrite
// orchestrator (in payload_rewrite.go) wires these helpers to
// ResolveAndRegister.

// collectSceneImageReferences returns the canonical scene-image
// reference list extracted from a payload, including image_link /
// image_links entries inside scenes and the top-level scene_image_paths.
func collectSceneImageReferences(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	var candidates []string

	// PR15.6: scene_image_paths is the canonical input.
	if v, ok := payloadMap["scene_image_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	// From scenes array — extract image_link from each scene entry.
	// Handles both []map[string]interface{} (normalized payload) and
	// []interface{} (raw JSON from json.Unmarshal).
	switch scenes := payloadMap["scenes"].(type) {
	case []map[string]interface{}:
		for _, scene := range scenes {
			if img, ok := scene["image_link"].(string); ok {
				candidates = append(candidates, img)
			}
			if imgs, ok := scene["image_links"].([]string); ok {
				candidates = append(candidates, imgs...)
			}
		}
	case []interface{}:
		for _, item := range scenes {
			if scene, ok := item.(map[string]interface{}); ok {
				if img, ok := scene["image_link"].(string); ok {
					candidates = append(candidates, img)
				}
				if imgs, ok := scene["image_links"].([]string); ok {
					candidates = append(candidates, imgs...)
				}
			}
		}
	}

	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		if v, ok := params["scene_image_paths"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
		}
	}

	return payload.DedupeStrings(candidates)
}

// applySceneImageReferences writes the canonical scene_image_paths
// array back to the payload, AND mirrors per-scene rewrites
// (image_link + image_links) AND the parameters sub-map.
//
// PR15.6 + refactor/payload-v2-single-shape: writes ONLY canonical
// keys; no legacy alias keys are written AND the legacy `parameters`
// sub-map mirror is no longer written either — top-level keys are
// the single source of truth. Any legacy `parameters` mirror present
// on the input is left untouched so the round-trip for old rows is
// preserved.
func applySceneImageReferences(payloadMap map[string]interface{}, refs []string) {
	if len(refs) == 0 || payloadMap == nil {
		return
	}

	payloadMap["scene_image_paths"] = append([]string(nil), refs...)

	// Keep every scene representation worker-visible. In particular, the
	// engine consumes items[].url and some legacy payloads still carry a JSON
	// encoded scenes_json string. Leaving either one as file:// would make the
	// remote worker try to open Computer A's filesystem.
	if items, ok := payloadMap["items"].([]interface{}); ok {
		for i, item := range items {
			if i >= len(refs) {
				break
			}
			if entry, ok := item.(map[string]interface{}); ok {
				entry["url"] = refs[i]
			}
		}
	} else if items, ok := payloadMap["items"].([]map[string]interface{}); ok {
		for i, item := range items {
			if i < len(refs) {
				item["url"] = refs[i]
			}
		}
	}
	if images, ok := payloadMap["images"].([]interface{}); ok {
		for i := range images {
			if i < len(refs) {
				images[i] = refs[i]
			}
		}
	} else if images, ok := payloadMap["images"].([]string); ok {
		for i := range images {
			if i < len(refs) {
				images[i] = refs[i]
			}
		}
	}
	if encoded, ok := payloadMap["scenes_json"].(string); ok && encoded != "" {
		var scenes []map[string]interface{}
		if json.Unmarshal([]byte(encoded), &scenes) == nil {
			for i, scene := range scenes {
				if i >= len(refs) {
					break
				}
				scene["image"] = refs[i]
				scene["image_link"] = refs[i]
				scene["image_links"] = []string{refs[i]}
			}
			if rewritten, err := json.Marshal(scenes); err == nil {
				payloadMap["scenes_json"] = string(rewritten)
			}
		}
	}

	switch scenes := payloadMap["scenes"].(type) {
	case []map[string]interface{}:
		for i, scene := range scenes {
			if i < len(refs) {
				scene["image"] = refs[i]
				scene["image_link"] = refs[i]
				scene["image_links"] = []string{refs[i]}
			}
		}
	case []interface{}:
		for i, item := range scenes {
			if i < len(refs) {
				if scene, ok := item.(map[string]interface{}); ok {
					scene["image"] = refs[i]
					scene["image_link"] = refs[i]
					scene["image_links"] = []string{refs[i]}
				}
			}
		}
	}
}
