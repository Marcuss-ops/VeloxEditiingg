package contract

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RenderOnlyPayload returns an immutable renderer-facing copy of payload.
// Control-plane routing, publication metadata, local filesystem references,
// and legacy scene aliases are never allowed past this boundary.
func RenderOnlyPayload(payload map[string]interface{}) (map[string]interface{}, error) {
	if payload == nil {
		return map[string]interface{}{}, nil
	}
	// Reject malformed values before filtering. Silently dropping an invalid
	// value would turn a broken TaskSpec into an apparently valid empty field.
	if _, err := json.Marshal(payload); err != nil {
		return nil, fmt.Errorf("render-only payload contains non-JSON value: %w", err)
	}
	projected := renderOnlyValue(payload).(map[string]interface{})
	if scenes, ok := payload["scenes"]; ok {
		projected["scenes"] = canonicalizeScenesValue(scenes)
	}
	if raw, ok := payload["scenes_json"].(string); ok && strings.TrimSpace(raw) != "" {
		var scenes interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, fmt.Errorf("render-only scenes_json: %w", err)
		}
		canonicalScenes := canonicalizeScenesValue(scenes)
		projected["scenes_json"] = mustRenderOnlyJSON(canonicalScenes)
		if _, present := projected["scenes"]; !present {
			projected["scenes"] = canonicalScenes
		}
	}
	if metadata, ok := projected["video_metadata"].(map[string]interface{}); ok {
		projected["video_metadata"] = rendererTechnicalMetadata(metadata)
		if len(projected["video_metadata"].(map[string]interface{})) == 0 {
			delete(projected, "video_metadata")
		}
	}
	return projected, nil
}

// rendererForbiddenKeys is deliberately broader than the old delivery
// envelope. These keys identify compatibility aliases or control/publication
// data and are forbidden at every nested level of the renderer payload.
var rendererForbiddenKeys = map[string]struct{}{
	"voiceover_paths":       {},
	"subtitle_tracks":       {},
	"clip_link":             {},
	"image_link":            {},
	"local_path":            {},
	"bindings":              {},
	"project_id":            {},
	"render_spec":           {},
	"delivery_plan":         {},
	"delivery_metadata":     {},
	"destinations":          {},
	"delivery_destinations": {},
	"destination_id":        {},
	"destination_ids":       {},
	"publications":          {},
	"publication_specs":     {},
	"publication_metadata":  {},
	"publication":           {},
	"metadata":              {},
	"metadata_override":     {},
	"title":                 {},
	"description":           {},
	"tags":                  {},
	"privacy":               {},
	"privacy_status":        {},
	"publish_at":            {},
	"schedule":              {},
	"scheduling":            {},
	"localizations":         {},
}

func renderOnlyValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			if _, forbidden := rendererForbiddenKeys[key]; forbidden {
				continue
			}
			if key == "video_metadata" {
				if metadata, ok := item.(map[string]interface{}); ok {
					out[key] = rendererTechnicalMetadata(metadata)
				}
				continue
			}
			out[key] = renderOnlyValue(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(typed))
		for _, item := range typed {
			out = append(out, renderOnlyValue(item))
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(typed))
		for _, item := range typed {
			out = append(out, renderOnlyValue(item).(map[string]interface{}))
		}
		return out
	default:
		return value
	}
}

func canonicalizeScenesValue(value interface{}) interface{} {
	switch scenes := value.(type) {
	case []interface{}:
		out := make([]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, canonicalizeSceneValue(scene))
		}
		return out
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, canonicalizeSceneValue(scene).(map[string]interface{}))
		}
		return out
	default:
		return renderOnlyValue(value)
	}
}

func canonicalizeSceneValue(value interface{}) interface{} {
	scene, ok := value.(map[string]interface{})
	if !ok {
		return renderOnlyValue(value)
	}
	out := make(map[string]interface{}, len(scene)+3)
	for key, item := range scene {
		out[key] = item
	}
	if _, hasClip := out["clip"]; !hasClip {
		if url, ok := out["clip_link"].(string); ok && strings.TrimSpace(url) != "" {
			out["clip"] = canonicalAsset(url, out["duration_seconds"])
		}
	}
	if _, hasImage := out["image"]; !hasImage {
		if url, ok := out["image_link"].(string); ok && strings.TrimSpace(url) != "" {
			out["image"] = canonicalAsset(url, nil)
		}
	}
	return renderOnlyValue(out)
}

func canonicalAsset(url string, duration interface{}) map[string]interface{} {
	asset := map[string]interface{}{"url": strings.TrimSpace(url)}
	if strings.HasPrefix(asset["url"].(string), "velox-asset://") {
		asset["asset_id"] = strings.TrimPrefix(asset["url"].(string), "velox-asset://")
	}
	if duration != nil {
		switch value := duration.(type) {
		case float64:
			if value > 0 {
				asset["duration_ms"] = int64(value * 1000)
			}
		case int:
			if value > 0 {
				asset["duration_ms"] = value * 1000
			}
		}
	}
	return asset
}

func mustRenderOnlyJSON(value interface{}) string {
	encoded, err := json.Marshal(renderOnlyValue(value))
	if err != nil {
		return "null"
	}
	return string(encoded)
}

func rendererTechnicalMetadata(metadata map[string]interface{}) map[string]interface{} {
	allowed := map[string]struct{}{
		"width": {}, "height": {}, "fps_num": {}, "fps_den": {},
		"pixel_format": {}, "sample_rate": {}, "audio_sample_rate": {},
		"audio_channels": {}, "video_codec": {}, "audio_codec": {},
	}
	out := make(map[string]interface{})
	for key, value := range metadata {
		if _, ok := allowed[key]; ok {
			out[key] = renderOnlyValue(value)
		}
	}
	return out
}
