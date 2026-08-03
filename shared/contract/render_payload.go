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
	if raw, ok := projected["scenes_json"].(string); ok && strings.TrimSpace(raw) != "" {
		var scenes interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, fmt.Errorf("render-only scenes_json: %w", err)
		}
		projected["scenes_json"] = mustRenderOnlyJSON(scenes)
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
