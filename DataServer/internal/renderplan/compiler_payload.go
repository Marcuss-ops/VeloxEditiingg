package renderplan

// compiler_payload.go: local, dependency-light payload parsing helpers for
// the RenderPlanCompiler. Kept pure so the compiler stays free of downloads,
// filesystem, and ffprobe.

import (
	"encoding/json"
	"strconv"
	"strings"

	"velox-shared/assetref"
)

// extractScenes reads the canonical scenes array or scenes_json string.
func extractScenes(payload map[string]interface{}) []map[string]interface{} {
	if raw, present := payload["scenes"]; present {
		if scenes := sliceMaps(raw); len(scenes) > 0 {
			return scenes
		}
	}
	if raw := strings.TrimSpace(strParam(payload, "scenes_json")); raw != "" {
		var scenes []map[string]interface{}
		if json.Unmarshal([]byte(raw), &scenes) == nil && len(scenes) > 0 {
			return scenes
		}
		var generic []interface{}
		if json.Unmarshal([]byte(raw), &generic) == nil {
			out := make([]map[string]interface{}, 0, len(generic))
			for _, item := range generic {
				if m, ok := item.(map[string]interface{}); ok {
					out = append(out, m)
				}
			}
			return out
		}
	}
	return nil
}

// durationDeclared reads the payload-declared total duration (seconds).
func durationDeclared(payload map[string]interface{}) int64 {
	return secondsToMS(floatParam(payload, "total_duration_secs"))
}

// assetIDOf extracts the canonical asset id from a map carrying a wire
// reference under one of the given keys (velox-asset:// or velox-drive://),
// or from an explicit asset_id field.
func assetIDOf(m map[string]interface{}, urlKeys ...string) (string, bool) {
	for _, key := range urlKeys {
		if id, ok := assetref.WireAssetID(strParam(m, key)); ok {
			return id, true
		}
	}
	if bare := strings.TrimSpace(strParam(m, "asset_id")); bare != "" {
		return bare, true
	}
	return "", false
}

func nonEmptySlice(value interface{}) bool {
	return len(sliceMaps(value)) > 0
}

func sliceMaps(value interface{}) []map[string]interface{} {
	switch typed := value.(type) {
	case []map[string]interface{}:
		return typed
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(typed))
		for _, item := range typed {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	default:
		return nil
	}
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	m, ok := value.(map[string]interface{})
	return m, ok
}

func strParam(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if s, ok := m[key].(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func boolParam(m map[string]interface{}, key string) bool {
	b, _ := m[key].(bool)
	return b
}

// floatParam reads a numeric value (float64/int/string) as float64.
func floatParam(m map[string]interface{}, key string) float64 {
	if m == nil {
		return 0
	}
	value := m[key]
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	case json.Number:
		f, _ := typed.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return f
	default:
		return 0
	}
}

func int64Param(m map[string]interface{}, key string) int64 {
	return int64(floatParam(m, key))
}

func secondsToMS(seconds float64) int64 {
	if seconds <= 0 {
		return 0
	}
	return int64(seconds * 1000)
}
