// Package payload / payload_json.go
//
// JSON helpers (MustJSON, DeepCopyMap) + media-source sniffing.
// Split out of payload.go; the package doc lives in payload.go.
package payload

import (
	"encoding/json"
	"strings"
)

func MustJSON(v interface{}) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

// DeepCopyMap esegue una copia profonda di map[string]interface{} usando JSON marshal/unmarshal.
// Utile per clonare strutture annidate senza condividere riferimenti.
func DeepCopyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	data, _ := json.Marshal(m)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result
}

// StringSetFromAny returns the non-empty, trimmed string values from a JSON
// array or a native string slice. It gives payload projections one canonical
// way to read identifier lists regardless of whether they came from JSON.
func StringSetFromAny(value interface{}) map[string]bool {
	selected := make(map[string]bool)
	add := func(id string) {
		if id = strings.TrimSpace(id); id != "" {
			selected[id] = true
		}
	}
	switch values := value.(type) {
	case []interface{}:
		for _, value := range values {
			if id, ok := value.(string); ok {
				add(id)
			}
		}
	case []string:
		for _, id := range values {
			add(id)
		}
	}
	return selected
}

// NestedMapFromPayload finds a map at the payload root or under runtime_payload.
// It keeps Master diagnostics and worker projections aligned on the same wire
// envelope rules.
func NestedMapFromPayload(input map[string]interface{}, key string) map[string]interface{} {
	if input == nil {
		return nil
	}
	if value, ok := input[key].(map[string]interface{}); ok {
		return value
	}
	if nested, ok := input["runtime_payload"].(map[string]interface{}); ok {
		if value, ok := nested[key].(map[string]interface{}); ok {
			return value
		}
	}
	return nil
}

// StringSetFromNestedPayload reads a string identifier list from a named
// payload map, accepting both root-level and runtime_payload envelopes.
func StringSetFromNestedPayload(input map[string]interface{}, mapKey, listKey string) map[string]bool {
	return StringSetFromAny(NestedMapFromPayload(input, mapKey)[listKey])
}

// IsLikelyMediaSource verifica se una stringa sembra essere una fonte multimediale
// (URL con http/https/file, o estensione video/audio comune).
func IsLikelyMediaSource(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	return strings.HasPrefix(value, "http://") ||
		strings.HasPrefix(value, "https://") ||
		strings.HasPrefix(value, "file://") ||
		strings.HasSuffix(value, ".mp4") ||
		strings.HasSuffix(value, ".mov") ||
		strings.HasSuffix(value, ".mkv") ||
		strings.HasSuffix(value, ".webm") ||
		strings.HasSuffix(value, ".mp3") ||
		strings.HasSuffix(value, ".wav") ||
		strings.HasSuffix(value, ".m4a")
}
