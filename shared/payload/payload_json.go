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
