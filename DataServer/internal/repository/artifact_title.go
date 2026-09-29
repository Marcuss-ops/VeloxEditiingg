package repository

import (
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// ResolveArtifactTitle keeps Drive filenames human-readable when a producer
// used an opaque digest as video_name but retained the submitted script title
// in the immutable job request.
func ResolveArtifactTitle(videoName, requestJSON string) string {
	videoName = strings.TrimSpace(videoName)
	if videoName != "" && !isOpaqueArtifactName(videoName) {
		return videoName
	}
	var request map[string]any
	if json.Unmarshal([]byte(requestJSON), &request) != nil {
		return ""
	}
	for _, key := range []string{"script_title", "title", "topic", "name"} {
		if title := findString(request, key); title != "" && !isOpaqueArtifactName(title) {
			return title
		}
	}
	return ""
}

func findString(value any, key string) string {
	switch current := value.(type) {
	case map[string]any:
		if candidate, ok := current[key].(string); ok && strings.TrimSpace(candidate) != "" {
			return strings.TrimSpace(candidate)
		}
		keys := make([]string, 0, len(current))
		for nestedKey := range current {
			keys = append(keys, nestedKey)
		}
		sort.Strings(keys)
		for _, nestedKey := range keys {
			if found := findString(current[nestedKey], key); found != "" {
				return found
			}
		}
	case []any:
		for _, nested := range current {
			if found := findString(nested, key); found != "" {
				return found
			}
		}
	}
	return ""
}

func isOpaqueArtifactName(value string) bool {
	value = strings.TrimSuffix(strings.TrimSpace(value), ".mp4")
	if len(value) != 32 && len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
