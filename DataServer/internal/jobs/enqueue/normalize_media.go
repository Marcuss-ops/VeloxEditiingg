// Package enqueue - voiceover, audio, subtitle, and media timeline helpers.
package enqueue

import (
	"fmt"
	"strings"

	"velox-server/internal/routing"
	"velox-shared/payload"
)

func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {
	candidates := []string{
		payload.FirstString(payloadMap, "voiceover_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers_urls"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	result := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, item := range candidates {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
		}
	}
	return result
}
func voiceoverCountFromPayload(payloadMap map[string]interface{}) int {
	if arr, ok := payloadMap["voiceover_paths"].([]string); ok {
		return len(arr)
	}
	if arr, ok := payloadMap["voiceover_paths"].([]interface{}); ok {
		return len(arr)
	}
	return len(normalizeVoiceoverList(payloadMap))
}
func hasClipTimelinePayload(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	for _, key := range []string{"clips", "items", "clip_segments", "intro_clip_paths", "stock_clip_paths"} {
		switch v := payloadMap[key].(type) {
		case []string:
			if len(v) > 0 {
				return true
			}
		case []interface{}:
			if len(v) > 0 {
				return true
			}
		}
	}
	return false
}
func normalizeAudioTracks(raw interface{}) []map[string]interface{} {
	var tracks []map[string]interface{}
	switch values := raw.(type) {
	case []map[string]interface{}:
		for _, value := range values {
			if len(value) > 0 {
				tracks = append(tracks, value)
			}
		}
	case []interface{}:
		for _, value := range values {
			if track, ok := value.(map[string]interface{}); ok && len(track) > 0 {
				tracks = append(tracks, track)
			}
		}
	}
	return tracks
}
func audioTrackKey(track map[string]interface{}) string {
	if track == nil {
		return ""
	}
	source := strings.TrimSpace(payload.FirstString(track, "source_url", "source", "url"))
	if source == "" {
		return ""
	}
	role := strings.TrimSpace(payload.FirstString(track, "role"))
	offset := payload.NormalizedDuration(track["start_time_offset"])
	return fmt.Sprintf("%s\x00%s\x00%.6f", role, source, offset)
}
func normalizeSubtitleTracks(raw interface{}) []map[string]interface{} {
	var tracks []map[string]interface{}
	switch values := raw.(type) {
	case []map[string]interface{}:
		for _, value := range values {
			if len(value) > 0 {
				tracks = append(tracks, value)
			}
		}
	case []interface{}:
		for _, value := range values {
			if track, ok := value.(map[string]interface{}); ok && len(track) > 0 {
				tracks = append(tracks, track)
			}
		}
	}
	return tracks
}
func syncAudioURLFromVoiceover(payloadMap map[string]interface{}) {
	if payloadMap == nil {
		return
	}
	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return
	}
	if strings.TrimSpace(payload.FirstString(payloadMap, "audio_url")) == "" || hasClipTimelinePayload(payloadMap) || strings.TrimSpace(payload.FirstString(payloadMap, "pipeline_id", routing.KeyPipelineID)) != "" {
		payloadMap["audio_url"] = voiceovers[0]
	}
}
