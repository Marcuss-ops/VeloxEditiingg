// Package enqueue - voiceover, audio, subtitle, and media timeline helpers.
package enqueue

import (
	"fmt"
	"strings"

	"velox-server/internal/routing"
	"velox-shared/compatibility"
	"velox-shared/payload"
)

func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	// Once the canonical field is present it is authoritative. Do not
	// re-scan broad legacy aliases such as `source` or `url`: canonical
	// JobPayloadV2 metadata uses those keys for non-audio fields, and a
	// second normalization must not reinterpret them as voiceover paths.
	if raw, present := payloadMap[compatibility.VoiceoverPathsKey]; present {
		if canonical := payload.NormalizeToStrings(raw); len(canonical) > 0 {
			return canonical
		}
		// Preserve the migration fallback for an explicitly empty
		// canonical field paired with a legacy value.
	}
	return compatibility.ReadStringList(payloadMap, compatibility.VoiceoverPathsKey)
}

// extractVoiceoverPaths reads the legacy and canonical voiceover shapes used
// by pipeline and API payloads. Keep this media concern in one place so the
// pipeline builder and scene normalizer cannot drift in alias handling or
// deduplication.
func extractVoiceoverPaths(p map[string]interface{}) []string {
	var candidates []string
	if s := payload.FirstString(p, "voiceover_path", "audio_path", "voiceover"); s != "" {
		candidates = append(candidates, s)
	}
	if v, ok := p["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if voiceover, ok := p["voiceover"].(map[string]interface{}); ok {
		candidates = append(candidates,
			payload.FirstString(voiceover, "local_path", "path", "drive_link", "url"),
		)
	}
	if nested, ok := p["voiceover_info"].(map[string]interface{}); ok {
		candidates = append(candidates,
			payload.FirstString(nested, "local_path", "path", "drive_link", "url"),
		)
	}
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

// hasAudioTracks reports whether at least one normalized audio track has a
// usable source alias. It deliberately reuses normalizeAudioTracks so the
// type-shape filtering is shared with the legacy timeline adapter.
func hasAudioTracks(payloadMap map[string]interface{}) bool {
	for _, track := range normalizeAudioTracks(payloadMap["audio_tracks"]) {
		if payload.FirstString(track, "source_url", "source", "url") != "" {
			return true
		}
	}
	return false
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
	return normalizeMapList(raw)
}

func normalizeMapList(raw interface{}) []map[string]interface{} {
	var result []map[string]interface{}
	switch values := raw.(type) {
	case []map[string]interface{}:
		for _, value := range values {
			if len(value) > 0 {
				result = append(result, value)
			}
		}
	case []interface{}:
		for _, value := range values {
			if item, ok := value.(map[string]interface{}); ok && len(item) > 0 {
				result = append(result, item)
			}
		}
	}
	return result
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
	return normalizeMapList(raw)
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
