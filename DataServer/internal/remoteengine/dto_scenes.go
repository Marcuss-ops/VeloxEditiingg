package remoteengine

import (
	"encoding/json"
	"strconv"
	"strings"

	"velox-shared/compatibility"
	"velox-shared/payload"
)

func extractScenesDTO(flat map[string]interface{}) []SceneResult {
	// Try scenes_json string first.
	if rawJSON := payload.FirstString(flat, "scenes_json"); rawJSON != "" {
		var scenes []SceneResult
		if err := json.Unmarshal([]byte(rawJSON), &scenes); err == nil && len(scenes) > 0 {
			return scenes
		}
		// Try as a generic []interface{}.
		var rawScenes []interface{}
		if err := json.Unmarshal([]byte(rawJSON), &rawScenes); err == nil {
			return convertRawScenes(rawScenes)
		}
	}

	// Try scenes as a parsed array.
	if rawScenes, ok := flat["scenes"].([]interface{}); ok && len(rawScenes) > 0 {
		return convertRawScenes(rawScenes)
	}

	return nil
}

// convertRawScenes converts a []interface{} of map[string]interface{}
// into typed []SceneResult.
//
// Phase 2 of the render-manifest plan: scene_id / index / kind /
// clip{} / voiceover{} / subtitles{} nested objects are read from
// the flat-map raw input so the typed DTO carries the canonical
// per-scene enrichment. The flat clip_link / image_link keys
// remain supported for back-compat with legacy creator outputs.
func convertRawScenes(raw []interface{}) []SceneResult {
	scenes := make([]SceneResult, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		scene := SceneResult{
			Text:       payload.FirstString(m, "text", "description", "narration"),
			SceneID:    payload.FirstString(m, "scene_id"),
			Index:      intFromAnyMap(m["index"]),
			Kind:       payload.FirstString(m, "kind"),
			ImageLink:  payload.FirstString(m, "image_link", "image_url", "image"),
			ClipLink:   payload.FirstString(m, "clip_link", "clip_url", "video_link"),
			StockLinks: append(append(compatibility.ReadStringList(m, "stock_links"), compatibility.ReadStringList(m, "stock_clip_links")...), compatibility.ReadStringList(m, "drive_links")...),
		}
		if fallback, ok := m["stock_fallback"].(bool); ok {
			scene.StockFallback = fallback
		}
		if dur, ok := m["duration_seconds"].(float64); ok {
			scene.DurationSeconds = dur
		}
		if clip, ok := m["clip"].(map[string]interface{}); ok {
			scene.Clip = convertClipAsset(clip)
		}
		if vo, ok := m["voiceover"].(map[string]interface{}); ok {
			scene.Voiceover = convertVoiceoverAsset(vo)
		}
		if sub, ok := m["subtitles"].(map[string]interface{}); ok {
			scene.Subtitles = convertSubtitlesAsset(sub)
		}
		scenes = append(scenes, scene)
	}
	return scenes
}

// intFromAnyMap coerces an arbitrary JSON-decoded value (int /
// int64 / float64 / string with numeric content / nil) into a Go int64.
// Returns 0 for unknown shapes so the caller can treat 0 as "absent".
// Used by convertRawScenes for the new scene.Index field and the
// ClipAsset.StartMS / EndMS / DurationMS / VoiceoverAsset.DurationMS
// fields — JSON numbers are decoded as float64 by encoding/json,
// so the typed fields need explicit coercion. int64 (rather than
// int) is the canonical Go type for millisecond durations so the
// renderer can compose 64-bit timestamps without overflow up to
// ~292M years.
func intFromAnyMap(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i
		}
	}
	return 0
}
