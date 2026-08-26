package remoteengine

import (
	"strconv"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

func extractScenesDTO(flat map[string]interface{}) []SceneResult {
	// scenes_json and scenes share one parser and normalizer. The DTO only
	// performs the adapter conversion after the canonical contract boundary.
	if rawJSON := payload.FirstString(flat, "scenes_json"); rawJSON != "" {
		if scenes, err := contract.ParseSceneMapsJSON([]byte(rawJSON)); err == nil && len(scenes) > 0 {
			return convertCanonicalScenes(scenes)
		}
	}
	if rawScenes, ok := flat["scenes"]; ok {
		if scenes, err := contract.ParseSceneMaps(rawScenes); err == nil && len(scenes) > 0 {
			return convertCanonicalScenes(scenes)
		}
	}
	return nil
}

// convertRawScenes remains as a compatibility adapter for package-local
// callers, but delegates parsing and normalization to the shared contract.
func convertRawScenes(raw []interface{}) []SceneResult {
	scenes, err := contract.ParseSceneMaps(raw)
	if err != nil {
		return nil
	}
	return convertCanonicalScenes(scenes)
}

// convertCanonicalScenes converts already parsed canonical scene maps into
// the remote-engine DTO. Asset field conversion belongs to this adapter; JSON
// parsing, alias normalization and default duration handling do not.
func convertCanonicalScenes(raw []map[string]interface{}) []SceneResult {
	scenes := make([]SceneResult, 0, len(raw))
	for _, m := range raw {
		scene := SceneResult{
			Text:       payload.FirstString(m, "text"),
			SceneID:    payload.FirstString(m, "scene_id"),
			Index:      intFromAnyMap(m["index"]),
			Kind:       payload.FirstString(m, "kind"),
			ImageLink:  payload.FirstString(m, "image_link"),
			ClipLink:   payload.FirstString(m, "clip_link", "clip_url", "video_link"),
			StockLinks: nil,
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
