// Package enqueue — clip input normalization (scenes, clips, JSON).
package enqueue

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/payload"
)

// normalizeClipPayload is a thin dispatcher over three explicit
// input-shape adapters. No registry, no per-format interface, no
// scene factory, no generic pipeline. Each branch is a pure function
// over a distinct input form:
//
//   - scenes array           → normalizeScenesInput
//   - scenes_json string     → normalizeScenesJSONInput
//   - raw clips array/string → normalizeClipsInput
//
// The canonical 6-tuple return is identical for all three; the
// dispatcher only inspects the input shape to pick the right adapter.
func normalizeClipPayload(rawPayload map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if scenes := normalizeSceneArray(rawPayload["scenes"]); len(scenes) > 0 {
		return normalizeScenesInput(rawPayload, scenes)
	}
	if raw := payload.FirstString(rawPayload, "scenes_json"); raw != "" {
		return normalizeScenesJSONInput(rawPayload, raw)
	}
	if rawClips, ok := rawPayload["clips"]; ok {
		return normalizeClipsInput(rawClips)
	}
	return nil, nil, nil, nil, "", fmt.Errorf("scenes, scenes_json, or clips are required")
}

// normalizeScenesInput handles the canonical scenes array input.
// When any scene carries a voiceover binding it routes to the
// narrated-clip timeline builder; otherwise it emits a flat
// per-scene clip_item list.
func normalizeScenesInput(rawPayload map[string]interface{}, scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if supportsNarratedClipScenes(scenes) {
		return buildNarratedClipPayload(scenes, withFallbackURLs(sceneFallbackNarrationClipURLs(rawPayload)))
	}

	sceneEntries := make([]map[string]interface{}, 0, len(scenes))
	items := make([]map[string]interface{}, 0, len(scenes))
	clips := make([]string, 0, len(scenes))
	for i, scene := range scenes {
		url := firstClipURL(scene)
		if url == "" {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: clip url is required", i)
		}
		duration := payload.NormalizedDuration(scene["duration_seconds"])
		if duration <= 0 {
			duration = 4.0
		}

		normalized := make(map[string]interface{}, len(scene)+4)
		for k, v := range scene {
			normalized[k] = v
		}
		normalized["clip_link"] = url
		normalized["clip_links"] = []string{url}
		normalized["duration_seconds"] = duration
		if text := payload.FirstString(scene, "text", "description"); text != "" {
			normalized["text"] = text
		}

		sceneEntries = append(sceneEntries, normalized)
		items = append(items, map[string]interface{}{
			"type":     "video",
			"url":      url,
			"duration": duration,
			"fit":      "contain",
		})
		clips = append(clips, url)
	}
	return sceneEntries, items, payload.DedupeStrings(clips), nil, "clips", nil
}

// normalizeScenesJSONInput parses a scenes_json string and routes to
// normalizeScenesInput. The rawPayload is carried through so the
// narrated-clip fallback URL pool (top-level stock_clip_paths /
// intro_clip_paths) is preserved across the JSON parse hop.
func normalizeScenesJSONInput(rawPayload map[string]interface{}, scenesJSON string) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(scenesJSON), &scenes); err != nil {
		return nil, nil, nil, nil, "", fmt.Errorf("invalid scenes_json: %w", err)
	}
	return normalizeScenesInput(rawPayload, scenes)
}

// normalizeClipsInput handles the raw clips array (string entries or
// map entries) and the string-slice variant. Each entry becomes one
// synthetic scene with a default 4s duration; missing URLs are
// rejected. The clips input form has no use for the raw payload map
// (no top-level fallback pool to extract), so it is not threaded in.
func normalizeClipsInput(rawClips interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	switch clips := rawClips.(type) {
	case []interface{}:
		return normalizeClipsAsInterface(clips)
	case []string:
		return normalizeClipsAsInterface(toInterfaceSlice(clips))
	default:
		return nil, nil, nil, nil, "", fmt.Errorf("clips: unsupported shape %T", rawClips)
	}
}

func normalizeClipsAsInterface(rawClips []interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	sceneEntries := make([]map[string]interface{}, 0, len(rawClips))
	items := make([]map[string]interface{}, 0, len(rawClips))
	clips := make([]string, 0, len(rawClips))
	for i, item := range rawClips {
		switch clip := item.(type) {
		case string:
			url := strings.TrimSpace(clip)
			if url == "" {
				return nil, nil, nil, nil, "", fmt.Errorf("clips[%d]: url is required", i)
			}
			sceneEntries = append(sceneEntries, map[string]interface{}{
				"text":             fmt.Sprintf("Clip %d", i+1),
				"clip_link":        url,
				"clip_links":       []string{url},
				"duration_seconds": 4.0,
			})
			items = append(items, map[string]interface{}{
				"type":     "video",
				"url":      url,
				"duration": 4.0,
				"fit":      "contain",
			})
			clips = append(clips, url)
		case map[string]interface{}:
			url := payload.FirstString(clip, "url", "clip_link", "drive_link")
			if url == "" {
				urls := payload.NormalizeStringList(clip, "clip_links", "drive_links")
				if len(urls) > 0 {
					url = urls[0]
				}
			}
			if url == "" {
				return nil, nil, nil, nil, "", fmt.Errorf("clips[%d]: url is required", i)
			}
			duration := payload.NormalizedDuration(clip["duration"])
			if duration <= 0 {
				duration = payload.NormalizedDuration(clip["duration_seconds"])
			}
			if duration <= 0 {
				duration = 4.0
			}
			sceneEntries = append(sceneEntries, map[string]interface{}{
				"text":             payload.FirstString(clip, "text", "description"),
				"clip_link":        url,
				"clip_links":       []string{url},
				"duration_seconds": duration,
			})
			items = append(items, map[string]interface{}{
				"type":     "video",
				"url":      url,
				"duration": duration,
				"fit":      "contain",
			})
			clips = append(clips, url)
		}
	}
	return sceneEntries, items, payload.DedupeStrings(clips), nil, "clips", nil
}

func toInterfaceSlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
