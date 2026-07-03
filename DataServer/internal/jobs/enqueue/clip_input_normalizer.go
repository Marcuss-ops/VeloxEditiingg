// Package enqueue — clip input normalization (scenes, clips, JSON).
package enqueue

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/payload"
)

// normalizeClipPayload detects the input shape (narrated-clip scenes,
// flat clip scenes, scenes_json, or raw clips array/strings) and
// produces the canonical sceneEntries, items, clip URLs, audio tracks,
// and video mode consumed by BuildClipPayloadForMaster.
func normalizeClipPayload(rawPayload map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if scenes := normalizeSceneArray(rawPayload["scenes"]); len(scenes) > 0 {
		if supportsNarratedClipScenes(scenes) {
			return buildNarratedClipPayload(scenes)
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

	if raw := payload.FirstString(rawPayload, "scenes_json"); raw != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("invalid scenes_json: %w", err)
		}
		return normalizeClipPayload(map[string]interface{}{"scenes": scenes})
	}

	switch rawClips := rawPayload["clips"].(type) {
	case []interface{}:
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
		if len(items) > 0 {
			return sceneEntries, items, payload.DedupeStrings(clips), nil, "clips", nil
		}
	case []string:
		return normalizeClipPayload(map[string]interface{}{"clips": toInterfaceSlice(rawClips)})
	}

	return nil, nil, nil, nil, "", fmt.Errorf("scenes, scenes_json, or clips are required")
}

func toInterfaceSlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
