// Package enqueue / clip_input_normalizer.go
//
// Input-shape adapters and URL extractors for the script-with-clips
// pipeline. Three explicit input forms — no registry, no per-format
// interface, no scene factory, no generic pipeline builder:
//
//  1. scenes-array under "scenes" (or `scenes_json` JSON string round-trip):
//     when ANY scene carries a voiceover URL, routes to
//     buildNarratedClipPayload (narrated_clip_timeline.go); otherwise
//     builds per-scene entries/items/clips inline. Returns
//     videoMode="clips".
//
//  2. clips-array as []interface{}: each entry is either a string URL
//     (becomes "Clip N" scene with default 4s duration) or a
//     {url / clip_link / drive_link, ...} map (uses duration /
//     duration_seconds when supplied, else 4.0 fallback). Returns
//     videoMode="clips".
//
//  3. clips-array as []string: converted to []interface{} and re-entered
//     into the same branch via self-recursion.
package enqueue

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

// normalizeClipPayload adapts the three explicit input shapes of
// script/generate-from-clips into the canonical (sceneEntries, clipItems,
// clipURLs, audioTracks, videoMode) tuple consumed by
// BuildClipPayloadForMaster.
//
// Returns an empty tuple + "scenes, scenes_json, or clips are required"
// when none of the three forms is present.
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

// supportsNarratedClipScenes returns true when at least one scene in the
// list carries a voiceover URL. Routing predicate used by
// normalizeClipPayload to dispatch to buildNarratedClipPayload instead of
// the inline clips build.
func supportsNarratedClipScenes(scenes []map[string]interface{}) bool {
	for _, scene := range scenes {
		if sceneVoiceoverURL(scene) != "" {
			return true
		}
	}
	return false
}

// normalizeSceneArray adapts a raw "scenes" value into a canonical
// []map[string]interface{} by invoking contract.NormalizeSceneEntry on
// each element. Both []map[string]interface{} and []interface{} shapes
// are accepted; non-map entries in the latter are silently skipped (the
// scene-video contract requires map entries).
//
// Moved from enqueue.go so the normalizeClipPayload family has a single
// home (clip_input_normalizer.go); enqueue_scene_image.go and other
// callers continue to reference it without import changes.
func normalizeSceneArray(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	default:
		return nil
	}
}

// firstClipURL returns the canonical first clip URL for a scene:
// `clip_link` / `drive_link` scalar form, OR the first URL from
// `clip_links` / `drive_links` list form. `bindings.clip.drive_link` is
// a secondary fallback for callers that nest their clip metadata.
func firstClipURL(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if s := payload.FirstString(scene, "clip_link", "drive_link"); s != "" {
		return s
	}
	if links := payload.NormalizeStringList(scene, "clip_links", "drive_links"); len(links) > 0 {
		return links[0]
	}
	return ""
}

// sceneFinalClipURL is the URL of the visual clip rendered as the final
// scene asset. Falls back to firstClipURL, then `bindings.clip.drive_link`.
func sceneFinalClipURL(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if url := firstClipURL(scene); url != "" {
		return url
	}
	if bindings, ok := scene["bindings"].(map[string]interface{}); ok {
		if clip, ok := bindings["clip"].(map[string]interface{}); ok {
			if url := payload.FirstString(clip, "drive_link", "url", "clip_link"); url != "" {
				return url
			}
		}
	}
	return ""
}

// sceneNarrationClipURL is the URL of the source clip used as the
// voiceover bed BEFORE the final clip is overlaid. Falls back to
// sceneFinalClipURL when no `stock_link` / `narration_clip_link` is
// present.
func sceneNarrationClipURL(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if url := payload.FirstString(scene, "stock_link", "narration_clip_link"); url != "" {
		return url
	}
	if bindings, ok := scene["bindings"].(map[string]interface{}); ok {
		if stock, ok := bindings["stock"].(map[string]interface{}); ok {
			if url := payload.FirstString(stock, "drive_link", "url", "clip_link"); url != "" {
				return url
			}
		}
	}
	return sceneFinalClipURL(scene)
}

// sceneVoiceoverURL is the URL of the audio asset that drives the
// scene's voiceover timing. Falls back to `bindings.voiceover.link/url`.
func sceneVoiceoverURL(scene map[string]interface{}) string {
	if scene == nil {
		return ""
	}
	if url := payload.FirstString(scene, "voiceover_link", "reference_voiceover", "voiceover_path"); url != "" {
		return url
	}
	if bindings, ok := scene["bindings"].(map[string]interface{}); ok {
		if voiceover, ok := bindings["voiceover"].(map[string]interface{}); ok {
			if url := payload.FirstString(voiceover, "link", "url", "drive_link", "local_path"); url != "" {
				return url
			}
		}
	}
	return ""
}

// toInterfaceSlice converts []string to []interface{} so the type-switch
// on rawPayload["clips"] in normalizeClipPayload can dispatch []string
// payloads to the []interface{} branch via self-recursion.
func toInterfaceSlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
