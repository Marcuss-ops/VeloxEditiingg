// Package enqueue — canonical payload builder for slideshow-style jobs.
package enqueue

import (
	"fmt"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

// BuildSlideshowPayloadForMaster builds a render-ready payload for definitions
// that reuse scene.composite.v1 but want the worker to route through
// `images.v1`. It reuses the canonical scene-image builder, then mirrors the
// normalized image/audio fields expected by the images pipeline compiler.
func BuildSlideshowPayloadForMaster(rawPayload map[string]interface{}, dataDir, videosDir, masterURL string) (map[string]interface{}, error) {
	normalized, err := BuildSceneImagePayloadForMaster(rawPayload, dataDir, videosDir, masterURL)
	if err != nil {
		return nil, err
	}

	images := payload.NormalizeToStrings(normalized["scene_image_paths"])
	if len(images) == 0 {
		switch scenes := normalized["scenes"].(type) {
		case []map[string]interface{}:
			for _, scene := range scenes {
				if image := contract.FirstSceneImageLink(scene); image != "" {
					images = append(images, image)
				}
			}
		case []interface{}:
			for _, item := range scenes {
				scene, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				if image := contract.FirstSceneImageLink(scene); image != "" {
					images = append(images, image)
				}
			}
		}
	}
	images = dedupeNonEmpty(images)
	if len(images) == 0 {
		return nil, fmt.Errorf("slideshow: at least one image is required")
	}

	voiceovers := normalizeVoiceoverList(normalized)
	if len(voiceovers) == 0 {
		return nil, fmt.Errorf("slideshow: at least one voiceover path is required")
	}

	normalized["images"] = images
	normalized["audio_url"] = voiceovers[0]
	normalized["video_mode"] = "slideshow"
	if effect := strings.TrimSpace(payload.FirstString(rawPayload, "effect")); effect != "" {
		normalized["effect"] = effect
	} else {
		normalized["effect"] = "slow_zoom"
	}
	if orientation := strings.TrimSpace(payload.FirstString(rawPayload, "orientation")); orientation != "" {
		normalized["orientation"] = orientation
	}
	return normalized, nil
}

func dedupeNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
