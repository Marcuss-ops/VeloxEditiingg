// Package hybrid / compiler_parse.go
//
// Input parsing for the hybrid.v1 compiler: map → Request, including
// the role-aware URL/duration routing against scenes[], the legacy
// images/clips fallback, and the typed coercion helpers
// (toString*, toFloat64Default, toBoolDefault, toSliceString).
package hybrid

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-worker-agent/pkg/video/plan"
)

func parseRequest(input map[string]interface{}) *Request {
	// audio_url is the canonical field; voiceover_url is accepted as
	// an alias for parity with payloads emitted by enqueue_clips.go
	// (which uses "voiceover_url" for the shared voiceover track).
	// audio_url wins when both are set.
	copyOnly := toBoolDefault(input["copy_only"], false) || strings.EqualFold(toString(input["video_mode"]), "clip_stock")
	req := &Request{
		AudioURL:  toStringDefault(input["audio_url"], toString(input["voiceover_url"])),
		Fit:       toStringDefault(input["fit"], "contain"),
		Layers:    parseLayers(input["layers"]),
		Subtitles: parseSceneSubtitleTracks(input),
		CopyOnly:  copyOnly,
	}

	if rawTracks, ok := input["audio_tracks"].([]interface{}); ok {
		for _, item := range rawTracks {
			trackMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			track := AudioTrackInput{
				SourceURL:       toStringDefault(trackMap["source_url"], toString(trackMap["url"])),
				Volume:          toFloat64Default(trackMap["volume"], 1.0),
				StartTimeOffset: toFloat64Default(trackMap["start_time_offset"], 0.0),
				DurationSeconds: toFloat64Default(trackMap["duration_seconds"], 0.0),
				Role:            toString(trackMap["role"]),
				Loop:            toBoolDefault(trackMap["loop"], false),
				FadeInSeconds:   toFloat64Default(trackMap["fade_in_seconds"], 0.0),
				FadeOutSeconds:  toFloat64Default(trackMap["fade_out_seconds"], 0.0),
				DuckingEnabled:  toBoolDefault(trackMap["ducking_enabled"], false),
			}
			// Detect explicit user config: if ANY of the
			// loop/fade/ducking keys exist in the raw map,
			// the user explicitly configured BGM behaviour.
			for _, key := range []string{"loop", "fade_in_seconds", "fade_out_seconds", "ducking_enabled"} {
				if _, exists := trackMap[key]; exists {
					track.hasExplicitBGMConfig = true
					break
				}
			}
			req.AudioTracks = append(req.AudioTracks, track)
		}
	}
	// Packet-copy can carry at most one already-final audio stream. Narrated
	// historical jobs legitimately contain one voiceover/clip track per
	// segment, so let the native renderer mix those tracks in-process instead
	// of failing the whole job or invoking an external ffmpeg process.
	if req.CopyOnly && len(req.AudioTracks) > 1 {
		req.CopyOnly = false
	}

	// Try explicit items array first. When present, this is the
	// CANONICAL timeline; the `clips` / `images` fallback below is
	// only used when items is absent (legacy compatibility index).
	//
	// Canonical-purity contract (Step 2/8): when items[] carries a
	// (role, scene) reference, resolve the URL and (when missing) the
	// duration from scene-level metadata rather than reconstructing
	// from clips[]/stock_clip_paths. scenes[] MAY be absent (legacy
	// callers pre-resolve URLs in items[]); in that case items[i].url
	// and items[i].duration are honored verbatim.
	var scenes []map[string]interface{}
	if rawScenes, ok := input["scenes"].([]interface{}); ok {
		for _, s := range rawScenes {
			if sm, ok := s.(map[string]interface{}); ok {
				scenes = append(scenes, sm)
			}
		}
	}
	if items, ok := input["items"].([]interface{}); ok {
		for _, item := range items {
			im, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			itemType := toStringDefault(im["type"], "image")
			itemURL := toString(im["url"])
			itemDuration := toFloat64Default(im["duration"], 4.0)
			itemFit := toStringDefault(im["fit"], req.Fit)
			itemHasDuration := toFloat64Default(im["duration"], 0.0) > 0

			// Role-based URL + (optional) duration routing.
			if role := toString(im["role"]); role != "" {
				sceneIdx := -1
				switch v := im["scene"].(type) {
				case int:
					sceneIdx = v
				case int64:
					sceneIdx = int(v)
				case float64:
					sceneIdx = int(v)
				}
				if sceneIdx >= 0 && sceneIdx < len(scenes) {
					scene := scenes[sceneIdx]
					switch role {
					case "voiceover_bed":
						if s := toString(scene["stock_link"]); s != "" {
							itemURL = s
						}
						if !itemHasDuration {
							if d := toFloat64Default(scene["voiceover_duration_seconds"], 0.0); d > 0 {
								itemDuration = d
							}
						}
					case "scene_clip":
						if s := toString(scene["clip_link"]); s != "" {
							itemURL = s
						}
						if !itemHasDuration {
							if d := toFloat64Default(scene["final_clip_duration_seconds"], 0.0); d > 0 {
								itemDuration = d
							}
						}
					}
				}
			}
			includeAudio := toBoolDefault(im["include_audio"], false)
			sceneID := toString(im["scene_id"])
			if sceneID == "" {
				if sceneIdx, ok := numericSceneIndex(im["scene"]); ok && sceneIdx >= 0 {
					if sceneIdx < len(scenes) {
						sceneID = toStringDefault(scenes[sceneIdx]["id"], fmt.Sprintf("scene-%d", sceneIdx))
					} else {
						sceneID = fmt.Sprintf("scene-%d", sceneIdx)
					}
				}
			}
			// A voiceover_bed is visual stock only. Its audio is supplied by
			// audio_tracks, and allowing source audio here would make the
			// native codec use -shortest and truncate a looped stock segment
			// at the source clip's real duration.
			if strings.EqualFold(toString(im["role"]), "voiceover_bed") {
				includeAudio = false
			}
			req.Items = append(req.Items, ItemInput{
				Type:                     itemType,
				URL:                      itemURL,
				SceneID:                  sceneID,
				ColorHex:                 toStringDefault(im["color_hex"], "#000000"),
				Duration:                 itemDuration,
				Fit:                      itemFit,
				Role:                     toString(im["role"]),
				VoiceoverDurationSeconds: toFloat64Default(im["voiceover_duration_seconds"], 0.0),
				FinalClipDurationSeconds: toFloat64Default(im["final_clip_duration_seconds"], 0.0),
				IncludeAudio:             includeAudio,
			})
		}
		return req
	}

	// The public Master contract carries per-scene media in scenes_json.
	// Convert clip entries into the canonical timeline while preserving
	// the clip's own audio; voiceover audio remains a separate concern.
	if encoded, ok := input["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(encoded), &scenes); err == nil {
			for sceneIndex, scene := range scenes {
				clipURL := toString(scene["clip_link"])
				if clip, ok := scene["clip"].(map[string]interface{}); ok {
					clipURL = toStringDefault(clip["url"], clipURL)
				}
				if clipURL == "" {
					continue
				}
				req.Items = append(req.Items, ItemInput{
					Type:         "video",
					URL:          clipURL,
					SceneID:      toStringDefault(scene["id"], fmt.Sprintf("scene-%d", sceneIndex)),
					Duration:     toFloat64Default(scene["duration_seconds"], 4.0),
					Fit:          req.Fit,
					IncludeAudio: true,
				})
			}
		}
		if len(req.Items) > 0 {
			return req
		}
	}

	// Fallback: build from images + clips arrays
	images := toSliceString(input["images"])
	clips := toSliceString(input["clips"])

	for _, url := range images {
		req.Items = append(req.Items, ItemInput{
			Type:     "image",
			URL:      url,
			Duration: 5.0,
			Fit:      "cover",
		})
	}
	for _, url := range clips {
		req.Items = append(req.Items, ItemInput{
			Type:     "video",
			URL:      url,
			Duration: 4.0,
			Fit:      "contain",
		})
	}

	return req
}

func parseCanvas(input map[string]interface{}) plan.CanvasSpec {
	canvas := plan.DefaultCanvas()
	output, ok := input["output"].(map[string]interface{})
	if !ok {
		return canvas
	}
	canvas.Width = int(toFloat64Default(output["width"], float64(canvas.Width)))
	canvas.Height = int(toFloat64Default(output["height"], float64(canvas.Height)))
	canvas.Fps = int(toFloat64Default(output["fps"], float64(canvas.Fps)))
	return canvas
}

func numericSceneIndex(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func toStringDefault(v interface{}, fallback string) string {
	s := toString(v)
	if s == "" {
		return fallback
	}
	return s
}

func toFloat64Default(v interface{}, fallback float64) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	}
	return fallback
}

func toBoolDefault(v interface{}, fallback bool) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		switch strings.ToLower(strings.TrimSpace(val)) {
		case "1", "true", "yes", "y":
			return true
		case "0", "false", "no", "n":
			return false
		}
	}
	return fallback
}

func parseLayers(raw interface{}) []plan.Layer {
	items, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	result := make([]plan.Layer, 0, len(items))
	for index, rawItem := range items {
		item, ok := rawItem.(map[string]interface{})
		if !ok {
			continue
		}
		id := toString(item["id"])
		if id == "" {
			id = fmt.Sprintf("layer_%d", index)
		}
		layer := plan.Layer{
			ID:              id,
			Type:            toString(item["type"]),
			Role:            toString(item["role"]),
			Text:            toString(item["text"]),
			Asset:           toString(item["asset"]),
			Source:          toString(item["source"]),
			Font:            toString(item["font"]),
			FontSize:        toFloat64Default(item["font_size"], 0),
			StartSeconds:    toFloat64Default(item["start_seconds"], 0),
			DurationSeconds: toFloat64Default(item["duration_seconds"], 0),
			Preset:          toString(item["preset"]),
			Animation:       toString(item["animation"]),
		}
		if position, ok := item["position"].([]interface{}); ok {
			for _, value := range position {
				layer.Position = append(layer.Position, toFloat64Default(value, 0))
			}
		}
		if layer.Type != "" {
			result = append(result, layer)
		}
	}
	return result
}

// parseSceneSubtitleTracks derives the worker's internal subtitle tracks from
// the canonical per-scene asset objects. The RenderPlan keeps a flat
// subtitle_tracks array because the renderer consumes that format, but the
// HTTP/enqueue contract has exactly one source of truth: scenes[].subtitles.
func parseSceneSubtitleTracks(input map[string]interface{}) []plan.SubtitleTrack {
	var scenes []map[string]interface{}
	if encoded, ok := input["scenes_json"].(string); ok && strings.TrimSpace(encoded) != "" {
		_ = json.Unmarshal([]byte(encoded), &scenes)
	}
	if len(scenes) == 0 {
		if raw, ok := input["scenes"].([]interface{}); ok {
			for _, value := range raw {
				if scene, ok := value.(map[string]interface{}); ok {
					scenes = append(scenes, scene)
				}
			}
		}
	}

	result := make([]plan.SubtitleTrack, 0, len(scenes))
	for _, scene := range scenes {
		subtitles, ok := scene["subtitles"].(map[string]interface{})
		if !ok {
			continue
		}
		track := plan.SubtitleTrack{
			Source: toStringDefault(subtitles["url"], toString(subtitles["source"])),
			Preset: toString(subtitles["preset"]),
			Font:   toString(subtitles["font"]),
		}
		if track.Source != "" {
			result = append(result, track)
		}
	}
	return result
}

func toSliceString(v interface{}) []string {
	switch val := v.(type) {
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case []string:
		return val
	}
	return nil
}
