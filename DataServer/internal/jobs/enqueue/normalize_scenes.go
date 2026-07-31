// Package enqueue - scene normalization and narrated clip timeline helpers.
package enqueue

import (
	"encoding/json"
	"strings"

	"velox-shared/contract"
	"velox-shared/payload"
)

func normalizeScenes(payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {
	if v, ok := payloadMap["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, contract.NormalizeSceneEntry(m))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, contract.NormalizeSceneEntry(item))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = contract.NormalizeSceneEntry(scenes[i])
		}
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}
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
func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, ok := payloadMap["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payloadMap["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}
func attachLegacySceneClipTimeline(out map[string]interface{}) {
	if out == nil || hasClipTimelinePayload(out) {
		return
	}
	scenes := normalizeSceneArray(out["scenes"])
	if len(scenes) == 0 {
		return
	}

	voiceovers := normalizeVoiceoverList(out)
	items := make([]map[string]interface{}, 0, len(scenes))
	clips := make([]string, 0, len(scenes))
	rawAudioTracks := normalizeAudioTracks(out["audio_tracks"])
	audioTracks := make([]map[string]interface{}, 0, len(rawAudioTracks)+len(scenes))
	seenAudioTracks := make(map[string]struct{}, len(rawAudioTracks)+len(scenes))
	for _, track := range rawAudioTracks {
		key := audioTrackKey(track)
		if key == "" {
			audioTracks = append(audioTracks, track)
			continue
		}
		if _, exists := seenAudioTracks[key]; exists {
			continue
		}
		audioTracks = append(audioTracks, track)
		seenAudioTracks[key] = struct{}{}
	}
	subtitleTracks := normalizeSubtitleTracks(out["subtitle_tracks"])
	offsetSeconds := 0.0

	for i, scene := range scenes {
		clipURL := firstClipURL(scene)
		if clipURL == "" {
			return
		}
		voiceoverDuration := sceneVoiceoverDurationSeconds(scene)
		clipDuration := sceneClipDurationSeconds(scene)
		duration := payload.NormalizedDuration(scene["duration_seconds"])
		// When a voiceover is present, the timeline is paced by the
		// VOICEOVER duration (the clip's audio is layered as a
		// secondary audio track at start_time_offset=voiceoverDuration).
		// Summing voiceoverDuration + clipDuration into the video-item
		// duration would double-count the clip's on-screen duration —
		// the clip plays for voiceoverDuration scene-seconds, with the
		// clip's original audio dangling at the end. The single
		// canonical timeline is the voiceover duration.
		// The scene's top-level duration_seconds is overridden here
		// because it is a presentation placeholder (the typed envelope's
		// authoritative timing lives on scene.voiceover.duration_ms /
		// scene.voiceover.duration_seconds).
		if voiceoverDuration > 0 {
			duration = voiceoverDuration
		}
		if duration <= 0 {
			if clipDuration > 0 {
				duration = clipDuration
			} else {
				duration = 4.0
			}
		}

		normalized := make(map[string]interface{}, len(scene)+3)
		for k, v := range scene {
			normalized[k] = v
		}
		normalized["clip_link"] = clipURL
		normalized["clip_links"] = []string{clipURL}
		normalized["duration_seconds"] = duration
		if voiceoverDuration > 0 {
			normalized["voiceover_duration_seconds"] = voiceoverDuration
			normalized["final_clip_duration_seconds"] = clipDuration
		}
		scenes[i] = normalized

		items = append(items, map[string]interface{}{
			"type":          "video",
			"url":           clipURL,
			"duration":      duration,
			"fit":           "contain",
			"include_audio": voiceoverDuration <= 0,
		})
		clips = append(clips, clipURL)

		voiceoverURL := ""
		if i < len(voiceovers) {
			voiceoverURL = strings.TrimSpace(voiceovers[i])
		}
		if voiceoverURL == "" {
			voiceoverURL = sceneVoiceoverURL(scene)
		}
		if voiceoverURL != "" {
			trackDuration := duration
			if voiceoverDuration > 0 {
				trackDuration = voiceoverDuration
			}
			voiceoverTrack := map[string]interface{}{
				"source_url":        voiceoverURL,
				"volume":            1.0,
				"start_time_offset": offsetSeconds,
				"duration_seconds":  trackDuration,
				"role":              "voiceover",
			}
			if key := audioTrackKey(voiceoverTrack); key == "" {
				audioTracks = append(audioTracks, voiceoverTrack)
			} else if _, exists := seenAudioTracks[key]; !exists {
				audioTracks = append(audioTracks, voiceoverTrack)
				seenAudioTracks[key] = struct{}{}
			}
			// Only layer the clip's original audio as a SECOND track
			// when the clip extends beyond the voiceover (i.e. the
			// clip duration > voiceover duration). In the canonical
			// narrated-clip shape (voiceover length drives the scene
			// duration), the clip's audio is suppressed — the legacy
			// double-track branch leaked scene_clip_audio seconds
			// that the worker's final mux step would have to roll
			// back, producing master-audio artifacts at the cross-
			// fade.
		}
		if len(subtitleTracks) == 0 {
			if track := sceneSubtitleTrack(scene); len(track) > 0 {
				subtitleTracks = append(subtitleTracks, track)
			}
		}
		offsetSeconds += duration
	}

	out["scenes"] = scenes
	if data, err := json.Marshal(scenes); err == nil {
		out["scenes_json"] = string(data)
	}
	out["items"] = items
	out["clips"] = payload.DedupeStrings(clips)
	if len(audioTracks) > 0 {
		out["audio_tracks"] = audioTracks
	}
	if len(subtitleTracks) > 0 {
		out["subtitle_tracks"] = subtitleTracks
	}
	out["video_mode"] = "clip_stock"
}
func sceneSubtitleTrack(scene map[string]interface{}) map[string]interface{} {
	if scene == nil {
		return nil
	}
	nested, ok := scene["subtitles"].(map[string]interface{})
	if !ok {
		return nil
	}
	source := strings.TrimSpace(payload.FirstString(nested, "source", "url"))
	if source == "" {
		return nil
	}
	track := map[string]interface{}{"source": source}
	if preset := strings.TrimSpace(payload.FirstString(nested, "preset")); preset != "" {
		track["preset"] = preset
	}
	if font := strings.TrimSpace(payload.FirstString(nested, "font")); font != "" {
		track["font"] = font
	}
	if format := strings.TrimSpace(payload.FirstString(nested, "format")); format != "" {
		track["format"] = format
	}
	if language := strings.TrimSpace(payload.FirstString(nested, "language")); language != "" {
		track["language"] = language
	}
	return track
}
func sceneVoiceoverDurationSeconds(scene map[string]interface{}) float64 {
	if scene == nil {
		return 0
	}
	if duration := payload.NormalizedDuration(scene["voiceover_duration_seconds"]); duration > 0 {
		return duration
	}
	if nested, ok := scene["voiceover"].(map[string]interface{}); ok {
		if duration := payload.NormalizedDuration(nested["duration_seconds"]); duration > 0 {
			return duration
		}
		if ms := payload.NormalizedDuration(nested["duration_ms"]); ms > 0 {
			return ms / 1000.0
		}
	}
	return 0
}
func sceneClipDurationSeconds(scene map[string]interface{}) float64 {
	if scene == nil {
		return 0
	}
	if duration := payload.NormalizedDuration(scene["final_clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if duration := payload.NormalizedDuration(scene["clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if nested, ok := scene["clip"].(map[string]interface{}); ok {
		if duration := payload.NormalizedDuration(nested["duration_seconds"]); duration > 0 {
			return duration
		}
		if ms := payload.NormalizedDuration(nested["duration_ms"]); ms > 0 {
			return ms / 1000.0
		}
	}
	return 0
}
