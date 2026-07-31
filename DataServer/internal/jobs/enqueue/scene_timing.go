// Package enqueue — scene timing resolution helpers.
package enqueue

import "velox-shared/payload"

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
