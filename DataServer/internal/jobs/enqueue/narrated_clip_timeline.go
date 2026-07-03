// Package enqueue / narrated_clip_timeline.go
//
// Voiceover-bed + final-clip timeline builder. Used by normalizeClipPayload
// when the scenes-array carries a voiceover URL. Each scene contributes up
// to TWO timeline items (voiceover bed + final clip), with offsetSeconds
// carrying consecutive scenes forward.
//
// Voiceover timing must come from an explicit `voiceover_duration_seconds`
// value on the scene or from probing the actual audio asset via the
// supplied `audioDurationProbe` (production: sharedmedia.DetectAudioDurationSecs).
// Scene-level `duration_seconds` is INTENTIONALLY ignored here because it
// is a presentation placeholder, not an audio timing contract.
package enqueue

import (
	"fmt"

	"velox-shared/payload"
	sharedmedia "velox-shared/media"
)

// audioDurationProbe resolves the voiceover audio duration for an audio
// URL when an explicit `voiceover_duration_seconds` is not provided on
// the scene. Production callers pass sharedmedia.DetectAudioDurationSecs;
// tests pass a stub like `func(string) float64 { return 4.0 }`.
//
// Returns 0 when the probe cannot determine duration (asset missing or
// unreadable). Callers must distinguish 0 from the explicit value.
type audioDurationProbe func(string) float64

// buildNarratedClipPayload is the production entry to the
// voiceover-bed + final-clip timeline. Voiceover timing is resolved via
// the default probe (sharedmedia.DetectAudioDurationSecs).
//
// Returns videoMode="clip_stock" to signal that the scene.composite
// worker must consume the per-scene audio_tracks (with start_time_offset)
// rather than a single global voiceover_path.
func buildNarratedClipPayload(scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	return buildNarratedClipPayloadWithDurationProbe(scenes, sharedmedia.DetectAudioDurationSecs)
}

// buildNarratedClipPayloadWithDurationProbe is the testable seam of
// buildNarratedClipPayload. Production callers use the wrapper above;
// tests pass a deterministic stub probe.
func buildNarratedClipPayloadWithDurationProbe(scenes []map[string]interface{}, probe audioDurationProbe) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	sceneEntries := make([]map[string]interface{}, 0, len(scenes))
	items := make([]map[string]interface{}, 0, len(scenes)*2)
	clips := make([]string, 0, len(scenes))
	audioTracks := make([]map[string]interface{}, 0, len(scenes))
	offsetSeconds := 0.0

	for i, scene := range scenes {
		narrationURL := sceneNarrationClipURL(scene)
		finalClipURL := sceneFinalClipURL(scene)
		voiceoverURL := sceneVoiceoverURL(scene)
		if finalClipURL == "" && narrationURL == "" {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: clip url is required", i)
		}
		if narrationURL == "" {
			narrationURL = finalClipURL
		}
		if finalClipURL == "" {
			finalClipURL = narrationURL
		}

		voiceoverDuration, err := resolveSceneVoiceoverDuration(scene, voiceoverURL, probe)
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		finalClipDuration := resolveSceneFinalClipDuration(scene)
		totalDuration := voiceoverDuration + finalClipDuration

		normalized := make(map[string]interface{}, len(scene)+6)
		for k, v := range scene {
			normalized[k] = v
		}
		normalized["clip_link"] = finalClipURL
		normalized["clip_links"] = []string{finalClipURL}
		normalized["duration_seconds"] = totalDuration
		normalized["voiceover_duration_seconds"] = voiceoverDuration
		normalized["final_clip_duration_seconds"] = finalClipDuration
		if text := payload.FirstString(scene, "text", "description"); text != "" {
			normalized["text"] = text
		}
		sceneEntries = append(sceneEntries, normalized)

		if voiceoverURL != "" {
			items = append(items, map[string]interface{}{
				"type":     "video",
				"url":      narrationURL,
				"duration": voiceoverDuration,
				"fit":      "contain",
				"role":     "voiceover_bed",
			})
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url":        voiceoverURL,
				"volume":            1.0,
				"start_time_offset": offsetSeconds,
			})
		}

		items = append(items, map[string]interface{}{
			"type":     "video",
			"url":      finalClipURL,
			"duration": finalClipDuration,
			"fit":      "contain",
			"role":     "scene_clip",
		})
		clips = append(clips, finalClipURL)
		offsetSeconds += totalDuration
	}

	return sceneEntries, items, payload.DedupeStrings(clips), audioTracks, "clip_stock", nil
}

// resolveSceneVoiceoverDuration returns the voiceover duration for a
// scene. Resolution order:
//  1. Explicit `voiceover_duration_seconds` on the scene.
//  2. probe(voiceoverURL) when supplied.
//
// Returns an error when neither path yields a positive duration, because
// the timeline cannot start without knowing how long the voiceover bed
// runs. A blank voiceoverURL is a non-event (duration=0; the scene
// contributes only the final clip to the timeline).
func resolveSceneVoiceoverDuration(scene map[string]interface{}, voiceoverURL string, probe audioDurationProbe) (float64, error) {
	if voiceoverURL == "" {
		return 0, nil
	}
	if duration := payload.NormalizedDuration(scene["voiceover_duration_seconds"]); duration > 0 {
		return duration, nil
	}
	if probe != nil {
		if duration := probe(voiceoverURL); duration > 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("voiceover duration unavailable for %q; provide voiceover_duration_seconds or a probeable audio asset", voiceoverURL)
}

// resolveSceneFinalClipDuration returns the final-clip duration for a
// scene. Resolution order:
//  1. Explicit `final_clip_duration_seconds` on the scene.
//  2. Legacy alias `clip_duration_seconds` (only consulted when not zero).
//  3. Default fallback 4.0.
//
// Generic `duration_seconds` is INTENTIONALLY NOT consulted: the narrated
// path computes total scene duration from voiceover + final-clip, and
// `duration_seconds` is the timeline-total stale placeholder.
func resolveSceneFinalClipDuration(scene map[string]interface{}) float64 {
	if duration := payload.NormalizedDuration(scene["final_clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if duration := payload.NormalizedDuration(scene["clip_duration_seconds"]); duration > 0 {
		return duration
	}
	return 4.0
}
