// Package enqueue — narrated clip timeline builder (voiceover bed + final clip).
package enqueue

import (
	"fmt"
	"strings"

	sharedmedia "velox-shared/media"
	"velox-shared/payload"
)

// audioDurationProbe is the function type for probing audio duration.
type audioDurationProbe func(string) float64

// narratedClipOptions bundles the optional knobs for the narrated-clip
// timeline builder. Top-level stock_clip_paths / intro_clip_paths from
// the raw payload act as per-scene fallback URLs for the narration
// bed when a scene does not declare its own stock_link /
// narration_clip_link. The probe field lets tests substitute the
// audio-duration detector without touching the canonical code path.
type narratedClipOptions struct {
	fallbackNarrationClipURLs []string
	probe                     audioDurationProbe
}

// sceneFallbackNarrationClipURLs returns the top-level stock paths
// declared at the payload root (rather than per-scene bindings). Used
// when a scene has no explicit `stock_link` / `narration_clip_link`
// but the operator wants each scene to borrow a stock clip from the
// payload-level pool. Priority:
//   - stock_clip_paths (preferred; alias: stock_clip_sources)
//   - intro_clip_paths (alias: start_clip_paths)
func sceneFallbackNarrationClipURLs(rawPayload map[string]interface{}) []string {
	if rawPayload == nil {
		return nil
	}
	if urls := payload.NormalizeStringList(rawPayload, "stock_clip_paths", "stock_clip_sources"); len(urls) > 0 {
		return urls
	}
	return payload.NormalizeStringList(rawPayload, "intro_clip_paths", "start_clip_paths")
}

// supportsNarratedClipScenes returns true when any scene carries a
// voiceover binding — signalling the narrated-clip timeline path.
func supportsNarratedClipScenes(scenes []map[string]interface{}) bool {
	for _, scene := range scenes {
		if sceneVoiceoverURL(scene) != "" {
			return true
		}
	}
	return false
}

// buildNarratedClipPayload is the canonical "voiceover bed + final clip"
// timeline builder. Voiceover timing must come from an explicit
// voiceover_duration_seconds value or from probing the actual audio asset.
// Generic scene duration_seconds is deliberately ignored here because it is a
// presentation placeholder, not an audio timing contract.
//
// New callers (carrying raw payloads with top-level stock_clip_paths)
// should prefer buildNarratedClipPayloadFromRaw which threads the
// fallback URL pool through narratedClipOptions.
func buildNarratedClipPayload(scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	return buildNarratedClipPayloadWithDurationProbe(scenes, sharedmedia.DetectAudioDurationSecs)
}

func buildNarratedClipPayloadWithDurationProbe(scenes []map[string]interface{}, probe audioDurationProbe) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	return buildNarratedClipPayloadWithOptions(scenes, narratedClipOptions{probe: probe})
}

// buildNarratedClipPayloadFromRaw is the entry point for callers that
// still have access to the raw top-level payload (e.g. the script/
// generate-from-clips handler). It threads top-level stock_clip_paths /
// intro_clip_paths down as per-scene fallback narration URLs so a scene
// without its own stock_link borrows from the payload pool.
func buildNarratedClipPayloadFromRaw(rawPayload map[string]interface{}, scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	return buildNarratedClipPayloadWithOptions(scenes, narratedClipOptions{
		fallbackNarrationClipURLs: sceneFallbackNarrationClipURLs(rawPayload),
		probe:                     sharedmedia.DetectAudioDurationSecs,
	})
}

// buildNarratedClipPayloadWithOptions is the merge of Velox Maintainer's
// buildNarratedClipPayloadWithDurationProbe (same iteration logic, same
// role tagging, same audio_tracks envelope) plus the per-scene
// fallback-URL pool passed via narratedClipOptions. When fallbackURLs
// is nil/short or the current scene is outside its range, the legacy
// single-arg sceneNarrationClipURL behaviour is preserved.
func buildNarratedClipPayloadWithOptions(scenes []map[string]interface{}, opts narratedClipOptions) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	sceneEntries := make([]map[string]interface{}, 0, len(scenes))
	items := make([]map[string]interface{}, 0, len(scenes)*2)
	clips := make([]string, 0, len(scenes))
	audioTracks := make([]map[string]interface{}, 0, len(scenes))
	offsetSeconds := 0.0
	probe := opts.probe
	if probe == nil {
		probe = sharedmedia.DetectAudioDurationSecs
	}

	for i, scene := range scenes {
		narrationURL := sceneNarrationClipURL(scene, opts.fallbackNarrationClipURLs, i)
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

// resolveSceneVoiceoverDuration returns the authoritative voiceover
// duration for a narrated scene. It checks:
//  1. Explicit voiceover_duration_seconds field.
//  2. Probe of the actual audio file.
//
// If the voiceover exists but is unmeasurable, it returns an error —
// generic duration_seconds is deliberately ignored here.
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

// resolveSceneFinalClipDuration returns the authoritative final clip
// duration. Canonical key: final_clip_duration_seconds. Legacy alias:
// clip_duration_seconds. Generic duration_seconds is intentionally NOT
// consulted — it is a presentation placeholder, not a clip timing contract.
func resolveSceneFinalClipDuration(scene map[string]interface{}) float64 {
	if duration := payload.NormalizedDuration(scene["final_clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if duration := payload.NormalizedDuration(scene["clip_duration_seconds"]); duration > 0 {
		return duration
	}
	return 4.0
}

// sceneVoiceoverURL extracts the voiceover audio URL from a narrated scene.
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

// sceneNarrationClipURL extracts the narration bed clip URL from a
// narrated scene. The optional fallbackURLs pool lets the caller
// supply a top-level stock_clip_paths / intro_clip_paths array whose
// i-th entry borrowed as narration bed when the scene itself does
// not declare a stock_link. Behaviour:
//
//   - scene.stock_link / scene.narration_clip_link / bindings.stock.*
//     → use it (legacy precedence, per-scene wins over pool);
//   - else, if sceneIndex is in range for fallbackURLs, use that;
//   - else, fall through to sceneFinalClipURL.
//
// The fallbackURLs+sceneIndex parameters are safe to pass as (nil, 0)
// for legacy single-frame call sites.
func sceneNarrationClipURL(scene map[string]interface{}, fallbackURLs []string, sceneIndex int) string {
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
	if sceneIndex >= 0 && sceneIndex < len(fallbackURLs) {
		if url := strings.TrimSpace(fallbackURLs[sceneIndex]); url != "" {
			return url
		}
	}
	return sceneFinalClipURL(scene)
}

// sceneFinalClipURL extracts the final clip URL from a narrated scene.
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

// firstClipURL returns the first available clip URL from a scene.
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
