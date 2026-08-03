// Package enqueue — narrated clip timeline builder (voiceover bed + final clip).
package enqueue

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"

	sharedmedia "velox-shared/media"
	"velox-shared/payload"
)

const canonicalAssetURLPrefix = "velox-asset://"

// audioDurationProbe is injected by tests and probes canonical media URLs.
type audioDurationProbe func(string) float64

// narratedClipOptions contains only renderer controls. Asset aliases and
// ingestion fallbacks do not belong here: ingestion must resolve them into
// canonical nested assets before this builder is called.
type narratedClipOptions struct {
	probe      audioDurationProbe
	randomSeed string
}

// supportsNarratedClipScenes selects the narrated path only for canonical
// scene.voiceover objects with a canonical URL.
func supportsNarratedClipScenes(scenes []map[string]interface{}) bool {
	for _, scene := range scenes {
		// Presence of the nested voiceover field selects the narrated
		// renderer even when malformed, so invalid canonical input fails
		// closed instead of bypassing strict validation through the flat
		// adapter. Legacy aliases without scene.voiceover remain outside
		// this renderer and are rejected by the canonical intake path.
		if _, present := scene["voiceover"]; present {
			return true
		}
	}
	return false
}

// buildNarratedClipPayload builds the canonical voiceover-bed + final-clip
// timeline. Every asset is read from scene.clip, scene.stock[] or
// scene.voiceover and must carry a canonical URL. Durations come from the
// nested duration_ms field or probing that URL. There is deliberately no
// synthetic four-second duration.
func buildNarratedClipPayload(scenes []map[string]interface{}, opts narratedClipOptions) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	probe := opts.probe
	if probe == nil {
		probe = sharedmedia.DetectAudioDurationSecs
	}

	sceneEntries := make([]map[string]interface{}, 0, len(scenes))
	items := make([]map[string]interface{}, 0, len(scenes)*2)
	clips := make([]string, 0, len(scenes))
	audioTracks := make([]map[string]interface{}, 0, len(scenes)*2)
	offsetSeconds := 0.0

	for i, scene := range scenes {
		clip, err := requiredCanonicalSceneAsset(scene, "clip")
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		clipURL := assetURL(clip)
		stock, err := canonicalStockAssets(scene)
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		voiceover, err := requiredCanonicalSceneAsset(scene, "voiceover")
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		voiceoverURL := assetURL(voiceover)
		if clipURL == "" && len(stock) == 0 {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: canonical clip or stock asset url is required", i)
		}

		voiceoverDuration, err := resolveSceneVoiceoverDuration(scene, voiceoverURL, probe)
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		clipDuration := 0.0
		if clipURL != "" {
			clipDuration, err = resolveSceneFinalClipDurationWithProbe(scene, clipURL, probe)
			if err != nil {
				return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
			}
		}

		// Preserve only canonical scene data at the renderer boundary.
		normalized := map[string]interface{}{}
		for _, key := range []string{"scene_id", "index", "kind", "text"} {
			if value, ok := scene[key]; ok && value != nil {
				normalized[key] = value
			}
		}
		if clip != nil {
			normalized["clip"] = clip
		}
		if len(stock) > 0 {
			normalized["stock"] = stock
		}
		if voiceover != nil {
			normalized["voiceover"] = voiceover
		}
		normalized["duration_seconds"] = voiceoverDuration + clipDuration
		sceneEntries = append(sceneEntries, normalized)

		if voiceoverURL != "" {
			bedAssets := stock
			if len(bedAssets) == 0 && clip != nil {
				bedAssets = []map[string]interface{}{clip}
			}
			bedAssets = shuffledAssets(bedAssets, opts.randomSeed, i)
			if len(bedAssets) == 1 {
				items = append(items, map[string]interface{}{
					"type": "video", "url": assetURL(bedAssets[0]),
					"duration": voiceoverDuration, "fit": "contain", "role": "voiceover_bed",
				})
			} else {
				remaining := voiceoverDuration
				for stockIndex := 0; remaining > 0; stockIndex++ {
					asset := bedAssets[stockIndex%len(bedAssets)]
					stockDuration, durationErr := canonicalAssetDuration(asset, probe)
					if durationErr != nil {
						return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: stock[%d]: %w", i, stockIndex%len(bedAssets), durationErr)
					}
					if stockDuration > remaining {
						stockDuration = remaining
					}
					items = append(items, map[string]interface{}{
						"type": "video", "url": assetURL(asset),
						"duration": stockDuration, "fit": "contain", "role": "voiceover_bed",
					})
					remaining -= stockDuration
					if stockIndex > 10000 {
						return nil, nil, nil, nil, "", fmt.Errorf("stock loop exceeded safety limit")
					}
				}
			}
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url": voiceoverURL, "volume": 1.0,
				"start_time_offset": offsetSeconds, "duration_seconds": voiceoverDuration,
				"role": "voiceover",
			})
		}

		if clipURL != "" {
			items = append(items, map[string]interface{}{
				"type": "video", "url": clipURL, "duration": clipDuration,
				"fit": "contain", "role": "scene_clip",
			})
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url": clipURL, "volume": 1.0,
				"start_time_offset": offsetSeconds + voiceoverDuration,
				"duration_seconds":  clipDuration, "role": "scene_clip_audio",
			})
			clips = append(clips, clipURL)
		}
		offsetSeconds += voiceoverDuration + clipDuration
	}

	return sceneEntries, items, payload.DedupeStrings(clips), audioTracks, "clip_stock", nil
}

func canonicalStockAssets(scene map[string]interface{}) ([]map[string]interface{}, error) {
	if scene == nil {
		return nil, nil
	}
	rawValue, present := scene["stock"]
	if !present || rawValue == nil {
		return nil, nil
	}
	var raw []interface{}
	switch value := rawValue.(type) {
	case []interface{}:
		raw = value
	case []map[string]interface{}:
		for _, item := range value {
			raw = append(raw, item)
		}
	case map[string]interface{}:
		raw = []interface{}{value}
	default:
		return nil, fmt.Errorf("canonical stock must be an array of asset objects")
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for index, value := range raw {
		asset, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("stock[%d]: canonical asset must be an object", index)
		}
		canonical := canonicalAsset(asset)
		if canonical == nil {
			return nil, fmt.Errorf("stock[%d]: canonical asset must include asset_id and url", index)
		}
		out = append(out, canonical)
	}
	return out, nil
}

func sceneAsset(scene map[string]interface{}, key string) map[string]interface{} {
	if scene == nil {
		return nil
	}
	asset, _ := scene[key].(map[string]interface{})
	return asset
}

func requiredCanonicalSceneAsset(scene map[string]interface{}, key string) (map[string]interface{}, error) {
	if scene == nil {
		return nil, nil
	}
	raw, present := scene[key]
	if !present {
		return nil, nil
	}
	if raw == nil {
		return nil, fmt.Errorf("canonical %s asset must be an object", key)
	}
	asset, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("canonical %s asset must be an object", key)
	}
	canonical := canonicalAsset(asset)
	if canonical == nil {
		return nil, fmt.Errorf("canonical %s asset must include asset_id and url", key)
	}
	return canonical, nil
}

func canonicalAsset(raw map[string]interface{}) map[string]interface{} {
	if raw == nil {
		return nil
	}
	url := strings.TrimSpace(payload.FirstString(raw, "url"))
	assetID := strings.TrimSpace(payload.FirstString(raw, "asset_id"))
	if url == "" || assetID == "" {
		return nil
	}
	prefix := canonicalAssetURLPrefix
	if !strings.HasPrefix(strings.ToLower(url), prefix) || strings.TrimPrefix(url, prefix) != assetID {
		return nil
	}
	out := map[string]interface{}{
		"asset_id": assetID,
		"url":      url,
	}
	if durationMS := canonicalDurationMS(raw); durationMS > 0 {
		out["duration_ms"] = durationMS
	}
	return out
}

func assetURL(asset map[string]interface{}) string {
	if asset == nil {
		return ""
	}
	return strings.TrimSpace(payload.FirstString(asset, "url"))
}

func canonicalDurationMS(asset map[string]interface{}) int64 {
	if asset == nil {
		return 0
	}
	value := payload.NormalizedDuration(asset["duration_ms"])
	if value <= 0 {
		return 0
	}
	return int64(value)
}

func canonicalAssetDuration(asset map[string]interface{}, probe audioDurationProbe) (float64, error) {
	if durationMS := canonicalDurationMS(asset); durationMS > 0 {
		return float64(durationMS) / 1000, nil
	}
	url := assetURL(asset)
	if url != "" && probe != nil {
		if duration := probe(url); duration > 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("duration_ms is required or asset must be probeable")
}

func shuffledAssets(assets []map[string]interface{}, seed string, sceneIndex int) []map[string]interface{} {
	out := append([]map[string]interface{}(nil), assets...)
	if len(out) < 2 {
		return out
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf("%s:%d", seed, sceneIndex)))
	rng := rand.New(rand.NewSource(int64(h.Sum64())))
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func resolveSceneVoiceoverDuration(scene map[string]interface{}, voiceoverURL string, probe audioDurationProbe) (float64, error) {
	if voiceoverURL == "" {
		return 0, nil
	}
	if voiceover := canonicalAsset(sceneAsset(scene, "voiceover")); voiceover != nil {
		if durationMS := canonicalDurationMS(voiceover); durationMS > 0 {
			return float64(durationMS) / 1000, nil
		}
	}
	if probe != nil {
		if duration := probe(voiceoverURL); duration > 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("voiceover duration unavailable; duration_ms is required or asset must be probeable")
}

func resolveSceneFinalClipDuration(scene map[string]interface{}) float64 {
	if clip := canonicalAsset(sceneAsset(scene, "clip")); clip != nil {
		if durationMS := canonicalDurationMS(clip); durationMS > 0 {
			return float64(durationMS) / 1000
		}
	}
	return 0
}

func resolveSceneFinalClipDurationWithProbe(scene map[string]interface{}, clipURL string, probe audioDurationProbe) (float64, error) {
	if duration := resolveSceneFinalClipDuration(scene); duration > 0 {
		return duration, nil
	}
	if clipURL != "" && probe != nil {
		if duration := probe(clipURL); duration > 0 {
			return duration, nil
		}
	}
	return 0, fmt.Errorf("clip duration unavailable; duration_ms is required or asset must be probeable")
}

func sceneVoiceoverURL(scene map[string]interface{}) string {
	return assetURL(canonicalAsset(sceneAsset(scene, "voiceover")))
}

func firstClipURL(scene map[string]interface{}) string {
	return assetURL(canonicalAsset(sceneAsset(scene, "clip")))
}

func sceneFinalClipURL(scene map[string]interface{}) string {
	return firstClipURL(scene)
}
