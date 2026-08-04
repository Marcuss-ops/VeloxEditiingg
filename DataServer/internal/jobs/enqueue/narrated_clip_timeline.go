// Package enqueue — narrated clip timeline builder (voiceover bed + final clip).
package enqueue

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"strings"

	sharedmedia "velox-shared/media"
	"velox-shared/payload"
)

const canonicalAssetURLPrefix = "velox-asset://"

// ErrCanonicalAssetDurationUnavailable is the stable classification for a
// canonical asset whose duration is neither declared nor probeable. Callers
// can use errors.Is while retaining the scene/asset context in the wrapped
// error message.
var ErrCanonicalAssetDurationUnavailable = errors.New("canonical asset duration unavailable")

// audioDurationProbe is injected by tests and probes canonical media URLs.
type audioDurationProbe func(string) float64

// narratedClipOptions contains only renderer controls. Asset aliases and
// ingestion fallbacks do not belong here: ingestion must resolve them into
// canonical nested assets before this builder is called.
type narratedClipOptions struct {
	probe                   audioDurationProbe
	randomSeed              string
	transitionSoundEffects  []string
	transitionSoundEffectDB float64
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
		clip, err := optionalCanonicalSceneAsset(scene, "clip")
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		clipURL := assetURL(clip)
		stock, err := canonicalStockAssets(scene)
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		for stockIndex, asset := range stock {
			stockDuration, durationErr := canonicalAssetDuration(asset, probe)
			if durationErr != nil {
				return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: stock[%d]: %w", i, stockIndex, durationErr)
			}
			setCanonicalDurationMS(asset, stockDuration)
		}
		voiceover, err := optionalCanonicalSceneAsset(scene, "voiceover")
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
		setCanonicalDurationMS(voiceover, voiceoverDuration)
		clipDuration := 0.0
		if clipURL != "" {
			clipDuration, err = resolveSceneFinalClipDurationWithProbe(scene, clipURL, probe)
			if err != nil {
				return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
			}
			setCanonicalDurationMS(clip, clipDuration)
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
					stockDuration := 0.0
					if durationMS := canonicalDurationMS(asset); durationMS > 0 {
						stockDuration = float64(durationMS) / 1000
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
		appendTransitionSoundEffects(&audioTracks, opts, offsetSeconds, voiceoverDuration, clipDuration, i, len(scenes))
		offsetSeconds += voiceoverDuration + clipDuration
	}

	return sceneEntries, items, payload.DedupeStrings(clips), audioTracks, "clip_stock", nil
}

// appendTransitionSoundEffects adds one short effect at every visual
// transition in the narrated clip/stock timeline. The timeline is:
//
//	stock (voiceover bed) -> clip -> next scene's stock -> clip ...
//
// Therefore a sound is needed at the start of each clip and at the boundary
// between scenes, but not twice at the same boundary. The selected source is
// deterministic for a given job seed, which keeps retries/cache keys stable
// while still distributing the configured pool across transitions.
func appendTransitionSoundEffects(tracks *[]map[string]interface{}, opts narratedClipOptions, offset, voiceoverDuration, clipDuration float64, sceneIndex, sceneCount int) {
	if len(opts.transitionSoundEffects) == 0 || tracks == nil {
		return
	}
	volume := 1.0
	if opts.transitionSoundEffectDB != 0 {
		volume = math.Pow(10, opts.transitionSoundEffectDB/20)
	}
	if volume <= 0 {
		volume = 0.1
	}

	appendEffect := func(at float64, transitionIndex int) {
		seed := fnv.New32a()
		_, _ = seed.Write([]byte(fmt.Sprintf("%s:sfx:%d", opts.randomSeed, transitionIndex)))
		selected := opts.transitionSoundEffects[int(seed.Sum32())%len(opts.transitionSoundEffects)]
		*tracks = append(*tracks, map[string]interface{}{
			"source_url":        selected,
			"volume":            volume,
			"start_time_offset": at,
			"role":              "sfx",
		})
	}

	// Stock/voiceover → clip.
	appendEffect(offset+voiceoverDuration, sceneIndex*2)
	// Clip → next scene's stock. This is the same instant as the next scene's
	// stock start, so the next iteration must not add another effect there.
	if sceneIndex < sceneCount-1 {
		appendEffect(offset+voiceoverDuration+clipDuration, sceneIndex*2+1)
	}
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

func setCanonicalDurationMS(asset map[string]interface{}, durationSeconds float64) {
	if asset == nil || durationSeconds <= 0 {
		return
	}
	asset["duration_ms"] = int64(math.Round(durationSeconds * 1000))
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
	return 0, fmt.Errorf("%w: duration_ms is required or asset must be probeable", ErrCanonicalAssetDurationUnavailable)
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
	return 0, fmt.Errorf("%w: voiceover duration unavailable; duration_ms is required or asset must be probeable", ErrCanonicalAssetDurationUnavailable)
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
	return 0, fmt.Errorf("%w: clip duration unavailable; duration_ms is required or asset must be probeable", ErrCanonicalAssetDurationUnavailable)
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
