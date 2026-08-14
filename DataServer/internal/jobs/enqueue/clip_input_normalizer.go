// Package enqueue — canonical clip input normalization.
package enqueue

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/assetref"
	"velox-shared/compatibility"
	sharedmedia "velox-shared/media"
	"velox-shared/payload"
)

// normalizeClipPayload accepts only canonical scene assets. A clip scene is
// represented by scene.clip, optional scene.stock[], and optional
// scene.voiceover; every asset must contain asset_id and url. The raw clips
// and alias-based adapters were intentionally retired so the renderer cannot
// reconstruct media from bindings, paths, or positional lists.
func normalizeClipPayload(rawPayload map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if rawPayload == nil {
		return nil, nil, nil, nil, "", fmt.Errorf("canonical scenes are required")
	}
	if err := rejectTopLevelNarratedAliases(rawPayload); err != nil {
		return nil, nil, nil, nil, "", err
	}

	if raw, present := rawPayload["scenes"]; present {
		scenes, err := canonicalSceneArray(raw)
		if err != nil {
			return nil, nil, nil, nil, "", err
		}
		if len(scenes) > 0 {
			return normalizeScenesInput(rawPayload, scenes)
		}
	}
	if raw := payload.FirstString(rawPayload, "scenes_json"); raw != "" {
		return normalizeScenesJSONInput(rawPayload, raw)
	}
	if _, present := rawPayload["clips"]; present {
		return nil, nil, nil, nil, "", fmt.Errorf("legacy clips input is unsupported; use scenes[].clip with asset_id, url, duration_ms")
	}
	return nil, nil, nil, nil, "", fmt.Errorf("canonical scenes or scenes_json are required")
}

func canonicalAsset(raw map[string]interface{}) map[string]interface{} {
	if raw == nil {
		return nil
	}
	assetID := strings.TrimSpace(payload.FirstString(raw, "asset_id"))
	url := strings.TrimSpace(payload.FirstString(raw, "url"))
	// The wire is self-sufficient: both velox schemes (local velox-asset://
	// and deferred velox-drive://) must carry the matching asset_id.
	wireID, isWire := assetref.WireAssetID(url)
	if assetID == "" || url == "" || !isWire || wireID != assetID {
		return nil
	}
	out := map[string]interface{}{"asset_id": assetID, "url": url}
	if sha := strings.TrimSpace(payload.FirstString(raw, "sha256")); sha != "" {
		out["sha256"] = sha
	}
	if size := payload.IntParam(raw, 0, "size_bytes"); size > 0 {
		out["size_bytes"] = size
	}
	if durationMS := canonicalDurationMS(raw); durationMS > 0 {
		out["duration_ms"] = durationMS
	}
	return out
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

func canonicalSceneArray(value interface{}) ([]map[string]interface{}, error) {
	var raw []interface{}
	switch scenes := value.(type) {
	case []interface{}:
		raw = scenes
	case []map[string]interface{}:
		for _, scene := range scenes {
			raw = append(raw, scene)
		}
	default:
		return nil, fmt.Errorf("scenes must be an array of canonical scene objects")
	}

	out := make([]map[string]interface{}, 0, len(raw))
	for index, value := range raw {
		scene, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("scenes[%d]: canonical scene must be an object", index)
		}
		canonical, err := canonicalScene(scene, index)
		if err != nil {
			return nil, err
		}
		out = append(out, canonical)
	}
	return out, nil
}

var retiredNarratedKeys = func() map[string]struct{} {
	keys := map[string]struct{}{
		"clip_link":             {},
		"clip_links":            {},
		"image_link":            {},
		"image_links":           {},
		"image":                 {},
		"drive_link":            {},
		"drive_links":           {},
		"local_path":            {},
		"bindings":              {},
		"reference_voiceover":   {},
		"reference_voiceovers":  {},
		"voiceover_link":        {},
		"stock_clip_paths":      {},
		"stock_clip_sources":    {},
		"intro_clip_paths":      {},
		"start_clip_paths":      {},
		"stock_links":           {},
		"stock_clip_links":      {},
		"clip_duration_seconds": {},
		"duration_seconds":      {},
	}
	if entry, ok := compatibility.Lookup(compatibility.VoiceoverPathsKey); ok {
		keys[entry.CanonicalKey] = struct{}{}
		for _, alias := range entry.Aliases {
			// Only top-level voiceover path/link aliases belong in this
			// retired-field set. The scene-level canonical key "voiceover"
			// is a structured asset and must remain valid.
			if strings.HasPrefix(alias, "voiceover_") {
				keys[alias] = struct{}{}
			}
		}
	}
	return keys
}()

func rejectTopLevelNarratedAliases(payloadMap map[string]interface{}) error {
	for key := range retiredNarratedKeys {
		if _, present := payloadMap[key]; present {
			return fmt.Errorf("top-level legacy field %q is unsupported; use scenes[].clip, scenes[].stock[], and scenes[].voiceover assets", key)
		}
	}
	return nil
}

func optionalCanonicalSceneAsset(scene map[string]interface{}, key string) (map[string]interface{}, error) {
	if scene == nil {
		return nil, nil
	}
	raw, present := scene[key]
	if !present || raw == nil {
		return nil, nil
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

func canonicalScene(scene map[string]interface{}, index int) (map[string]interface{}, error) {
	for key := range scene {
		// duration_seconds is a canonical scene field. The retired alias
		// list only applies to top-level legacy payloads; rejecting it here
		// would make the public scene contract impossible to normalize.
		if key == "duration_seconds" {
			switch scene[key].(type) {
			case int, int32, int64, float32, float64, json.Number:
				continue
			}
		}
		if _, retired := retiredNarratedKeys[key]; retired {
			return nil, fmt.Errorf("scenes[%d]: legacy field %q is unsupported; use clip, stock, and voiceover asset objects", index, key)
		}
	}

	clip, err := optionalCanonicalSceneAsset(scene, "clip")
	if err != nil {
		return nil, fmt.Errorf("scenes[%d]: %w", index, err)
	}
	stock, err := canonicalStockAssets(scene)
	if err != nil {
		return nil, fmt.Errorf("scenes[%d]: %w", index, err)
	}
	voiceover, err := optionalCanonicalSceneAsset(scene, "voiceover")
	if err != nil {
		return nil, fmt.Errorf("scenes[%d]: %w", index, err)
	}
	if clip == nil && len(stock) == 0 {
		return nil, fmt.Errorf("scenes[%d]: canonical clip or stock asset is required", index)
	}

	out := make(map[string]interface{}, 3)
	for _, key := range []string{"scene_id", "index", "kind", "text", "duration_seconds", "subtitles"} {
		if value, present := scene[key]; present && value != nil {
			out[key] = value
		}
	}
	if clip != nil {
		out["clip"] = clip
	}
	if len(stock) > 0 {
		out["stock"] = stock
	}
	if voiceover != nil {
		out["voiceover"] = voiceover
	}
	return out, nil
}

// normalizeScenesInput handles canonical scenes with or without a voiceover.
// Narrated scenes use the strict timeline builder; non-narrated scenes still
// use the same canonical asset vocabulary and duration rules.
func normalizeScenesInput(rawPayload map[string]interface{}, scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if supportsNarratedClipScenes(scenes) {
		sfxSources, sfxDB := transitionSoundEffectConfig(rawPayload)
		entries, items, clips, generatedAudio, mode, err := buildNarratedClipPayload(scenes, narratedClipOptions{
			randomSeed:              payload.FirstString(rawPayload, "job_id", "script_id", "video_name"),
			transitionSoundEffects:  sfxSources,
			transitionSoundEffectDB: sfxDB,
		})
		if err != nil {
			return nil, nil, nil, nil, "", err
		}
		return entries, items, clips, mergeAudioTracks(rawPayload["audio_tracks"], generatedAudio), mode, nil
	}

	probe := sharedmedia.DetectAudioDurationSecs
	for i, scene := range scenes {
		if err := validateCanonicalStockDurations(scene, probe); err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
	}
	entries := make([]map[string]interface{}, 0, len(scenes))
	items := make([]map[string]interface{}, 0, len(scenes))
	clips := make([]string, 0, len(scenes))
	for i, scene := range scenes {
		clip := sceneAsset(scene, "clip")
		url := assetURL(clip)
		duration, err := resolveSceneFinalClipDurationWithProbe(scene, url, probe)
		if err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: %w", i, err)
		}
		setCanonicalDurationMS(clip, duration)
		entry := cloneCanonicalScene(scene)
		entries = append(entries, entry)
		items = append(items, map[string]interface{}{
			"type": "video", "url": url, "duration": duration, "fit": "contain", "role": "scene_clip", "include_audio": true,
		})
		clips = append(clips, url)
	}
	return entries, items, payload.DedupeStrings(clips), nil, "clips", nil
}

func validateCanonicalStockDurations(scene map[string]interface{}, probe audioDurationProbe) error {
	stock, err := canonicalStockAssets(scene)
	if err != nil {
		return err
	}
	for index, asset := range stock {
		duration, durationErr := canonicalAssetDuration(asset, probe)
		if durationErr != nil {
			return fmt.Errorf("stock[%d]: %w", index, durationErr)
		}
		setCanonicalDurationMS(asset, duration)
	}
	return nil
}

func cloneCanonicalScene(scene map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(scene))
	for key, value := range scene {
		out[key] = value
	}
	return out
}

func transitionSoundEffectConfig(rawPayload map[string]interface{}) ([]string, float64) {
	config, ok := rawPayload["transition_sound_effects"].(map[string]interface{})
	if !ok {
		return nil, 0
	}
	if enabled, present := config["enabled"].(bool); present && !enabled {
		return nil, 0
	}
	sources := payload.ToSliceString(config["sources"])
	if len(sources) == 0 {
		return nil, 0
	}
	volumeDB := -20.0
	if _, present := config["volume_db"]; present {
		volumeDB = payload.AsFloat(config["volume_db"])
	}
	return payload.DedupeStrings(sources), volumeDB
}

func mergeAudioTracks(raw interface{}, generated []map[string]interface{}) []map[string]interface{} {
	merged := normalizeAudioTracks(raw)
	seen := make(map[string]struct{}, len(merged)+len(generated))
	unique := make([]map[string]interface{}, 0, len(merged)+len(generated))
	for _, track := range append(merged, generated...) {
		key := audioTrackKey(track)
		if key != "" {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		unique = append(unique, track)
	}
	return unique
}

func normalizeScenesJSONInput(rawPayload map[string]interface{}, scenesJSON string) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(scenesJSON), &scenes); err != nil {
		return nil, nil, nil, nil, "", fmt.Errorf("invalid scenes_json: %w", err)
	}
	return normalizeClipPayload(map[string]interface{}{"scenes": scenes, "audio_tracks": rawPayload["audio_tracks"], "job_id": rawPayload["job_id"], "video_name": rawPayload["video_name"], "transition_sound_effects": rawPayload["transition_sound_effects"]})
}
