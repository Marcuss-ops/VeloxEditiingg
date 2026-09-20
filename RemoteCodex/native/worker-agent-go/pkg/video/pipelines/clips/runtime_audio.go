package clips

import (
	"fmt"

	"velox-worker-agent/pkg/video/plan"
)

// appendRuntimeAudioTracks projects the runtime audio contract onto the same
// RenderPlan used by stock, clips and scene voiceover. The asset resolver has
// already replaced runtime_assets[*].url with a verified local cache path;
// this function only joins the declared runtime_audio IDs to those entries.
// Keeping this projection here means BGM, SFX and TTS use the same downloader,
// cache and native audio mixer as every other audio track.
func appendRuntimeAudioTracks(renderPlan *plan.RenderPlan, input map[string]interface{}) (*plan.RenderPlan, error) {
	if renderPlan == nil {
		return nil, fmt.Errorf("clips.v1: nil render plan")
	}
	runtimeAudio := runtimeAudioPayload(input)
	if len(runtimeAudio) == 0 {
		return renderPlan, nil
	}
	assets := runtimeAssetIndex(input["runtime_assets"])

	if id := firstNonEmptyString(runtimeAudio, "tts_asset_id", "voiceover_asset_id", "voice_asset_id"); id != "" {
		track, err := runtimeAudioTrack("tts", id, runtimeAudio, assets,
			[]string{"tts_url", "voiceover_url", "voice_url"}, 1, false)
		if err != nil {
			return nil, err
		}
		renderPlan.AudioTracks = append(renderPlan.AudioTracks, track)
	}
	if id := firstNonEmptyString(runtimeAudio, "music_asset_id", "bgm_asset_id"); id != "" {
		track, err := runtimeAudioTrack("background_music", id, runtimeAudio, assets,
			[]string{"music_url", "bgm_url"}, 0.25, true)
		if err != nil {
			return nil, err
		}
		renderPlan.AudioTracks = append(renderPlan.AudioTracks, track)
	}
	if id := firstNonEmptyString(runtimeAudio, "sfx_asset_id", "effect_asset_id"); id != "" {
		track, err := runtimeAudioTrack("sfx", id, runtimeAudio, assets,
			[]string{"sfx_url", "effect_url"}, 1, false)
		if err != nil {
			return nil, err
		}
		renderPlan.AudioTracks = append(renderPlan.AudioTracks, track)
	}
	return renderPlan, nil
}

func runtimeAudioPayload(input map[string]interface{}) map[string]interface{} {
	if input == nil {
		return nil
	}
	if payload, ok := input["runtime_audio"].(map[string]interface{}); ok {
		return payload
	}
	if nested, ok := input["runtime_payload"].(map[string]interface{}); ok {
		if payload, ok := nested["runtime_audio"].(map[string]interface{}); ok {
			return payload
		}
	}
	return nil
}

func runtimeAssetIndex(raw interface{}) map[string]map[string]interface{} {
	index := make(map[string]map[string]interface{})
	items, ok := raw.([]interface{})
	if !ok {
		if typed, typedOK := raw.([]map[string]interface{}); typedOK {
			items = make([]interface{}, 0, len(typed))
			for _, item := range typed {
				items = append(items, item)
			}
		}
	}
	for _, item := range items {
		asset, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		id := firstNonEmptyString(asset, "asset_id", "id")
		if id != "" {
			index[id] = asset
		}
	}
	return index
}

func runtimeAudioTrack(role, assetID string, runtimeAudio map[string]interface{}, assets map[string]map[string]interface{}, urlKeys []string, defaultVolume float64, loop bool) (plan.AudioTrack, error) {
	asset := assets[assetID]
	url := ""
	if asset != nil {
		url = firstNonEmptyString(asset, "url", "source_url")
	}
	if url == "" {
		url = firstNonEmptyString(runtimeAudio, urlKeys...)
	}
	if url == "" {
		return plan.AudioTrack{}, fmt.Errorf("clips.v1: runtime audio asset %q (%s) has no resolved local URL", assetID, role)
	}

	track := plan.AudioTrack{SourceURL: url, Volume: defaultVolume, Role: role, Loop: loop}
	if value := firstPositiveNumber(runtimeAudio, runtimeAudioKeys(role, "volume")); value > 0 {
		track.Volume = value
	}
	if value := firstNonNegativeNumber(runtimeAudio, append(runtimeAudioKeys(role, "start_seconds"), "start_seconds")...); value >= 0 {
		track.StartTimeOffset = value
	}
	if value := firstPositiveNumber(runtimeAudio, runtimeAudioKeys(role, "duration_seconds")); value > 0 {
		track.DurationSeconds = value
	} else if !loop && asset != nil {
		if durationMS := firstPositiveNumber(asset, []string{"duration_ms"}); durationMS > 0 {
			track.DurationSeconds = durationMS / 1000
		}
	}
	if role == "background_music" {
		track.DuckingEnabled = toBoolDefault(runtimeAudio["music_ducking"], toBoolDefault(runtimeAudio["ducking_enabled"], false))
	}
	return track, nil
}

func runtimeAudioKeys(role, suffix string) []string {
	switch role {
	case "tts":
		return []string{"tts_" + suffix, "voiceover_" + suffix, "voice_" + suffix}
	case "background_music":
		return []string{"music_" + suffix, "bgm_" + suffix}
	case "sfx":
		return []string{"sfx_" + suffix, "effect_" + suffix}
	default:
		return []string{role + "_" + suffix}
	}
}

func firstPositiveNumber(fields map[string]interface{}, keys []string) float64 {
	for _, key := range keys {
		if value := toFloat64Default(fields[key], 0); value > 0 {
			return value
		}
	}
	return 0
}

func firstNonNegativeNumber(fields map[string]interface{}, keys ...string) float64 {
	for _, key := range keys {
		if value := toFloat64Default(fields[key], -1); value >= 0 {
			return value
		}
	}
	return -1
}
