package clips

import (
	"fmt"
	"math"
	"sort"
	"strings"

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
		voiceoverTracks := splitVoiceoverAroundSceneClips(track, renderPlan.AudioTracks)
		renderPlan.AudioTracks = append(renderPlan.AudioTracks, voiceoverTracks...)
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

// splitVoiceoverAroundSceneClips lays the full narration source over the
// output timeline in chunks. Scene clip audio occupies output time but does
// not consume narration source time; after each clip, narration resumes from
// the exact source offset where it paused. Consecutive clip scenes naturally
// form one longer pause.
func splitVoiceoverAroundSceneClips(voiceover plan.AudioTrack, tracks []plan.AudioTrack) []plan.AudioTrack {
	clips := make([]plan.AudioTrack, 0, len(tracks))
	for _, track := range tracks {
		if strings.EqualFold(strings.TrimSpace(track.Role), "scene_clip_audio") && track.DurationSeconds > 0 {
			clips = append(clips, track)
		}
	}
	if len(clips) == 0 || voiceover.DurationSeconds <= 0 {
		return []plan.AudioTrack{voiceover}
	}
	sort.SliceStable(clips, func(i, j int) bool {
		return clips[i].StartTimeOffset < clips[j].StartTimeOffset
	})

	segments := make([]plan.AudioTrack, 0, len(clips)+1)
	sourceOffset := 0.0
	outputOffset := 0.0
	appendSegment := func(duration float64) {
		if duration <= 1e-9 || sourceOffset >= voiceover.DurationSeconds {
			return
		}
		remaining := voiceover.DurationSeconds - sourceOffset
		if duration > remaining {
			duration = remaining
		}
		segment := voiceover
		segment.Role = "voiceover"
		segment.SourceInSeconds = sourceOffset
		segment.StartTimeOffset = outputOffset
		segment.DurationSeconds = duration
		segments = append(segments, segment)
		sourceOffset += duration
		outputOffset += duration
	}

	for _, clip := range clips {
		clipStart := math.Max(0, clip.StartTimeOffset)
		clipEnd := clip.StartTimeOffset + clip.DurationSeconds
		if clipEnd <= outputOffset {
			continue
		}
		if clipStart < outputOffset {
			clipStart = outputOffset
		}
		appendSegment(clipStart - outputOffset)
		if sourceOffset >= voiceover.DurationSeconds {
			break
		}
		if clipEnd > outputOffset {
			outputOffset = clipEnd
		}
	}
	if sourceOffset < voiceover.DurationSeconds {
		appendSegment(voiceover.DurationSeconds - sourceOffset)
	}
	if len(segments) == 0 {
		voiceover.Role = "voiceover"
		return []plan.AudioTrack{voiceover}
	}
	return segments
}

func runtimeClipAudioSceneIDs(input map[string]interface{}) map[string]bool {
	runtimeAudio := runtimeAudioPayload(input)
	selected := make(map[string]bool)
	switch values := runtimeAudio["clip_audio_scene_ids"].(type) {
	case []interface{}:
		for _, value := range values {
			if id, ok := value.(string); ok && strings.TrimSpace(id) != "" {
				selected[strings.TrimSpace(id)] = true
			}
		}
	case []string:
		for _, id := range values {
			if strings.TrimSpace(id) != "" {
				selected[strings.TrimSpace(id)] = true
			}
		}
	}
	return selected
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

// hasRuntimeFinalAudio reports whether the payload supplies a complete mix.
// When present, that track owns the timeline and source-clip audio is omitted.
func hasRuntimeFinalAudio(input map[string]interface{}) bool {
	audio := runtimeAudioPayload(input)
	return firstNonEmptyString(audio, "voiceover_asset_id", "tts_asset_id", "voice_asset_id") != ""
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
