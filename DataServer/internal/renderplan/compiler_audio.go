package renderplan

// compiler_audio.go: audio compilation for the RenderPlanCompiler — compiled
// audio_tracks and per-scene voiceover derivation.

import (
	"strings"
)

// compileAudioTracks converts payload audio_tracks into plan audio entries.
// Only tracks whose source_url is a canonical velox wire reference (or that
// carry an explicit asset_id) become plan audio; other sources stay deferred.
func compileAudioTracks(payload map[string]interface{}) []AudioTrack {
	var out []AudioTrack
	for _, track := range sliceMaps(payload["audio_tracks"]) {
		assetID, ok := assetIDOf(track, "source_url")
		if !ok {
			if bare := strings.TrimSpace(strParam(track, "asset_id")); bare != "" {
				assetID, ok = bare, true
			}
		}
		if !ok {
			continue
		}
		duration := secondsToMS(floatParam(track, "duration_seconds"))
		if duration <= 0 {
			duration = int64Param(track, "duration_ms")
		}
		out = append(out, AudioTrack{
			AssetID:     assetID,
			AssetSHA256: strParam(track, "sha256"),
			Role:        strParam(track, "role"),
			StartMS:     secondsToMS(floatParam(track, "start_time_offset")),
			DurationMS:  duration,
			Volume:      floatParam(track, "volume"),
			Loop:        boolParam(track, "loop"),
			FadeInMS:    secondsToMS(floatParam(track, "fade_in_seconds")),
			FadeOutMS:   secondsToMS(floatParam(track, "fade_out_seconds")),
		})
	}
	return out
}

// compileSceneVoiceovers derives per-scene voiceover audio tracks with the
// scene's timeline start offset.
func compileSceneVoiceovers(scenes []map[string]interface{}) []AudioTrack {
	var out []AudioTrack
	cursor := int64(0)
	for _, scene := range scenes {
		sceneDuration := sceneDurationMS(scene)
		if voiceover, ok := asMap(scene["voiceover"]); ok {
			assetID, hasID := assetIDOf(voiceover, "url")
			if !hasID {
				if bare := strings.TrimSpace(strParam(voiceover, "asset_id")); bare != "" {
					assetID, hasID = bare, true
				}
			}
			if hasID {
				duration := int64Param(voiceover, "duration_ms")
				if duration <= 0 {
					duration = sceneDuration
				}
				out = append(out, AudioTrack{
					AssetID:     assetID,
					AssetSHA256: strParam(voiceover, "sha256"),
					Role:        "voiceover",
					StartMS:     cursor,
					DurationMS:  duration,
					Volume:      1.0,
				})
			}
		}
		cursor += sceneDuration
	}
	return out
}
