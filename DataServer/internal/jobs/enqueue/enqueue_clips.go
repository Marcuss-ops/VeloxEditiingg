// Package enqueue — canonical payload builder for script/generate-from-clips.
package enqueue

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-shared/contract"
	sharedmedia "velox-shared/media"
	"velox-shared/paths"
	"velox-shared/payload"

	"github.com/google/uuid"
)

// BuildClipPayloadForMaster builds the canonical script-with-clips payload.
// It accepts either a SpecScene-style `scenes` payload carrying `drive_links`
// / `clip_links`, or a flat `clips` payload. The resulting map is ready for
// Enqueuer.Enqueue and ultimately for the scene.composite worker executor.
func BuildClipPayloadForMaster(rawPayload map[string]interface{}, dataDir, videosDir, _ string) (map[string]interface{}, error) {
	videoName := payload.FirstString(rawPayload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = paths.SanitizeVideoName(payload.FirstString(rawPayload, "source_text"))
	}
	if videoName == "" {
		videoName = "generate_from_clips_" + time.Now().UTC().Format("20060102_150405")
	}

	sceneEntries, clipItems, clipURLs, audioTracks, videoMode, err := normalizeClipPayload(rawPayload)
	if err != nil {
		return nil, err
	}
	if len(clipItems) == 0 {
		return nil, fmt.Errorf("at least one clip is required")
	}

	scriptText := payload.FirstString(rawPayload, "script_text", "script", "source_text")
	if scriptText == "" {
		var parts []string
		for _, scene := range sceneEntries {
			if text := payload.FirstString(scene, "text", "description"); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			scriptText = buildScriptText(rawPayload)
		} else {
			scriptText = strings.Join(parts, "\n")
		}
	}

	voiceoverPaths := payload.NormalizeStringList(rawPayload, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")
	if len(voiceoverPaths) == 0 && len(audioTracks) > 0 {
		for _, track := range audioTracks {
			if url := payload.FirstString(track, "source_url", "url"); url != "" {
				voiceoverPaths = append(voiceoverPaths, url)
			}
		}
		voiceoverPaths = payload.DedupeStrings(voiceoverPaths)
	}

	jobID := payload.FirstString(rawPayload, "job_id", "script_id")
	if jobID == "" {
		jobID = "scriptclip_" + uuid.NewString()
	}
	jobRunID := payload.FirstString(rawPayload, "job_run_id", "run_id")
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := payload.FirstString(rawPayload, "correlation_id")
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}

	outputPath := payload.FirstString(rawPayload, "output_path")
	if outputPath == "" {
		outputPath = paths.DefaultOutputPath(videosDir, dataDir, videoName, "generate_from_clips")
	}

	audioLanguage := payload.FirstString(rawPayload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(rawPayload)+24)
	for k, v := range rawPayload {
		normalized[k] = v
	}
	for _, alias := range []string{"id", "run_id", "title", "voiceover_path", "audio_path"} {
		delete(normalized, alias)
	}

	v2 := contract.NewJobPayloadV2(normalized)
	v2.SetIdentity(jobID, jobRunID, correlationID)
	v2.VideoName = videoName
	v2.ScriptText = scriptText
	v2.Scenes = sceneEntries
	v2.ScenesJSON = payload.MustJSON(sceneEntries)
	v2.SceneCount = len(sceneEntries)
	v2.VoiceoverPaths = append([]string{}, voiceoverPaths...)
	v2.VoiceoverCount = len(voiceoverPaths)
	v2.AudioLanguage = audioLanguage
	v2.VideoMode = videoMode
	v2.OutputPath = outputPath
	v2.DriveOutput = ResolveDriveOutputFolderReference(dataDir, payload.FirstString(rawPayload, "drive_output_folder", "output_directory"))
	v2.SubmittedVia = "api_script_generate_from_clips"
	v2.Source = "script_generate_from_clips"
	v2.Version = "v2"
	v2.Status = "PENDING"
	if youtubeGroup := payload.FirstString(rawPayload, "youtube_group", "channel_id"); youtubeGroup != "" {
		v2.YoutubeGroup = youtubeGroup
		v2.ChannelID = youtubeGroup
	}

	out, err := v2.ToMap()
	if err != nil {
		return nil, err
	}
	out["clips"] = clipURLs
	out["items"] = clipItems
	if len(audioTracks) > 0 {
		out["audio_tracks"] = audioTracks
	}
	out["fit"] = payload.FirstString(rawPayload, "fit")
	if out["fit"] == "" {
		out["fit"] = "contain"
	}
	if len(voiceoverPaths) > 0 && len(audioTracks) == 0 {
		out["audio_url"] = voiceoverPaths[0]
	}
	return out, nil
}

func normalizeClipPayload(rawPayload map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
	if scenes := normalizeSceneArray(rawPayload["scenes"]); len(scenes) > 0 {
		if supportsNarratedClipScenes(scenes) {
			return buildNarratedClipPayload(scenes)
		}
		sceneEntries := make([]map[string]interface{}, 0, len(scenes))
		items := make([]map[string]interface{}, 0, len(scenes))
		clips := make([]string, 0, len(scenes))
		for i, scene := range scenes {
			url := firstClipURL(scene)
			if url == "" {
				return nil, nil, nil, nil, "", fmt.Errorf("scenes[%d]: clip url is required", i)
			}
			duration := payload.NormalizedDuration(scene["duration_seconds"])
			if duration <= 0 {
				duration = 4.0
			}

			normalized := make(map[string]interface{}, len(scene)+4)
			for k, v := range scene {
				normalized[k] = v
			}
			normalized["clip_link"] = url
			normalized["clip_links"] = []string{url}
			normalized["duration_seconds"] = duration
			if text := payload.FirstString(scene, "text", "description"); text != "" {
				normalized["text"] = text
			}

			sceneEntries = append(sceneEntries, normalized)
			items = append(items, map[string]interface{}{
				"type":     "video",
				"url":      url,
				"duration": duration,
				"fit":      "contain",
			})
			clips = append(clips, url)
		}
		return sceneEntries, items, payload.DedupeStrings(clips), nil, "clips", nil
	}

	if raw := payload.FirstString(rawPayload, "scenes_json"); raw != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, nil, nil, nil, "", fmt.Errorf("invalid scenes_json: %w", err)
		}
		return normalizeClipPayload(map[string]interface{}{"scenes": scenes})
	}

	switch rawClips := rawPayload["clips"].(type) {
	case []interface{}:
		sceneEntries := make([]map[string]interface{}, 0, len(rawClips))
		items := make([]map[string]interface{}, 0, len(rawClips))
		clips := make([]string, 0, len(rawClips))
		for i, item := range rawClips {
			switch clip := item.(type) {
			case string:
				url := strings.TrimSpace(clip)
				if url == "" {
					return nil, nil, nil, nil, "", fmt.Errorf("clips[%d]: url is required", i)
				}
				sceneEntries = append(sceneEntries, map[string]interface{}{
					"text":             fmt.Sprintf("Clip %d", i+1),
					"clip_link":        url,
					"clip_links":       []string{url},
					"duration_seconds": 4.0,
				})
				items = append(items, map[string]interface{}{
					"type":     "video",
					"url":      url,
					"duration": 4.0,
					"fit":      "contain",
				})
				clips = append(clips, url)
			case map[string]interface{}:
				url := payload.FirstString(clip, "url", "clip_link", "drive_link")
				if url == "" {
					urls := payload.NormalizeStringList(clip, "clip_links", "drive_links")
					if len(urls) > 0 {
						url = urls[0]
					}
				}
				if url == "" {
					return nil, nil, nil, nil, "", fmt.Errorf("clips[%d]: url is required", i)
				}
				duration := payload.NormalizedDuration(clip["duration"])
				if duration <= 0 {
					duration = payload.NormalizedDuration(clip["duration_seconds"])
				}
				if duration <= 0 {
					duration = 4.0
				}
				sceneEntries = append(sceneEntries, map[string]interface{}{
					"text":             payload.FirstString(clip, "text", "description"),
					"clip_link":        url,
					"clip_links":       []string{url},
					"duration_seconds": duration,
				})
				items = append(items, map[string]interface{}{
					"type":     "video",
					"url":      url,
					"duration": duration,
					"fit":      "contain",
				})
				clips = append(clips, url)
			}
		}
		if len(items) > 0 {
			return sceneEntries, items, payload.DedupeStrings(clips), nil, "clips", nil
		}
	case []string:
		return normalizeClipPayload(map[string]interface{}{"clips": toInterfaceSlice(rawClips)})
	}

	return nil, nil, nil, nil, "", fmt.Errorf("scenes, scenes_json, or clips are required")
}

func supportsNarratedClipScenes(scenes []map[string]interface{}) bool {
	for _, scene := range scenes {
		if sceneVoiceoverURL(scene) != "" {
			return true
		}
	}
	return false
}

func buildNarratedClipPayload(scenes []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}, []string, []map[string]interface{}, string, error) {
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

		voiceoverDuration := payload.NormalizedDuration(scene["voiceover_duration_seconds"])
		if voiceoverDuration <= 0 {
			voiceoverDuration = payload.NormalizedDuration(scene["duration_seconds"])
		}
		if voiceoverDuration <= 0 && voiceoverURL != "" {
			voiceoverDuration = sharedmedia.DetectAudioDurationSecs(voiceoverURL)
		}
		if voiceoverDuration <= 0 {
			voiceoverDuration = 4.0
		}

		finalClipDuration := payload.NormalizedDuration(scene["final_clip_duration_seconds"])
		if finalClipDuration <= 0 {
			finalClipDuration = payload.NormalizedDuration(scene["clip_duration_seconds"])
		}
		if finalClipDuration <= 0 {
			finalClipDuration = 4.0
		}

		normalized := make(map[string]interface{}, len(scene)+6)
		for k, v := range scene {
			normalized[k] = v
		}
		normalized["clip_link"] = finalClipURL
		normalized["clip_links"] = []string{finalClipURL}
		normalized["duration_seconds"] = voiceoverDuration
		normalized["voiceover_duration_seconds"] = voiceoverDuration
		normalized["final_clip_duration_seconds"] = finalClipDuration
		if text := payload.FirstString(scene, "text", "description"); text != "" {
			normalized["text"] = text
		}
		sceneEntries = append(sceneEntries, normalized)

		items = append(items, map[string]interface{}{
			"type":     "video",
			"url":      narrationURL,
			"duration": voiceoverDuration,
			"fit":      "contain",
			"role":     "voiceover_bed",
		})
		items = append(items, map[string]interface{}{
			"type":     "video",
			"url":      finalClipURL,
			"duration": finalClipDuration,
			"fit":      "contain",
			"role":     "scene_clip",
		})
		clips = append(clips, finalClipURL)

		if voiceoverURL != "" {
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url":        voiceoverURL,
				"volume":            1.0,
				"start_time_offset": offsetSeconds,
			})
		}
		offsetSeconds += voiceoverDuration + finalClipDuration
	}

	return sceneEntries, items, payload.DedupeStrings(clips), audioTracks, "clip_stock", nil
}

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

func sceneNarrationClipURL(scene map[string]interface{}) string {
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
	return sceneFinalClipURL(scene)
}

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

func toInterfaceSlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
