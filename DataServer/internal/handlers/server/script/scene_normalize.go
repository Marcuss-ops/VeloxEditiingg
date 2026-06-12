package script

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"velox-shared/contract"
	"velox-shared/media"
	"velox-shared/paths"
	"velox-server/internal/config"
)

func (h *ScriptHandlers) buildSceneImagePayload(cfg *config.Config, payload map[string]interface{}) (map[string]interface{}, error) {
	videoName := firstNonEmptyString(payload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = paths.SanitizeVideoName(firstNonEmptyString(payload, "topic", "source_text"))
	}
	if videoName == "" {
		videoName = "script_with_images_" + time.Now().UTC().Format("20060102_150405")
	}

	scriptText := firstNonEmptyString(payload, "script_text")
	if scriptText == "" {
		scriptText = buildScriptText(payload)
	}

	sceneEntries, sceneImagePaths, err := normalizeScenesPayload(payload)
	if err != nil {
		return nil, err
	}
	if len(sceneEntries) == 0 {
		return nil, fmt.Errorf("at least one scene or image is required")
	}

	voiceoverPaths := normalizeStringList(payload, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")
	if len(voiceoverPaths) == 0 {
		if src := firstNonEmptyString(payload, "source_text"); isLikelyMediaSource(src) {
			voiceoverPaths = []string{src}
		}
	}
	if len(voiceoverPaths) == 0 {
		return nil, fmt.Errorf("voiceover_path or source_media is required")
	}

	sceneCount := len(sceneEntries)

	totalDuration := floatFromPayload(payload, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
	perSceneDuration := floatFromPayload(payload, 0, "scene_duration_secs", "image_duration_secs")

	if perSceneDuration <= 0 && totalDuration <= 0 {
		if len(voiceoverPaths) > 0 {
			detected := media.DetectAudioDurationSecs(voiceoverPaths[0])
			if detected > 0 {
				totalDuration = detected
				log.Printf("Audio duration auto-detected: %.1fs (%.1f min) from %s", totalDuration, totalDuration/60.0, voiceoverPaths[0])
			}
		}
	}
	if perSceneDuration <= 0 && totalDuration > 0 {
		perSceneDuration = totalDuration / float64(sceneCount)
		log.Printf("Distributing audio across %d scenes: %.1fs per scene", sceneCount, perSceneDuration)
	}
	if perSceneDuration <= 0 {
		perSceneDuration = 5
	}
	if totalDuration <= 0 {
		totalDuration = perSceneDuration * float64(sceneCount)
	}

	for i := range sceneEntries {
		sceneEntries[i]["duration_seconds"] = perSceneDuration
	}

	outputPath := firstNonEmptyString(payload, "output_path")
	if outputPath == "" {
		outputPath = h.defaultOutputPath(cfg, videoName)
	}

	jobID := firstNonEmptyString(payload, "job_id", "script_id")
	if jobID == "" {
		jobID = "scriptimg_" + uuid.NewString()
	}
	jobRunID := firstNonEmptyString(payload, "job_run_id", "run_id")
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := firstNonEmptyString(payload, "correlation_id")
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}

	now := time.Now().UTC().Format(time.RFC3339)
	audioLanguage := firstNonEmptyString(payload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(payload)+24)
	for k, v := range payload {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["version"] = "v1"
	normalized["created_at"] = ensureRFC3339(firstNonEmptyString(payload, "created_at"), now)
	normalized["updated_at"] = ensureRFC3339(firstNonEmptyString(payload, "updated_at"), now)
	normalized["video_name"] = videoName
	normalized["title"] = videoName
	normalized["script_text"] = scriptText
	normalized["scenes"] = sceneEntries
	normalized["scenes_json"] = mustJSON(sceneEntries)
	normalized["voiceover_paths"] = voiceoverPaths
	normalized["voiceover_path"] = voiceoverPaths[0]
	normalized["audio_path"] = voiceoverPaths[0]
	normalized["audio_language_for_srt"] = audioLanguage
	normalized["video_mode"] = scriptSceneMode
	normalized["output_path"] = outputPath
	normalized["drive_output_folder"] = firstNonEmptyString(payload, "drive_output_folder", "output_directory")
	normalized["scene_count"] = sceneCount
	normalized["voiceover_count"] = len(voiceoverPaths)
	normalized["total_duration_secs"] = totalDuration
	normalized["scene_duration_secs"] = perSceneDuration
	normalized["scene_image_paths"] = sceneImagePaths
	normalized["priority"] = ensureInt(payload["priority"], 1)
	normalized["timeout_secs"] = ensureInt(payload["timeout_secs"], 3600)
	normalized["submitted_via"] = "api_script_generate_with_images"
	normalized["source"] = "script_generate_with_images"

	normalized["parameters"] = map[string]interface{}{
		"version":                "v1",
		"job_id":                 jobID,
		"job_run_id":             jobRunID,
		"run_id":                 jobRunID,
		"correlation_id":         correlationID,
		"job_type":               "process_video",
		"video_name":             videoName,
		"script_text":            scriptText,
		"scenes_json":            normalized["scenes_json"],
		"scenes":                 sceneEntries,
		"voiceover_paths":        voiceoverPaths,
		"voiceover_path":         voiceoverPaths[0],
		"audio_path":             voiceoverPaths[0],
		"audio_language_for_srt": audioLanguage,
		"video_mode":             scriptSceneMode,
		"output_path":            outputPath,
		"drive_output_folder":    normalized["drive_output_folder"],
		"scene_count":            sceneCount,
		"voiceover_count":        len(voiceoverPaths),
		"total_duration_secs":    totalDuration,
		"scene_duration_secs":    perSceneDuration,
		"scene_image_paths":      sceneImagePaths,
		"priority":               normalized["priority"],
		"timeout_secs":           normalized["timeout_secs"],
		"submitted_via":          "api_script_generate_with_images",
		"source":                 "script_generate_with_images",
	}

	return normalized, nil
}

func (h *ScriptHandlers) defaultOutputPath(cfg *config.Config, videoName string) string {
	videosDir := ""
	if cfg != nil {
		videosDir = cfg.VideosDir
	}
	return paths.DefaultOutputPath(videosDir, h.dataDir, videoName, "script_with_images")
}

func normalizeScenesPayload(payload map[string]interface{}) ([]map[string]interface{}, []string, error) {
	if scenes := normalizeSceneArray(payload["scenes"]); len(scenes) > 0 {
		sceneEntries := make([]map[string]interface{}, 0, len(scenes))
		sceneImagePaths := make([]string, 0, len(scenes))
		fallbacks := collectSceneImageCandidates(scenes)
		for idx, scene := range scenes {
			normalized := contract.NormalizeSceneEntry(scene)
			if image, ok := normalized["image_link"].(string); !ok || strings.TrimSpace(image) == "" {
				if len(fallbacks) > 0 {
					fallback := fallbacks[idx%len(fallbacks)]
					normalized["image_link"] = fallback
					normalized["image_links"] = []string{fallback}
				}
			}
			if image := contract.FirstSceneImageLink(normalized); image != "" {
				sceneImagePaths = append(sceneImagePaths, image)
			}
			if duration := normalizedDuration(normalized["duration_seconds"]); duration <= 0 {
				normalized["duration_seconds"] = 5.0
			}
			sceneEntries = append(sceneEntries, normalized)
		}
		return sceneEntries, dedupeStrings(sceneImagePaths), nil
	}

	if raw := firstNonEmptyString(payload, "scenes_json"); raw != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, nil, fmt.Errorf("invalid scenes_json: %w", err)
		}
		return normalizeScenesPayload(map[string]interface{}{"scenes": scenes})
	}

	images := normalizeStringList(payload, "images", "image_links", "image_urls", "image_paths")
	if len(images) == 0 {
		return nil, nil, fmt.Errorf("scenes or images are required")
	}
	sceneCount := intFromPayload(payload, len(images), "scene_count")
	if sceneCount <= 0 {
		sceneCount = len(images)
	}
	perSceneDuration := floatFromPayload(payload, 5, "scene_duration_secs", "image_duration_secs")
	totalDuration := floatFromPayload(payload, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
	if totalDuration > 0 {
		perSceneDuration = totalDuration / float64(sceneCount)
	}

	sceneEntries := make([]map[string]interface{}, 0, sceneCount)
	sceneImagePaths := make([]string, 0, sceneCount)
	for i := 0; i < sceneCount; i++ {
		img := images[i%len(images)]
		scene := map[string]interface{}{
			"text":             fmt.Sprintf("Scene %d", i+1),
			"image_link":       img,
			"image_links":      []string{img},
			"duration_seconds": perSceneDuration,
			"zoom": map[string]interface{}{
				"type":        "light_zoom_in",
				"start_scale": 1.0,
				"end_scale":   1.08,
			},
		}
		sceneEntries = append(sceneEntries, scene)
		sceneImagePaths = append(sceneImagePaths, img)
	}
	return sceneEntries, dedupeStrings(sceneImagePaths), nil
}

func normalizeSceneArray(value interface{}) []map[string]interface{} {
	switch scenes := value.(type) {
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, scene := range scenes {
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(scenes))
		for _, item := range scenes {
			scene, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			out = append(out, contract.NormalizeSceneEntry(scene))
		}
		return out
	default:
		return nil
	}
}

func collectSceneImageCandidates(scenes []map[string]interface{}) []string {
	out := make([]string, 0, len(scenes))
	for _, scene := range scenes {
		if image := contract.FirstSceneImageLink(scene); image != "" {
			out = append(out, image)
		}
	}
	return dedupeStrings(out)
}

func firstSceneImageLink(scene map[string]interface{}) string {
	return contract.FirstSceneImageLink(scene)
}

func normalizeSceneEntry(scene map[string]interface{}) map[string]interface{} {
	return contract.NormalizeSceneEntry(scene)
}

func detectAudioDurationSecs(url string) float64 {
	return media.DetectAudioDurationSecs(url)
}
