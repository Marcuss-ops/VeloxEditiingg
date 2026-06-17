// Package enqueue — canonical payload builder for script/generate-with-images.
package enqueue

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-shared/contract"
	"velox-shared/media"
	"velox-shared/paths"
	"velox-shared/payload"

	"github.com/google/uuid"
)

// =============================================================================
// Canonical payload builder: script/generate-with-images
// =============================================================================

// BuildSceneImagePayload builds a normalized process_video payload from a raw
// request body. This is the canonical builder used by the script/generate-with-images
// endpoint. It auto-detects audio duration, normalizes scenes with image fallbacks,
// and computes per-scene durations.
func BuildSceneImagePayload(rawPayload map[string]interface{}, dataDir, videosDir string) (map[string]interface{}, error) {
	return BuildSceneImagePayloadForMaster(rawPayload, dataDir, videosDir, "")
}

// BuildSceneImagePayloadForMaster builds the canonical script-with-images payload
// and stages remote voiceover assets behind the master so workers can fetch them
// from a local master URL instead of Google Drive.
func BuildSceneImagePayloadForMaster(rawPayload map[string]interface{}, dataDir, videosDir, masterURL string) (map[string]interface{}, error) {
	return buildSceneImagePayload(rawPayload, dataDir, videosDir, masterURL)
}

func buildSceneImagePayload(rawPayload map[string]interface{}, dataDir, videosDir, masterURL string) (map[string]interface{}, error) {
	videoName := payload.FirstString(rawPayload, "video_name", "title", "topic")
	if videoName == "" {
		videoName = paths.SanitizeVideoName(payload.FirstString(rawPayload, "topic", "source_text"))
	}
	if videoName == "" {
		videoName = "script_with_images_" + time.Now().UTC().Format("20060102_150405")
	}

	scriptText := payload.FirstString(rawPayload, "script_text", "script", "source_text")
	if scriptText == "" {
		scriptText = buildScriptText(rawPayload)
	}

	sceneEntries, sceneImagePaths, err := NormalizeScenesPayload(rawPayload)
	if err != nil {
		return nil, err
	}
	if len(sceneEntries) == 0 {
		return nil, fmt.Errorf("at least one scene or image is required")
	}

	voiceoverPaths := payload.NormalizeStringList(rawPayload, "voiceover_paths", "voiceover_path", "audio_path", "source_media", "source_media_url", "audio_source")
	if len(voiceoverPaths) == 0 {
		if src := payload.FirstString(rawPayload, "source_text"); payload.IsLikelyMediaSource(src) {
			voiceoverPaths = []string{src}
		}
	}
	if len(voiceoverPaths) == 0 {
		return nil, fmt.Errorf("voiceover_path or source_media is required")
	}

	sceneCount := len(sceneEntries)

	jobID := payload.FirstString(rawPayload, "job_id", "script_id")
	if jobID == "" {
		jobID = "scriptimg_" + uuid.NewString()
	}
	jobRunID := payload.FirstString(rawPayload, "job_run_id", "run_id")
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := payload.FirstString(rawPayload, "correlation_id")
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}

	totalDuration := payload.FloatParam(rawPayload, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
	perSceneDuration := payload.FloatParam(rawPayload, 0, "scene_duration_secs", "image_duration_secs")

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

	stagedVoiceoverPaths, err := stageVoiceoverAssets(dataDir, masterURL, jobID, voiceoverPaths)
	if err != nil {
		return nil, err
	}
	stagedSceneImagePaths, err := stageSceneImageAssets(dataDir, masterURL, jobID, sceneEntries, sceneImagePaths)
	if err != nil {
		return nil, err
	}

	for i := range sceneEntries {
		sceneEntries[i]["duration_seconds"] = perSceneDuration
		if i < len(stagedSceneImagePaths) && stagedSceneImagePaths[i] != "" {
			sceneEntries[i]["image_link"] = stagedSceneImagePaths[i]
			sceneEntries[i]["image_links"] = []string{stagedSceneImagePaths[i]}
		}
	}

	outputPath := payload.FirstString(rawPayload, "output_path")
	if outputPath == "" {
		outputPath = paths.DefaultOutputPath(videosDir, dataDir, videoName, "script_with_images")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	audioLanguage := payload.FirstString(rawPayload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(rawPayload)+24)
	for k, v := range rawPayload {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["version"] = "v1"
	normalized["created_at"] = payload.EnsureRFC3339(payload.FirstString(rawPayload, "created_at"), now)
	normalized["updated_at"] = payload.EnsureRFC3339(payload.FirstString(rawPayload, "updated_at"), now)
	normalized["video_name"] = videoName
	normalized["title"] = videoName
	normalized["script_text"] = scriptText
	normalized["scenes"] = sceneEntries
	normalized["scenes_json"] = payload.MustJSON(sceneEntries)
	normalized["voiceover_paths"] = stagedVoiceoverPaths
	normalized["voiceover_path"] = stagedVoiceoverPaths[0]
	normalized["audio_path"] = stagedVoiceoverPaths[0]
	normalized["audio_language_for_srt"] = audioLanguage
	normalized["video_mode"] = "scene_image"
	normalized["output_path"] = outputPath
	normalized["drive_output_folder"] = ResolveDriveOutputFolderReference(dataDir, payload.FirstString(rawPayload, "drive_output_folder", "output_directory"))
	normalized["scene_count"] = sceneCount
	normalized["voiceover_count"] = len(voiceoverPaths)
	normalized["total_duration_secs"] = totalDuration
	normalized["scene_duration_secs"] = perSceneDuration
	normalized["scene_image_paths"] = stagedSceneImagePaths
	if youtubeGroup := payload.FirstString(rawPayload, "youtube_group", "channel_id"); youtubeGroup != "" {
		normalized["youtube_group"] = youtubeGroup
		normalized["channel_id"] = youtubeGroup
	}
	normalized["priority"] = payload.EnsureInt(rawPayload["priority"], 1)
	normalized["timeout_secs"] = payload.EnsureInt(rawPayload["timeout_secs"], 3600)
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
		"voiceover_paths":        stagedVoiceoverPaths,
		"voiceover_path":         stagedVoiceoverPaths[0],
		"audio_path":             stagedVoiceoverPaths[0],
		"audio_language_for_srt": audioLanguage,
		"video_mode":             "scene_image",
		"output_path":            outputPath,
		"drive_output_folder":    normalized["drive_output_folder"],
		"scene_count":            sceneCount,
		"voiceover_count":        len(voiceoverPaths),
		"total_duration_secs":    totalDuration,
		"scene_duration_secs":    perSceneDuration,
		"scene_image_paths":      stagedSceneImagePaths,
		"youtube_group":          normalized["youtube_group"],
		"channel_id":             normalized["channel_id"],
		"priority":               normalized["priority"],
		"timeout_secs":           normalized["timeout_secs"],
		"submitted_via":          "api_script_generate_with_images",
		"source":                 "script_generate_with_images",
	}

	return normalized, nil
}

func stageVoiceoverAssets(dataDir, masterURL, jobID string, voiceoverPaths []string) ([]string, error) {
	if len(voiceoverPaths) == 0 {
		return nil, fmt.Errorf("voiceover_path or source_media is required")
	}

	staged := make([]string, 0, len(voiceoverPaths))
	if strings.TrimSpace(masterURL) == "" {
		return append(staged, voiceoverPaths...), nil
	}

	assetDir := filepath.Join(dataDir, "worker_downloads", "script_assets", jobID)
	if err := os.MkdirAll(assetDir, 0o755); err != nil {
		return nil, fmt.Errorf("create script asset dir: %w", err)
	}

	baseMasterURL := strings.TrimRight(strings.TrimSpace(masterURL), "/")
	client := &http.Client{Timeout: 90 * time.Second}
	for idx, source := range voiceoverPaths {
		filename := stagedAssetFilename("voiceover", source, idx)
		if filename == "" {
			filename = fmt.Sprintf("voiceover_%d", idx+1)
		}
		destPath := filepath.Join(assetDir, filename)
		if err := copyOrDownloadAsset(client, source, destPath); err != nil {
			return nil, fmt.Errorf("stage voiceover %d: %w", idx+1, err)
		}
		staged = append(staged, fmt.Sprintf("%s/api/worker/assets/voiceover/%s/%s", baseMasterURL, jobID, filename))
	}

	return staged, nil
}

func stageSceneImageAssets(dataDir, masterURL, jobID string, sceneEntries []map[string]interface{}, sceneImagePaths []string) ([]string, error) {
	if len(sceneImagePaths) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(masterURL) == "" {
		return append([]string{}, sceneImagePaths...), nil
	}

	assetDir := filepath.Join(dataDir, "worker_downloads", "script_assets", jobID)
	if err := os.MkdirAll(assetDir, 0o755); err != nil {
		return nil, fmt.Errorf("create script asset dir: %w", err)
	}

	baseMasterURL := strings.TrimRight(strings.TrimSpace(masterURL), "/")
	client := &http.Client{Timeout: 90 * time.Second}
	staged := make([]string, 0, len(sceneImagePaths))

	for idx, source := range sceneImagePaths {
		filename := stagedAssetFilename("scene_image", source, idx)
		if filename == "" {
			filename = fmt.Sprintf("scene_image_%d", idx+1)
		}
		destPath := filepath.Join(assetDir, filename)
		if err := copyOrDownloadAsset(client, source, destPath); err != nil {
			return nil, fmt.Errorf("stage scene image %d: %w", idx+1, err)
		}
		staged = append(staged, fmt.Sprintf("%s/api/worker/assets/scene-image/%s/%s", baseMasterURL, jobID, filename))
	}

	return staged, nil
}

func stagedAssetFilename(kind, source string, idx int) string {
	if trimmed := strings.TrimSpace(source); trimmed != "" {
		prefix := strings.TrimSpace(kind)
		if prefix == "" {
			prefix = "asset"
		}
		if _, err := os.Stat(trimmed); err == nil {
			base := filepath.Base(trimmed)
			if base != "" && base != "." && base != string(filepath.Separator) {
				return fmt.Sprintf("%s_%d_%s", prefix, idx+1, base)
			}
		}
		if u, err := neturl.Parse(trimmed); err == nil && u.Scheme != "" {
			base := filepath.Base(u.Path)
			if base != "" && base != "." && base != "uc" && base != "download" {
				return fmt.Sprintf("%s_%d_%s", prefix, idx+1, base)
			}
		}
	}
	if strings.TrimSpace(kind) == "scene_image" {
		return fmt.Sprintf("scene_image_%d", idx+1)
	}
	return fmt.Sprintf("%s_%d", strings.TrimSpace(kind), idx+1)
}

func copyOrDownloadAsset(client *http.Client, source, destPath string) error {
	if source == "" {
		return fmt.Errorf("empty source")
	}
	if info, err := os.Stat(source); err == nil && !info.IsDir() {
		input, err := os.Open(source)
		if err != nil {
			return err
		}
		defer input.Close()
		return writeStreamToFile(input, destPath)
	}

	resolved := paths.NormalizeDriveURL(source)
	req, err := http.NewRequest(http.MethodGet, resolved, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download failed: %s", resp.Status)
	}
	return writeStreamToFile(resp.Body, destPath)
}

func writeStreamToFile(r io.Reader, destPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	tmpPath := destPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, r)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// NormalizeScenesPayload normalizes a scenes payload from various input shapes.
// Supports: scenes array, scenes_json string, or flat image list with auto-scene generation.
// Returns scene entries, deduplicated image paths, and error.
func NormalizeScenesPayload(payloadMap map[string]interface{}) ([]map[string]interface{}, []string, error) {
	if scenes := normalizeSceneArray(payloadMap["scenes"]); len(scenes) > 0 {
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
			if duration := payload.NormalizedDuration(normalized["duration_seconds"]); duration <= 0 {
				normalized["duration_seconds"] = 5.0
			}
			sceneEntries = append(sceneEntries, normalized)
		}
		return sceneEntries, payload.DedupeStrings(sceneImagePaths), nil
	}

	if raw := payload.FirstString(payloadMap, "scenes_json"); raw != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err != nil {
			return nil, nil, fmt.Errorf("invalid scenes_json: %w", err)
		}
		return NormalizeScenesPayload(map[string]interface{}{"scenes": scenes})
	}

	images := payload.NormalizeStringList(payloadMap, "images", "image_links", "image_urls", "image_paths")
	if len(images) == 0 {
		return nil, nil, fmt.Errorf("scenes or images are required")
	}
	sceneCount := payload.IntParam(payloadMap, len(images), "scene_count")
	if sceneCount <= 0 {
		sceneCount = len(images)
	}
	perSceneDuration := payload.FloatParam(payloadMap, 5, "scene_duration_secs", "image_duration_secs")
	totalDuration := payload.FloatParam(payloadMap, 0, "total_duration_secs", "duration_secs", "video_duration_secs")
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
	return sceneEntries, payload.DedupeStrings(sceneImagePaths), nil
}

func collectSceneImageCandidates(scenes []map[string]interface{}) []string {
	out := make([]string, 0, len(scenes))
	for _, scene := range scenes {
		if image := contract.FirstSceneImageLink(scene); image != "" {
			out = append(out, image)
		}
	}
	return payload.DedupeStrings(out)
}

func buildScriptText(payloadMap map[string]interface{}) string {
	var parts []string
	if s := payload.FirstString(payloadMap, "topic", "title"); s != "" {
		parts = append(parts, s)
	}
	if s := payload.FirstString(payloadMap, "source_text"); s != "" {
		parts = append(parts, s)
	}
	if len(parts) == 0 {
		parts = append(parts, "script with images")
	}
	return strings.Join(parts, " - ")
}
