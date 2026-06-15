// Package enqueue fornisce funzioni condivise per la normalizzazione, il building e
// l'inoltro di job video (process_video) nella coda. Usato da endpoint canonici come
// script/generate-with-images e pipeline.
package enqueue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"velox-server/internal/queue"
)

// =============================================================================
// Core enqueue entry point
// =============================================================================

// EnqueueSceneVideoJob normalizza un payload scene-video e lo persiste nella coda.
// È il punto d'ingresso condiviso per script/generate-with-images, pipeline e altri
// flussi canonici.
func EnqueueSceneVideoJob(ctx context.Context, q *queue.FileQueue, payloadMap map[string]interface{}) (map[string]interface{}, error) {
	if q == nil {
		return nil, fmt.Errorf("queue unavailable")
	}

	normalized, err := normalizeSceneVideoPayload(payloadMap)
	if err != nil {
		return nil, err
	}

	jobID, _ := normalized["job_id"].(string)
	if jobID == "" {
		jobID = uuid.NewString()
		normalized["job_id"] = jobID
	}

	if err := q.SubmitJob(ctx, jobID, normalized); err != nil {
		return nil, err
	}

	return buildSceneVideoResponse(normalized), nil
}

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
	normalized["drive_output_folder"] = payload.FirstString(rawPayload, "drive_output_folder", "output_directory")
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
		filename := stagedAssetFilename(source, idx)
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
		filename := stagedAssetFilename(source, idx)
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

func stagedAssetFilename(source string, idx int) string {
	if trimmed := strings.TrimSpace(source); trimmed != "" {
		if _, err := os.Stat(trimmed); err == nil {
			base := filepath.Base(trimmed)
			if base != "" && base != "." && base != string(filepath.Separator) {
				return base
			}
		}
		if u, err := neturl.Parse(trimmed); err == nil && u.Scheme != "" {
			base := filepath.Base(u.Path)
			if base != "" && base != "." && base != "uc" && base != "download" {
				return base
			}
		}
	}
	return fmt.Sprintf("voiceover_%d", idx+1)
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

// =============================================================================
// Pipeline payload builder (remote engine → worker handoff)
// =============================================================================

// BuildPipelinePayload builds a process_video payload from a remote pipeline
// engine result, ready for enqueue.
func BuildPipelinePayload(result map[string]interface{}) (map[string]interface{}, error) {
	if result == nil {
		return nil, fmt.Errorf("pipeline result is empty")
	}

	flat := FlattenPipelineResult(result)

	title := payload.FirstString(flat, "video_name", "title", "script_title", "name")
	if title == "" {
		title = firstMetadataTitle(flat)
	}

	scriptText := payload.FirstString(flat, "script_text", "script", "generated_script", "text")
	if scriptText == "" {
		if markdownPath := payload.FirstString(flat, "markdown_path"); markdownPath != "" {
			if data, readErr := os.ReadFile(markdownPath); readErr == nil {
				scriptText = strings.TrimSpace(string(data))
			}
		}
	}

	scenesJSON := payload.FirstString(flat, "scenes_json")
	if scenesJSON == "" {
		if scenesValue, ok := flat["scenes"]; ok {
			if data, marshalErr := json.Marshal(scenesValue); marshalErr == nil {
				scenesJSON = string(data)
			}
		}
	}
	if scenesJSON == "" {
		if jsonPath := payload.FirstString(flat, "json_path"); jsonPath != "" {
			if extracted, extractErr := extractScenesJSONFromFile(jsonPath); extractErr == nil {
				scenesJSON = extracted
			}
		}
	}

	voiceovers := extractVoiceoverPaths(flat)
	if len(voiceovers) == 0 {
		return nil, fmt.Errorf("voiceover path missing from pipeline result")
	}
	if title == "" {
		return nil, fmt.Errorf("video title missing from pipeline result")
	}
	if scriptText == "" {
		return nil, fmt.Errorf("script text missing from pipeline result")
	}
	if scenesJSON == "" {
		return nil, fmt.Errorf("scenes payload missing from pipeline result")
	}

	jobPayload := map[string]interface{}{
		"job_id":                 payload.FirstString(flat, "job_id", "script_id", "trace_id"),
		"job_run_id":             payload.FirstString(flat, "job_run_id", "run_id", "trace_id"),
		"run_id":                 payload.FirstString(flat, "run_id", "job_run_id", "trace_id"),
		"correlation_id":         payload.FirstString(flat, "correlation_id", "trace_id"),
		"video_name":             title,
		"title":                  title,
		"script_text":            scriptText,
		"scenes_json":            scenesJSON,
		"voiceover_paths":        voiceovers,
		"voiceover_path":         voiceovers[0],
		"audio_path":             voiceovers[0],
		"output_path":            payload.FirstString(flat, "output_path", "output_dir"),
		"youtube_group":          payload.FirstString(flat, "youtube_group"),
		"audio_language_for_srt": payload.FirstString(flat, "audio_language_for_srt", "audio_lang"),
		"job_type":               "process_video",
		"submitted_via":          "pipeline_generate_with_images",
		"source":                 "pipeline_generate_with_images",
		"priority":               1,
		"timeout_secs":           3600,
	}

	if jobID := strings.TrimSpace(payload.FirstString(flat, "job_id", "script_id", "trace_id")); jobID != "" {
		jobPayload["job_id"] = jobID
		jobPayload["id"] = jobID
	}
	if runID := strings.TrimSpace(payload.FirstString(flat, "job_run_id", "run_id", "trace_id")); runID != "" {
		jobPayload["job_run_id"] = runID
		jobPayload["run_id"] = runID
	}
	if corrID := strings.TrimSpace(payload.FirstString(flat, "correlation_id", "trace_id")); corrID != "" {
		jobPayload["correlation_id"] = corrID
	}
	if len(voiceovers) > 0 {
		jobPayload["voiceover_path"] = voiceovers[0]
		jobPayload["audio_path"] = voiceovers[0]
	}

	return jobPayload, nil
}

// FlattenPipelineResult flattens a nested pipeline result by merging top-level
// keys with any nested "result" map.
func FlattenPipelineResult(result map[string]interface{}) map[string]interface{} {
	flat := make(map[string]interface{}, len(result)+8)
	for k, v := range result {
		flat[k] = v
	}
	if nested, ok := result["result"].(map[string]interface{}); ok {
		for k, v := range nested {
			flat[k] = v
		}
	}
	return flat
}

// ShouldForwardPipelineResult checks whether a pipeline result is complete
// enough to be forwarded to a worker for video rendering.
func ShouldForwardPipelineResult(result map[string]interface{}) bool {
	if result == nil {
		return false
	}
	flat := FlattenPipelineResult(result)
	status := strings.ToLower(strings.TrimSpace(payload.FirstString(flat, "status")))
	if status != "" && status != "completed" && status != "succeeded" && status != "done" {
		return false
	}
	if payload.FirstString(flat, "scenes_json", "json_path") == "" && payload.FirstString(flat, "scenes") == "" {
		return false
	}
	if len(extractVoiceoverPaths(flat)) == 0 {
		return false
	}
	return true
}

// =============================================================================
// Job response formatter (shared across endpoints)
// =============================================================================

// RenderJobResponse builds a standard job response map from a raw job record.
func RenderJobResponse(job map[string]interface{}, full bool) map[string]interface{} {
	if job == nil {
		return map[string]interface{}{"ok": false}
	}
	response := map[string]interface{}{
		"ok":                  true,
		"job_id":              payload.FirstString(job, "job_id"),
		"script_id":           payload.FirstString(job, "job_id", "script_id"),
		"status":              payload.FirstString(job, "status"),
		"video_name":          payload.FirstString(job, "video_name", "title"),
		"job_run_id":          payload.FirstString(job, "job_run_id", "run_id"),
		"run_id":              payload.FirstString(job, "run_id", "job_run_id"),
		"created_at":          job["created_at"],
		"updated_at":          job["updated_at"],
		"started_at":          job["started_at"],
		"completed_at":        job["completed_at"],
		"output_path":         payload.FirstString(job, "output_path"),
		"drive_output_folder": payload.FirstString(job, "drive_output_folder"),
		"scene_count":         job["scene_count"],
		"voiceover_count":     job["voiceover_count"],
		"video_mode":          payload.FirstString(job, "video_mode"),
	}
	if errMsg := payload.FirstString(job, "error", "last_error", "error_message"); errMsg != "" {
		response["error"] = errMsg
	}
	if result := job["result"]; result != nil {
		response["result"] = result
	}
	if full {
		response["job"] = job
		response["request"] = job["request"]
	}
	return response
}

// =============================================================================
// Internal: scene video payload normalization (used by EnqueueSceneVideoJob)
// =============================================================================

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	title := strings.TrimSpace(payload.FirstString(payloadMap, "video_name", "title", "project_name"))
	if title == "" {
		return nil, &validationError{field: "video_name", message: "is required"}
	}

	scriptText := strings.TrimSpace(payload.FirstString(payloadMap, "script_text", "script", "source_text", "title", "video_name"))
	if scriptText == "" {
		scriptText = title
	}
	if scriptText == "" {
		return nil, &validationError{field: "script_text", message: "is required"}
	}

	scenesValue, scenesJSON, err := normalizeScenes(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, &validationError{field: "scenes", message: "at least one scene is required"}
	}

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return nil, &validationError{field: "voiceover_paths", message: "at least one voiceover path is required"}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	jobID := strings.TrimSpace(payload.FirstString(payloadMap, "job_id", "id"))
	if jobID == "" {
		jobID = "scene_" + uuid.NewString()
	}
	jobRunID := strings.TrimSpace(payload.FirstString(payloadMap, "job_run_id", "run_id"))
	if jobRunID == "" {
		jobRunID = "run_" + uuid.NewString()
	}
	correlationID := strings.TrimSpace(payload.FirstString(payloadMap, "correlation_id"))
	if correlationID == "" {
		correlationID = "corr_" + uuid.NewString()
	}
	jobFingerprint := sceneVideoFingerprint(
		jobID,
		title,
		scriptText,
		scenesJSON,
		voiceovers,
		payload.FirstString(payloadMap, "youtube_group"),
		payload.FirstString(payloadMap, "output_path"),
		payload.FirstString(payloadMap, "audio_language_for_srt", "audio_lang"),
	)

	normalized := make(map[string]interface{}, len(payloadMap)+16)
	for k, v := range payloadMap {
		normalized[k] = v
	}
	normalized["job_id"] = jobID
	normalized["id"] = jobID
	normalized["job_run_id"] = jobRunID
	normalized["run_id"] = jobRunID
	normalized["correlation_id"] = correlationID
	normalized["job_type"] = "process_video"
	normalized["created_at"] = payload.EnsureRFC3339(payload.FirstString(payloadMap, "created_at"), now)
	normalized["updated_at"] = payload.EnsureRFC3339(payload.FirstString(payloadMap, "updated_at"), now)
	normalized["video_name"] = title
	normalized["title"] = title
	normalized["script_text"] = scriptText
	normalized["scenes"] = scenesValue
	normalized["scenes_json"] = scenesJSON
	normalized["voiceover_paths"] = voiceovers
	normalized["voiceover_path"] = voiceovers[0]
	normalized["audio_path"] = voiceovers[0]
	normalized["priority"] = payload.EnsureInt(payloadMap["priority"], 1)
	normalized["timeout_secs"] = payload.EnsureInt(payloadMap["timeout_secs"], 3600)
	normalized["scene_count"] = len(scenesValue)
	normalized["voiceover_count"] = len(voiceovers)
	normalized["submitted_via"] = "api_v1_scene_video"
	normalized["source"] = "scene_video_api"
	normalized["job_fingerprint"] = jobFingerprint
	normalized["version"] = "v1"
	normalized["parameters"] = map[string]interface{}{
		"version":         "v1",
		"job_id":          jobID,
		"job_run_id":      jobRunID,
		"run_id":          jobRunID,
		"correlation_id":  correlationID,
		"job_type":        "process_video",
		"video_name":      title,
		"script_text":     scriptText,
		"scenes_json":     scenesJSON,
		"scenes":          scenesValue,
		"voiceover_paths": voiceovers,
		"audio_path":      voiceovers[0],
		"youtube_group":   payload.FirstString(payloadMap, "youtube_group"),
		"output_path":     payload.FirstString(payloadMap, "output_path"),
		"job_fingerprint": jobFingerprint,
		"submitted_via":   "api_v1_scene_video",
		"source":          "scene_video_api",
		"scene_count":     len(scenesValue),
		"voiceover_count": len(voiceovers),
		"priority":        payload.EnsureInt(payloadMap["priority"], 1),
		"timeout_secs":    payload.EnsureInt(payloadMap["timeout_secs"], 3600),
	}

	if v := strings.TrimSpace(payload.FirstString(payloadMap, "youtube_group")); v != "" {
		normalized["youtube_group"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_video_id")); v != "" {
		normalized["output_video_id"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "audio_language_for_srt", "audio_lang")); v != "" {
		normalized["audio_language_for_srt"] = v
		normalized["parameters"].(map[string]interface{})["audio_language_for_srt"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_path")); v != "" {
		normalized["output_path"] = v
		normalized["parameters"].(map[string]interface{})["output_path"] = v
	}
	if v := strings.TrimSpace(payload.FirstString(payloadMap, "scene_image_paths")); v != "" {
		normalized["scene_image_paths"] = v
		normalized["parameters"].(map[string]interface{})["scene_image_paths"] = v
	}

	return normalized, nil
}

func buildSceneVideoResponse(normalized map[string]interface{}) map[string]interface{} {
	jobID, _ := normalized["job_id"].(string)
	jobRunID := strings.TrimSpace(payload.FirstString(normalized, "job_run_id", "run_id"))
	correlationID := strings.TrimSpace(payload.FirstString(normalized, "correlation_id"))
	jobFingerprint := strings.TrimSpace(payload.FirstString(normalized, "job_fingerprint"))

	return map[string]interface{}{
		"ok":                true,
		"job_id":            jobID,
		"job_run_id":        jobRunID,
		"correlation_id":    correlationID,
		"job_type":          "process_video",
		"status":            "PENDING",
		"enqueue_confirmed": true,
		"dispatch_status":   "queued_for_workers",
		"scene_count":       sceneCountFromPayload(normalized),
		"voiceover_count":   voiceoverCountFromPayload(normalized),
		"job_fingerprint":   jobFingerprint,
	}
}

// =============================================================================
// Internal helpers
// =============================================================================

func normalizeScenes(payloadMap map[string]interface{}) ([]map[string]interface{}, string, error) {
	if v, ok := payloadMap["scenes"]; ok {
		switch scenes := v.(type) {
		case []interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				result = append(result, contract.NormalizeSceneEntry(m))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		case []map[string]interface{}:
			result := make([]map[string]interface{}, 0, len(scenes))
			for _, item := range scenes {
				result = append(result, contract.NormalizeSceneEntry(item))
			}
			data, err := json.Marshal(result)
			if err != nil {
				return nil, "", err
			}
			return result, string(data), nil
		}
	}

	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err != nil {
			return nil, "", err
		}
		for i := range scenes {
			scenes[i] = contract.NormalizeSceneEntry(scenes[i])
		}
		data, err := json.Marshal(scenes)
		if err != nil {
			return nil, "", err
		}
		return scenes, string(data), nil
	}

	return nil, "", nil
}

func normalizeVoiceoverList(payloadMap map[string]interface{}) []string {
	candidates := []string{
		payload.FirstString(payloadMap, "voiceover_path", "voiceover", "unified_voiceover_link"),
	}
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if v, ok := payloadMap["voiceovers_urls"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	result := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, item := range candidates {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			if _, exists := seen[trimmed]; exists {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
		}
	}
	return result
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
	return payload.DedupeStrings(out)
}

func extractVoiceoverPaths(p map[string]interface{}) []string {
	var candidates []string
	if s := payload.FirstString(p, "voiceover_path", "audio_path", "voiceover"); s != "" {
		candidates = append(candidates, s)
	}
	if v, ok := p["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}
	if voiceover, ok := p["voiceover"].(map[string]interface{}); ok {
		candidates = append(candidates,
			payload.FirstString(voiceover, "local_path", "path", "drive_link", "url"),
		)
	}
	if nested, ok := p["voiceover_info"].(map[string]interface{}); ok {
		candidates = append(candidates,
			payload.FirstString(nested, "local_path", "path", "drive_link", "url"),
		)
	}
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func extractScenesJSONFromFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var raw interface{}
	if err := json.Unmarshal(bytes.TrimSpace(data), &raw); err != nil {
		return "", err
	}
	switch v := raw.(type) {
	case map[string]interface{}:
		for _, key := range []string{"scenes_json", "scenes", "scene_plan", "scene_json"} {
			if value, ok := v[key]; ok {
				data, err := json.Marshal(value)
				if err != nil {
					return "", err
				}
				return string(data), nil
			}
		}
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(data), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}

func firstMetadataTitle(p map[string]interface{}) string {
	metadata, ok := p["metadata"]
	if !ok {
		return ""
	}
	switch v := metadata.(type) {
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				if title := payload.FirstString(m, "title", "name"); title != "" {
					return title
				}
			}
		}
	case []map[string]interface{}:
		for _, item := range v {
			if title := payload.FirstString(item, "title", "name"); title != "" {
				return title
			}
		}
	}
	return ""
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

func sceneCountFromPayload(payloadMap map[string]interface{}) int {
	if scenes, ok := payloadMap["scenes"].([]interface{}); ok {
		return len(scenes)
	}
	if scenes, ok := payloadMap["scenes"].([]map[string]interface{}); ok {
		return len(scenes)
	}
	if s, ok := payloadMap["scenes_json"].(string); ok && strings.TrimSpace(s) != "" {
		var scenes []interface{}
		if err := json.Unmarshal([]byte(s), &scenes); err == nil {
			return len(scenes)
		}
	}
	return 0
}

func voiceoverCountFromPayload(payloadMap map[string]interface{}) int {
	if arr, ok := payloadMap["voiceover_paths"].([]string); ok {
		return len(arr)
	}
	if arr, ok := payloadMap["voiceover_paths"].([]interface{}); ok {
		return len(arr)
	}
	return len(normalizeVoiceoverList(payloadMap))
}

func sceneVideoFingerprint(parts ...interface{}) string {
	h := sha256.New()
	for _, part := range parts {
		switch v := part.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				h.Write([]byte(trimmed))
			}
		case []string:
			for _, item := range v {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					h.Write([]byte(trimmed))
				}
			}
		default:
			if part == nil {
				continue
			}
			if data, err := json.Marshal(part); err == nil {
				h.Write(data)
			}
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

type validationError struct {
	field   string
	message string
}

func (e *validationError) Error() string {
	return e.field + ": " + e.message
}
