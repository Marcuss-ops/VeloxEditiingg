// Package enqueue — canonical payload builder for script/generate-with-images.
package enqueue

import (
	"encoding/json"
	"fmt"
	"log"
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

	audioLanguage := payload.FirstString(rawPayload, "audio_language_for_srt", "language")
	if audioLanguage == "" {
		audioLanguage = "it"
	}

	normalized := make(map[string]interface{}, len(rawPayload)+24)
	for k, v := range rawPayload {
		normalized[k] = v
	}
	// refactor/payload-v2-single-shape: strip raw-input legacy aliases
	// (`id`/`run_id`/`title`/`voiceover_path`/`audio_path`) BEFORE the
	// canonical projection so they cannot leak into the typed envelope
	// even on noisy inputs.
	for _, alias := range []string{"id", "run_id", "title", "voiceover_path", "audio_path"} {
		delete(normalized, alias)
	}

	// Build the canonical typed V2 envelope from the raw (alias-stripped)
	// input. NewJobPayloadV2 ignores the just-deleted alias keys so the
	// downstream ToMap emits only canonical fields; no `parameters`
	// sub-map mirror. The struct fields are populated from the raw map
	// directly; the post-projection assignments below finish the extra
	// fields NormalizeScenesPayload + staging already computed for us.
	v2 := contract.NewJobPayloadV2(normalized)
	v2.SetIdentity(jobID, jobRunID, correlationID)
	v2.VideoName = videoName
	v2.ScriptText = scriptText
	v2.Scenes = sceneEntries
	v2.ScenesJSON = payload.MustJSON(sceneEntries)
	v2.SceneCount = sceneCount
	v2.VoiceoverPaths = stagedVoiceoverPaths
	v2.VoiceoverCount = len(voiceoverPaths)
	v2.AudioLanguage = audioLanguage
	v2.VideoMode = "scene_image"
	v2.OutputPath = outputPath
	v2.DriveOutput = ResolveDriveOutputFolderReference(dataDir, payload.FirstString(rawPayload, "drive_output_folder", "output_directory"))
	v2.SceneImagePaths = stagedSceneImagePaths
	v2.SubmittedVia = "api_script_generate_with_images"
	v2.Source = "script_generate_with_images"
	v2.Version = "v2"
	v2.Status = "PENDING"
	v2.TotalDurationSecs = totalDuration
	v2.SceneDurationSecs = perSceneDuration
	if youtubeGroup := payload.FirstString(rawPayload, "youtube_group", "channel_id"); youtubeGroup != "" {
		v2.YoutubeGroup = youtubeGroup
		v2.ChannelID = youtubeGroup
	}

	// Single source of truth: project the typed envelope. Duration
	// fields now flow through ToMap's canonical marshaling rather than
	// being patched onto the map post-hoc — adding a new field to the
	// struct now ripples automatically to every writer.
	v2Out, err := v2.ToMap()
	if err != nil {
		return nil, err
	}
	if len(stagedSceneImagePaths) > 0 {
		v2Out["images"] = append([]string(nil), stagedSceneImagePaths...)
	}
	items := make([]map[string]interface{}, 0, len(sceneEntries))
	for _, scene := range sceneEntries {
		imageURL := payload.FirstString(scene, "image_link", "image")
		if imageURL == "" {
			continue
		}
		duration := payload.NormalizedDuration(scene["duration_seconds"])
		if duration <= 0 {
			duration = perSceneDuration
		}
		items = append(items, map[string]interface{}{
			"type":     "image",
			"url":      imageURL,
			"duration": duration,
			"fit":      "cover",
		})
	}
	if len(items) > 0 {
		v2Out["items"] = items
	}
	v2Out["pipeline_id"] = "hybrid.v1"

	// Carry through delivery_plan (canonical top-level key) from the raw
	// payload so the enqueue-time validateDeliveryPlanRequires preflight
	// passes. The V2 typed struct does not yet model this field, so ToMap
	// drops it; without this carry-through, every scene-image enqueue is
	// rejected with "delivery_plan is required".
	if dp, ok := rawPayload["delivery_plan"]; ok && dp != nil {
		v2Out["delivery_plan"] = dp
	}

	// NOTE: voiceover/scene-image rewrite is intentionally NOT invoked here.
	// The Enqueuer (constructed by the caller via NewEnqueuer) owns the
	// rewrite step and applies it in Enqueue/Submit. Doing it here too
	// would double-rewrite already-resolved paths. The pure builder stays
	// free of side effects on injected services; service dependency travels
	// downstream through the Enqueuer.
	return v2Out, nil
}

func stageVoiceoverAssets(_ /* dataDir */, _ /* masterURL */, _ /* jobID */ string, voiceoverPaths []string) ([]string, error) {
	if len(voiceoverPaths) == 0 {
		return nil, fmt.Errorf("voiceover_path or source_media is required")
	}
	// Voiceover-asset rewriting (path → velox-asset:// reference) is owned by
	// the Enqueuer now. This helper returns paths verbatim; the Enqueuer's
	// Enqueue/Submit does the rewrite as a single, idempotent step.
	return append([]string{}, voiceoverPaths...), nil
}

func stageSceneImageAssets(_ /* dataDir */, _ /* masterURL */, _ /* jobID */ string, _ /* sceneEntries */ []map[string]interface{}, sceneImagePaths []string) ([]string, error) {
	if len(sceneImagePaths) == 0 {
		return nil, nil
	}
	// Scene-image rewriting is owned by the Enqueuer (see stageVoiceoverAssets).
	return append([]string{}, sceneImagePaths...), nil
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
