// Package enqueue — pipeline payload builder (remote engine → worker handoff).
package enqueue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-shared/payload"
)

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

	// `video_name` is the technical render/job name. Never fall back to
	// video_metadata.title: publication titles belong exclusively to the
	// control-plane PublicationSpecs and must not reach the renderer.
	title := payload.FirstString(flat, "video_name")

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
	if len(voiceovers) == 0 && !hasRenderableMedia(flat) {
		// A render job with no voiceover AND no renderable scene media
		// has nothing the worker can mux. surfaced here so the resolve
		// path fails fast with an actionable message rather than letting
		// the worker burn its render budget on a zero-track timeline.
		// Mirrors the same gate already in normalizeSceneVideoPayload.
		return nil, fmt.Errorf("voiceover (and no renderable scene media) missing from pipeline result")
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

	// PR15.6: canonical-only payload via JobPayloadV2. Legacy alias keys
	// (id/run_id/title/voiceover_path/audio_path) are emitted ONLY on the
	// HTTP edge. delivery_plan is now carried by the typed envelope itself.
	p, err := contract.NewJobPayloadV2Checked(flat)
	if err != nil {
		return nil, err
	}
	p.VideoName = title
	p.ScriptText = scriptText
	// BuildPipelinePayload is also called directly by legacy/runner paths,
	// so enforce the same renderer boundary here as normalizeSceneVideoPayload
	// and remoteengine.ToWorkerPayloadChecked. Only technical render options may
	// survive; publication title/description/tags/privacy/scheduling and
	// localizations never enter the canonical worker map.
	if rawMetadata, ok := flat["video_metadata"]; ok {
		p.VideoMetadata = rendererVideoMetadata(rawMetadataMap(rawMetadata))
	}
	p.ScenesJSON = scenesJSON
	p.VoiceoverPaths = voiceovers
	p.OutputPath = payload.FirstString(flat, "output_path", "output_dir")
	p.DriveOutput = payload.FirstString(flat, "drive_output_folder", "output_directory")
	p.AudioLanguage = payload.FirstString(flat, "audio_language_for_srt", "audio_lang")
	p.SubmittedVia = "pipeline_generate_with_images"
	p.Source = "pipeline_generate_with_images"
	p.Priority = 1
	p.TimeoutSecs = 3600
	p.Status = contract.InputAssemblyPending
	p.SetIdentity(
		payload.FirstString(flat, "job_id", "script_id", "trace_id"),
		payload.FirstString(flat, "job_run_id", "run_id", "trace_id"),
		payload.FirstString(flat, "correlation_id", "trace_id"),
	)

	out, err := p.ToMap()
	if err != nil {
		return nil, err
	}
	// Preserve timeline fields that the typed V2 envelope doesn't carry
	// natively — layers and renderable media keys. copyTimelinePayloadFields
	// mirrors the same preservation done in normalizeSceneVideoPayload.
	copyTimelinePayloadFields(out, flat)
	// The typed V2 envelope intentionally stores canonical scenes_json, while
	// clips.v1 validates the renderer-facing top-level clips array. Rebuild it
	// at this final boundary so no later map projection can silently drop the
	// clip inputs.
	if !hasNonEmptySlice(out["clips"]) {
		if clips := clipsFromScenesJSON(scenesJSON); len(clips) > 0 {
			out["clips"] = clips
		}
	}
	if !hasNonEmptySlice(out["clips"]) {
		if scenesValue, ok := flat["scenes"]; ok {
			if data, marshalErr := json.Marshal(scenesValue); marshalErr == nil {
				if clips := clipsFromScenesJSON(string(data)); len(clips) > 0 {
					out["clips"] = clips
				}
			}
		}
	}
	// BuildPipelinePayload is the worker-facing projection. Keep the
	// canonical delivery envelope available to callers that extracted it
	// before this step, but never send routing/control-plane fields to the
	// renderer.
	workerPayload, err := cloneRendererPayload(out)
	if err != nil {
		return nil, fmt.Errorf("project renderer payload: %w", err)
	}
	if !hasNonEmptySlice(workerPayload["clips"]) {
		if clips := clipsFromScenesJSON(scenesJSON); len(clips) > 0 {
			workerPayload["clips"] = clips
		}
	}
	return workerPayload, nil
}

func hasNonEmptySlice(value interface{}) bool {
	switch values := value.(type) {
	case []interface{}:
		return len(values) > 0
	case []map[string]interface{}:
		return len(values) > 0
	default:
		return false
	}
}

func clipsFromScenesJSON(scenesJSON string) []interface{} {
	scenes, err := contract.ParseSceneMapsJSON([]byte(scenesJSON))
	if err != nil {
		return nil
	}
	clips := make([]interface{}, 0, len(scenes))
	for _, scene := range scenes {
		clip, ok := scene["clip"].(map[string]interface{})
		if !ok {
			continue
		}
		url := payload.FirstString(clip, "url", "drive_link", "clip_link")
		if url == "" {
			if assetID := payload.FirstString(clip, "asset_id", "drive_file_id"); assetID != "" {
				if ref, err := assetref.NewDeferredDrive(assetID); err == nil {
					url = ref.Wire()
				}
			}
		}
		if url == "" {
			continue
		}
		clips = append(clips, map[string]interface{}{
			"url":      url,
			"duration": scene["duration_seconds"],
		})
	}
	return clips
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
	// The remote-engine/creator handoff uses InputAssemblyCompleted. Do not
	// accept job-lifecycle aliases here: SUCCEEDED belongs to the Velox job
	// domain and DONE is not part of the remote-engine contract. Keeping this
	// gate strict prevents an already-terminal job status from being mistaken
	// for a producer-side completed input handoff.
	if status != "" && status != string(contract.InputAssemblyCompleted) {
		return false
	}
	if payload.FirstString(flat, "scenes_json", "json_path") == "" && payload.FirstString(flat, "scenes") == "" {
		return false
	}
	// Forwardable when ANY audio source is present: voiceover or renderable
	// media (items/clips/images with URLs). Without at least one, the
	// worker has nothing to mux into the output AAC stream.
	if len(extractVoiceoverPaths(flat)) == 0 && !hasRenderableMedia(flat) {
		return false
	}
	return true
}

func hasRenderableMedia(flat map[string]interface{}) bool {
	for _, key := range []string{"items", "clips", "images", "intro_clip_paths", "stock_clip_paths", "scene_image_paths"} {
		if values, ok := flat[key].([]interface{}); ok && len(values) > 0 {
			return true
		}
		if values, ok := flat[key].([]string); ok && len(values) > 0 {
			return true
		}
	}
	if encoded := payload.FirstString(flat, "scenes_json"); encoded != "" {
		if scenes, err := contract.ParseSceneMapsJSON([]byte(encoded)); err == nil {
			for _, scene := range scenes {
				if payload.FirstString(scene, "clip_link", "image_link") != "" {
					return true
				}
				for _, key := range []string{"clip", "image", "stock", "voiceover"} {
					if asset, ok := scene[key].(map[string]interface{}); ok && payload.FirstString(asset, "url", "asset_id") != "" {
						return true
					}
				}
			}
		}
	}
	return false
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

func rawMetadataMap(value interface{}) map[string]interface{} {
	metadata, _ := value.(map[string]interface{})
	return metadata
}
