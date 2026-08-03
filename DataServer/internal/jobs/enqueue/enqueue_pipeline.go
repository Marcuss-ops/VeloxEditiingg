// Package enqueue — pipeline payload builder (remote engine → worker handoff).
package enqueue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

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
	if len(voiceovers) == 0 && !hasRenderableMedia(flat) && !hasAudioTracks(flat) {
		// A render job with no voiceover AND no renderable scene media
		// AND no audio_tracks has nothing the worker can mux. surfaced
		// here so the resolve path fails fast with an actionable
		// message rather than letting the worker burn its render
		// budget on a zero-track timeline. Mirrors the same gate
		// already in normalizeSceneVideoPayload.
		return nil, fmt.Errorf("voiceover (and no renderable scene media or audio_tracks) missing from pipeline result")
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
	p := contract.NewJobPayloadV2(flat)
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
	p.Status = "PENDING"
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
	// natively — audio_tracks, layers, and renderable
	// media keys. copyTimelinePayloadFields mirrors the same preservation
	// done in normalizeSceneVideoPayload.
	copyTimelinePayloadFields(out, flat)
	// The canonical API projection carries scenes_json, while the remote
	// hybrid executor consumes the compiled timeline as items. Build that
	// timeline here for narrated stock recipes so stock-only scenes remain
	// visual, randomised per scene, and exactly cover each voiceover.
	if strings.EqualFold(payload.FirstString(flat, "video_mode"), "clip_stock") {
		_, items, _, audioTracks, _, timelineErr := normalizeClipPayload(flat)
		if timelineErr != nil {
			return nil, fmt.Errorf("build clip-stock timeline: %w", timelineErr)
		}
		if len(items) > 0 {
			out["items"] = items
		}
		if len(audioTracks) > 0 {
			out["audio_tracks"] = audioTracks
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
	return workerPayload, nil
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
	// Forwardable when ANY audio source is present: voiceover, renderable
	// media (items/clips/images with URLs), or top-level audio_tracks
	// (background music, scene clip audio, global narration). Without at
	// least one, the worker has nothing to mux into the output AAC stream.
	if len(extractVoiceoverPaths(flat)) == 0 && !hasRenderableMedia(flat) && !hasAudioTracks(flat) {
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
		var scenes []map[string]interface{}
		if json.Unmarshal([]byte(encoded), &scenes) == nil {
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
