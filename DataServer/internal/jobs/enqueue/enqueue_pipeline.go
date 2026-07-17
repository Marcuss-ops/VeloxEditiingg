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

	// PR15.6: canonical-only payload via JobPayloadV2. Legacy alias keys
	// (id/run_id/title/voiceover_path/audio_path) are emitted ONLY on the
	// HTTP edge. delivery_plan is now carried by the typed envelope itself.
	p := contract.NewJobPayloadV2(flat)
	p.VideoName = title
	p.ScriptText = scriptText
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

	return p.ToMap()
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
