// Package remoteengine: typed DTO for remote pipeline results.
//
// Area 2 — The remote result must NOT be passed directly to the worker.
// It must first be converted into a typed DTO (RemotePipelineResult) so
// the contract between the remote engine and the Velox worker is explicit
// and verified at the adapter boundary, not scattered across handlers
// and the resolver as string-key lookups on a generic map.

package remoteengine

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/payload"
)

// ── Known remote statuses ────────────────────────────────────────────────────

// KnownRemoteStatuses is the closed set of statuses the remote engine may
// return in the initial response and in poll responses. Any status outside
// this set is a contract violation.
var KnownRemoteStatuses = map[string]bool{
	"queued":    true,
	"running":   true,
	"completed": true,
	"failed":    true,
	"cancelled": true,
}

// ── Initial response validation ──────────────────────────────────────────────

// InitialResponse is the validated result of a POST /api/script/generate-with-images
// call. The remote engine must return at least a job_id and a known status.
type InitialResponse struct {
	JobID     string
	Status    string
	RawResult map[string]interface{} // the full raw map, preserved for the async polling path
}

// ErrContractMissingJobID is the contract error when the remote response
// does not include a job_id (or trace_id / id fallback).
var ErrContractMissingJobID = &RemoteError{
	Class:   RemoteErrorPermanent,
	Code:    "CONTRACT_MISSING_JOB_ID",
	Message: "remote response missing job_id",
}

// ErrContractUnknownStatus is the contract error when the remote response
// includes a status that is not in KnownRemoteStatuses.
var ErrContractUnknownStatus = &RemoteError{
	Class:   RemoteErrorPermanent,
	Code:    "CONTRACT_UNKNOWN_STATUS",
	Message: "remote response has unknown status",
}

// ValidateInitialResponse validates that the raw remote response contains
// at least a job_id (with fallback to trace_id / id) and a known status.
// Returns a typed *RemoteError (PERMANENT, contract violation) on failure.
//
// The raw map is preserved in InitialResponse.RawResult so the caller can
// pass it to the polling path or to ParseRemotePipelineResult when the
// job is completed.
func ValidateInitialResponse(raw map[string]interface{}) (*InitialResponse, error) {
	if raw == nil {
		return nil, &RemoteError{
			Class:   RemoteErrorMalformed,
			Code:    "CONTRACT_NIL_RESPONSE",
			Message: "remote response is nil",
		}
	}

	jobID := payload.FirstString(raw, "job_id", "trace_id", "id")
	if jobID == "" {
		return nil, ErrContractMissingJobID
	}

	statusRaw, _ := raw["status"].(string)
	status := strings.ToLower(strings.TrimSpace(statusRaw))
	if status == "" {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "CONTRACT_MISSING_STATUS",
			Message: "remote response missing status",
		}
	}

	if !KnownRemoteStatuses[status] {
		return nil, &RemoteError{
			Class:   RemoteErrorPermanent,
			Code:    "CONTRACT_UNKNOWN_STATUS",
			Message: fmt.Sprintf("remote response has unknown status %q (known: queued, running, completed, failed, cancelled)", status),
			Body:    truncateBody(jsonString(raw), 4096),
			Cause:   ErrContractUnknownStatus,
		}
	}

	return &InitialResponse{
		JobID:     jobID,
		Status:    status,
		RawResult: raw,
	}, nil
}

// ── Typed DTO ────────────────────────────────────────────────────────────────

// RemotePipelineResult is the typed DTO converted from the remote engine's
// raw response map. It is the canonical shape that flows into the Velox
// worker pipeline — no caller should pass the raw map directly.
//
// Conversion is done by ParseRemotePipelineResult, which extracts and
// validates each sub-component from the flattened remote result.
type RemotePipelineResult struct {
	RemoteJobID string
	Script      ScriptResult
	Scenes      []SceneResult
	Voiceover   VoiceoverResult
	Metadata    VideoMetadata
	Assets      []AssetReference
	// Raw preserves the original map for backward-compatibility with
	// enqueue.BuildPipelinePayload which still operates on maps. Callers
	// that need the typed fields should access them directly; callers
	// that need to feed BuildPipelinePayload can use ToWorkerPayload.
	Raw map[string]interface{}
}

// ScriptResult holds the generated script text and optional markdown/JSON paths.
type ScriptResult struct {
	Text         string // the script body (markdown or plain text)
	Title        string // video title / name
	MarkdownPath string // optional path to the .md file on the remote engine's disk
	JSONPath     string // optional path to the .json file on the remote engine's disk
}

// SceneResult holds a single scene with its text and image reference.
type SceneResult struct {
	Text      string `json:"text"`
	ImageLink string `json:"image_link,omitempty"`
	// ClipLink is an alternative to ImageLink for video-clip-based scenes.
	ClipLink string `json:"clip_link,omitempty"`
	// DurationSeconds is the intended duration of the scene (0 = auto).
	DurationSeconds int `json:"duration_seconds,omitempty"`
}

// VoiceoverResult holds the voiceover audio reference(s).
type VoiceoverResult struct {
	Paths []string // local paths or URLs to voiceover audio files
}

// VideoMetadata holds the social-platform metadata for the finished video.
type VideoMetadata struct {
	Title         string   `json:"title,omitempty"`
	Description   string   `json:"description,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	PrivacyStatus string   `json:"privacy_status,omitempty"`
}

// AssetReference holds a reference to a remote asset (image, clip, etc).
type AssetReference struct {
	Type string `json:"type"` // "image", "clip", "audio", "subtitle"
	URL  string `json:"url"`
	// LocalPath is the path on the remote engine's filesystem (if any).
	LocalPath string `json:"local_path,omitempty"`
}

// ── Parsing ──────────────────────────────────────────────────────────────────

// ParseRemotePipelineResult converts a raw remote engine response map into
// the typed RemotePipelineResult DTO. It flattens the nested "result"
// envelope and extracts each sub-component with validation.
//
// This function does NOT reject incomplete results — it extracts whatever
// fields are present and leaves the rest zero-valued. The caller should
// use enqueue.ShouldForwardPipelineResult to check completeness before
// forwarding to the worker.
func ParseRemotePipelineResult(raw map[string]interface{}) (*RemotePipelineResult, error) {
	if raw == nil {
		return nil, fmt.Errorf("remoteengine: ParseRemotePipelineResult: raw map is nil")
	}

	// Flatten the nested "result" envelope (same logic as enqueue.FlattenPipelineResult).
	flat := flattenResult(raw)

	result := &RemotePipelineResult{
		RemoteJobID: payload.FirstString(flat, "job_id", "trace_id", "id"),
		Raw:         raw,
	}

	// ── Script ───────────────────────────────────────────────────────
	result.Script = ScriptResult{
		Text:         payload.FirstString(flat, "script_text", "script", "generated_script", "text"),
		Title:        payload.FirstString(flat, "video_name", "title", "script_title", "name"),
		MarkdownPath: payload.FirstString(flat, "markdown_path"),
		JSONPath:     payload.FirstString(flat, "json_path"),
	}

	// If script text is empty but a markdown_path is present, the caller
	// (enqueue.BuildPipelinePayload) will read it from disk. We don't
	// read it here because the file is on the remote engine's filesystem.

	// ── Scenes ───────────────────────────────────────────────────────
	result.Scenes = extractScenesDTO(flat)

	// ── Voiceover ────────────────────────────────────────────────────
	result.Voiceover = VoiceoverResult{
		Paths: extractVoiceoverPathsDTO(flat),
	}

	// ── Metadata ─────────────────────────────────────────────────────
	result.Metadata = extractMetadataDTO(flat)

	// ── Assets ───────────────────────────────────────────────────────
	result.Assets = extractAssetsDTO(flat)

	return result, nil
}

// ToWorkerPayload converts the typed DTO into a map[string]interface{}
// that enqueue.BuildPipelinePayload can consume. This ensures the worker
// receives a payload DERIVED from the typed DTO, not the raw remote map
// passed through unchecked.
//
// The merge strategy preserves all fields from the flattened raw map
// (so delivery_plan, output_path, and other non-DTO fields are not lost)
// while overlaying the typed DTO fields on top — the typed values take
// precedence, having been validated and normalized by ParseRemotePipelineResult.
//
// Overlaid fields include:
//   - job_id / trace_id / job_run_id / correlation_id (from RemoteJobID)
//   - video_name (from Script.Title)
//   - script_text (from Script.Text)
//   - scenes_json (serialized from Scenes)
//   - voiceover_paths (from Voiceover.Paths)
//   - video_metadata (from Metadata)
//   - json_path / markdown_path (from Script, for on-disk fallback)
func (r *RemotePipelineResult) ToWorkerPayload() map[string]interface{} {
	if r == nil {
		return map[string]interface{}{}
	}

	// Start with the flattened raw map as a base so non-DTO fields
	// (delivery_plan, output_path, etc.) are preserved.
	m := map[string]interface{}{}
	if r.Raw != nil {
		flat := flattenResult(r.Raw)
		for k, v := range flat {
			m[k] = v
		}
	}

	// Overlay typed DTO fields — these take precedence over raw values.
	if r.RemoteJobID != "" {
		m["job_id"] = r.RemoteJobID
		m["trace_id"] = r.RemoteJobID
		m["job_run_id"] = r.RemoteJobID
		m["correlation_id"] = r.RemoteJobID
	}

	if r.Script.Title != "" {
		m["video_name"] = r.Script.Title
	}
	if r.Script.Text != "" {
		m["script_text"] = r.Script.Text
	}
	if r.Script.MarkdownPath != "" {
		m["markdown_path"] = r.Script.MarkdownPath
	}
	if r.Script.JSONPath != "" {
		m["json_path"] = r.Script.JSONPath
	}

	if len(r.Scenes) > 0 {
		if scenesJSON, err := json.Marshal(r.Scenes); err == nil {
			m["scenes_json"] = string(scenesJSON)
		}
	}

	if len(r.Voiceover.Paths) > 0 {
		// BuildPipelinePayload expects voiceover_paths as []string.
		m["voiceover_paths"] = r.Voiceover.Paths
	}

	if r.Metadata.Title != "" || r.Metadata.Description != "" || len(r.Metadata.Tags) > 0 || r.Metadata.PrivacyStatus != "" {
		meta := map[string]interface{}{}
		if r.Metadata.Title != "" {
			meta["title"] = r.Metadata.Title
		}
		if r.Metadata.Description != "" {
			meta["description"] = r.Metadata.Description
		}
		if len(r.Metadata.Tags) > 0 {
			meta["tags"] = r.Metadata.Tags
		}
		if r.Metadata.PrivacyStatus != "" {
			meta["privacy_status"] = r.Metadata.PrivacyStatus
		}
		m["video_metadata"] = meta
	}

	return m
}

// ── Internal helpers ─────────────────────────────────────────────────────────

// flattenResult merges top-level keys with the nested "result" map.
// Mirrors enqueue.FlattenPipelineResult but kept local to avoid a cross-
// package dependency.
func flattenResult(result map[string]interface{}) map[string]interface{} {
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

// extractScenesDTO extracts scenes from the flat map. Supports:
//   - scenes_json string (JSON array of scene objects)
//   - scenes []interface{} (already parsed)
func extractScenesDTO(flat map[string]interface{}) []SceneResult {
	// Try scenes_json string first.
	if rawJSON := payload.FirstString(flat, "scenes_json"); rawJSON != "" {
		var scenes []SceneResult
		if err := json.Unmarshal([]byte(rawJSON), &scenes); err == nil && len(scenes) > 0 {
			return scenes
		}
		// Try as a generic []interface{}.
		var rawScenes []interface{}
		if err := json.Unmarshal([]byte(rawJSON), &rawScenes); err == nil {
			return convertRawScenes(rawScenes)
		}
	}

	// Try scenes as a parsed array.
	if rawScenes, ok := flat["scenes"].([]interface{}); ok && len(rawScenes) > 0 {
		return convertRawScenes(rawScenes)
	}

	return nil
}

// convertRawScenes converts a []interface{} of map[string]interface{}
// into typed []SceneResult.
func convertRawScenes(raw []interface{}) []SceneResult {
	scenes := make([]SceneResult, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		scene := SceneResult{
			Text:      payload.FirstString(m, "text", "description", "narration"),
			ImageLink: payload.FirstString(m, "image_link", "image_url", "image"),
			ClipLink:  payload.FirstString(m, "clip_link", "clip_url", "video_link"),
		}
		if dur, ok := m["duration_seconds"].(float64); ok {
			scene.DurationSeconds = int(dur)
		}
		scenes = append(scenes, scene)
	}
	return scenes
}

// extractVoiceoverPathsDTO extracts voiceover paths from the flat map.
// Supports multiple key shapes: voiceover_paths ([]string or []interface{}),
// voiceover_path (string), voiceover.local_path, voiceover_info.local_path.
func extractVoiceoverPathsDTO(flat map[string]interface{}) []string {
	var candidates []string

	if s := payload.FirstString(flat, "voiceover_path", "audio_path", "voiceover"); s != "" {
		candidates = append(candidates, s)
	}

	if v, ok := flat["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	if voiceover, ok := flat["voiceover"].(map[string]interface{}); ok {
		if s := payload.FirstString(voiceover, "local_path", "path", "drive_link", "url"); s != "" {
			candidates = append(candidates, s)
		}
	}

	if nested, ok := flat["voiceover_info"].(map[string]interface{}); ok {
		if s := payload.FirstString(nested, "local_path", "path", "drive_link", "url"); s != "" {
			candidates = append(candidates, s)
		}
	}

	// Dedup + trim.
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

// extractMetadataDTO extracts video metadata from the flat map.
// Supports: top-level video_metadata object, or metadata array with
// a title field (legacy remote engine shape).
func extractMetadataDTO(flat map[string]interface{}) VideoMetadata {
	var meta VideoMetadata

	if rawMeta, ok := flat["video_metadata"].(map[string]interface{}); ok {
		meta.Title = payload.FirstString(rawMeta, "title", "name")
		meta.Description = payload.FirstString(rawMeta, "description")
		if tags, ok := rawMeta["tags"].([]interface{}); ok {
			for _, t := range tags {
				if s, ok := t.(string); ok && strings.TrimSpace(s) != "" {
					meta.Tags = append(meta.Tags, strings.TrimSpace(s))
				}
			}
		}
		meta.PrivacyStatus = payload.FirstString(rawMeta, "privacy_status")
	}

	// Fallback: metadata array (legacy remote engine shape).
	if meta.Title == "" {
		if metadata, ok := flat["metadata"]; ok {
			switch v := metadata.(type) {
			case []interface{}:
				for _, item := range v {
					if m, ok := item.(map[string]interface{}); ok {
						if title := payload.FirstString(m, "title", "name"); title != "" {
							meta.Title = title
							break
						}
					}
				}
			case []map[string]interface{}:
				for _, item := range v {
					if title := payload.FirstString(item, "title", "name"); title != "" {
						meta.Title = title
						break
					}
				}
			}
		}
	}

	return meta
}

// extractAssetsDTO extracts asset references from the flat map.
// Currently the remote engine does not have a dedicated assets field,
// but scene image_link / clip_link values are collected as assets.
func extractAssetsDTO(flat map[string]interface{}) []AssetReference {
	var assets []AssetReference

	for _, scene := range extractScenesDTO(flat) {
		if scene.ImageLink != "" {
			assets = append(assets, AssetReference{
				Type: "image",
				URL:  scene.ImageLink,
			})
		}
		if scene.ClipLink != "" {
			assets = append(assets, AssetReference{
				Type: "clip",
				URL:  scene.ClipLink,
			})
		}
	}

	return assets
}

// jsonString serializes a map to a JSON string, returning "{}" on error.
func jsonString(m map[string]interface{}) string {
	if m == nil {
		return "{}"
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
