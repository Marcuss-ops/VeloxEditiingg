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
	"strconv"
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

// ClipAsset is the per-scene clip asset reference typed DTO.
// Mirrors apiwire.SubmitClip (without validate tags — typed-DTO
// layer does not own wire validation, the SubmitJob handler does).
//
// Phase 2 of the render-manifest plan: scene.Clip carries the
// authoritative clip URL directly. Worker reads it from
// scenes_json[i].clip.url — no more positional coupling with
// voiceover_paths[].
type ClipAsset struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	StartMS     int64  `json:"start_ms,omitempty"`
	EndMS       int64  `json:"end_ms,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
}

// VoiceoverAsset is the per-scene voiceover asset reference typed DTO.
// Mirrors apiwire.SubmitVoiceover. The nested form REPLACES the legacy
// position-coupled voiceover_paths[N] ↔ scenes[N] relationship: a
// single scene carries its own voiceover URL directly. See
// ToWorkerPayload for the merge-into-voiceover_paths[] back-compat
// strategy that keeps legacy worker consumers working.
type VoiceoverAsset struct {
	AssetID     string `json:"asset_id,omitempty"`
	DriveFileID string `json:"drive_file_id,omitempty"`
	URL         string `json:"url,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Language    string `json:"language,omitempty"`
}

// SubtitlesAsset is the per-scene subtitles asset reference typed DTO.
// Mirrors apiwire.SubmitSubtitles.
type SubtitlesAsset struct {
	AssetID  string `json:"asset_id,omitempty"`
	Format   string `json:"format,omitempty"`
	URL      string `json:"url,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Language string `json:"language,omitempty"`
}

// SceneResult holds a single scene with its text and image reference.
//
// Phase 2 of the render-manifest plan: the per-scene Clip / Voiceover
// / Subtitles nested objects are the canonical SOURCE OF TRUTH for
// asset URLs. The legacy top-level VoiceoverResult.Paths is preserved
// for back-compat with the creator-machine wire shape (and to keep
// the typed DTO surface stable for tests); ToWorkerPayload merges
// both sources into the worker payload (per-scene voiceover.url
// FIRST, top-level Paths second, deduped by URL).
type SceneResult struct {	Text           string           `json:"text"`
	SceneID        string           `json:"scene_id,omitempty"`
	Index          int64            `json:"index,omitempty"`
	Kind           string           `json:"kind,omitempty"`
	ImageLink      string           `json:"image_link,omitempty"`
	// ClipLink is an alternative to ImageLink for video-clip-based scenes.
	ClipLink string `json:"clip_link,omitempty"`
	// DurationSeconds is the intended duration of the scene in seconds.
	// The OpenAPI contract on SubmitScene enforces 0.1 <= duration_seconds
	// <= 86400; the type is float64 so sub-second values (e.g. 0.1) survive
	// the JSON round-trip WITHOUT truncation. An int type would silently
	// turn "0.1" into "0" via the float64->int cast, an explicit
	// cross-package dependency that was neutralised when the SubmitJob
	// contract adopted sub-second durations for fine-grained scene cuts.
	DurationSeconds float64          `json:"duration_seconds,omitempty"`
	Clip            *ClipAsset       `json:"clip,omitempty"`
	Voiceover       *VoiceoverAsset  `json:"voiceover,omitempty"`
	Subtitles       *SubtitlesAsset  `json:"subtitles,omitempty"`
}

// VoiceoverResult holds the voiceover audio reference(s).
//
// Phase 2 of the render-manifest plan: the per-scene voiceover.url
// is the canonical SOURCE OF TRUTH (a single scene carries its own
// voiceover URL). The top-level Paths field is preserved for
// back-compat with the creator-machine wire shape — both inputs
// are merged into the worker payload (per-scene voiceover.url first,
// top-level Paths second, deduped by URL) by ToWorkerPayload.
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

	// Phase-2 voiceover_paths[] merge strategy (Phase 2 of the
	// render-manifest plan): the per-scene voiceover.url is the
	// SOURCE OF TRUTH, but the legacy worker consumer still reads
	// from the top-level voiceover_paths[] array. To keep both
	// paths consistent we merge both sources into a single deduped
	// array, with per-scene URLs FIRST (the authoritative source)
	// and top-level r.Voiceover.Paths SECOND (preserved for the
	// creator-machine path that doesn't yet use per-scene nested).
	//
	// Note: scenes_json is set above regardless of voiceover; the
	// new worker consumers should read from scenes_json[i].voiceover.url
	// directly (no positional coupling). The merged voiceover_paths[]
	// is purely a back-compat shim for legacy worker code.
	if merged := mergeVoiceoverPaths(r); len(merged) > 0 {
		m["voiceover_paths"] = merged
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
//
// Phase 2 of the render-manifest plan: scene_id / index / kind /
// clip{} / voiceover{} / subtitles{} nested objects are read from
// the flat-map raw input so the typed DTO carries the canonical
// per-scene enrichment. The flat clip_link / image_link keys
// remain supported for back-compat with legacy creator outputs.
func convertRawScenes(raw []interface{}) []SceneResult {
	scenes := make([]SceneResult, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		scene := SceneResult{
			Text:      payload.FirstString(m, "text", "description", "narration"),
			SceneID:   payload.FirstString(m, "scene_id"),
			Index:     intFromAnyMap(m["index"]),
			Kind:      payload.FirstString(m, "kind"),
			ImageLink: payload.FirstString(m, "image_link", "image_url", "image"),
			ClipLink:  payload.FirstString(m, "clip_link", "clip_url", "video_link"),
		}
		if dur, ok := m["duration_seconds"].(float64); ok {
			scene.DurationSeconds = dur
		}
		if clip, ok := m["clip"].(map[string]interface{}); ok {
			scene.Clip = convertClipAsset(clip)
		}
		if vo, ok := m["voiceover"].(map[string]interface{}); ok {
			scene.Voiceover = convertVoiceoverAsset(vo)
		}
		if sub, ok := m["subtitles"].(map[string]interface{}); ok {
			scene.Subtitles = convertSubtitlesAsset(sub)
		}
		scenes = append(scenes, scene)
	}
	return scenes
}

// intFromAnyMap coerces an arbitrary JSON-decoded value (int /
// int64 / float64 / string with numeric content / nil) into a Go int64.
// Returns 0 for unknown shapes so the caller can treat 0 as "absent".
// Used by convertRawScenes for the new scene.Index field and the
// ClipAsset.StartMS / EndMS / DurationMS / VoiceoverAsset.DurationMS
// fields — JSON numbers are decoded as float64 by encoding/json,
// so the typed fields need explicit coercion. int64 (rather than
// int) is the canonical Go type for millisecond durations so the
// renderer can compose 64-bit timestamps without overflow up to
// ~292M years.
func intFromAnyMap(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i
		}
	}
	return 0
}

func convertClipAsset(m map[string]interface{}) *ClipAsset {
	if m == nil {
		return nil
	}
	return &ClipAsset{
		AssetID:     payload.FirstString(m, "asset_id"),
		DriveFileID: payload.FirstString(m, "drive_file_id"),
		URL:         payload.FirstString(m, "url"),
		SHA256:      payload.FirstString(m, "sha256"),
		StartMS:     intFromAnyMap(m["start_ms"]),
		EndMS:       intFromAnyMap(m["end_ms"]),
		DurationMS:  intFromAnyMap(m["duration_ms"]),
	}
}

func convertVoiceoverAsset(m map[string]interface{}) *VoiceoverAsset {
	if m == nil {
		return nil
	}
	return &VoiceoverAsset{
		AssetID:     payload.FirstString(m, "asset_id"),
		DriveFileID: payload.FirstString(m, "drive_file_id"),
		URL:         payload.FirstString(m, "url"),
		SHA256:      payload.FirstString(m, "sha256"),
		DurationMS:  intFromAnyMap(m["duration_ms"]),
		Language:    payload.FirstString(m, "language"),
	}
}

func convertSubtitlesAsset(m map[string]interface{}) *SubtitlesAsset {
	if m == nil {
		return nil
	}
	return &SubtitlesAsset{
		AssetID:  payload.FirstString(m, "asset_id"),
		Format:   payload.FirstString(m, "format"),
		URL:      payload.FirstString(m, "url"),
		SHA256:   payload.FirstString(m, "sha256"),
		Language: payload.FirstString(m, "language"),
	}
}

// mergeVoiceoverPaths produces the merged voiceover_paths[] for the
// worker payload (Phase-2 back-compat strategy). Per-scene URLs
// (scenes[i].voiceover.url) come FIRST (authoritative source); the
// top-level r.Voiceover.Paths come SECOND (legacy creator-machine
// source); duplicates (same trimmed URL) are deduped. Returns nil
// when no source supplies any URL — the worker payload then has no
// voiceover_paths key at all (vs. an empty array, which would
// surface as a falsy check on legacy worker consumers).
func mergeVoiceoverPaths(r *RemotePipelineResult) []string {
	seen := map[string]struct{}{}
	var merged []string

	// Per-scene voiceover URLs first.
	for _, s := range r.Scenes {
		if s.Voiceover == nil {
			continue
		}
		if trimmed := strings.TrimSpace(s.Voiceover.URL); trimmed != "" {
			if _, dup := seen[trimmed]; !dup {
				seen[trimmed] = struct{}{}
				merged = append(merged, trimmed)
			}
		}
	}

	// Top-level voiceover.Paths second (legacy creator source).
	for _, p := range r.Voiceover.Paths {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			if _, dup := seen[trimmed]; !dup {
				seen[trimmed] = struct{}{}
				merged = append(merged, trimmed)
			}
		}
	}

	return merged
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
