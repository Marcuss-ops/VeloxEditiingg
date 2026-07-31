package remoteengine

import (
	"encoding/json"
	"fmt"
	"strings"

	"velox-shared/payload"
)

// ── Initial response validation ──────────────────────────────────────────────

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
