// Package enqueue — normalize.go
//
// Stateless payload-normalization helpers split out from enqueue.go.
// These functions transform, fingerprint, and validate the inbound
// JSON payload before the Enqueuer.Lifecycle phase picks it up. They
// share no state with the orchestrator (no DB, no *Enqueuer receivers,
// no asset-service wiring) and may be called freely from any package
// site that holds a `map[string]interface{}` payload.
//
// Cross-file visibility: same `package enqueue`, so private symbols
// (validationError alias, PlanDestination, *ResolvedPlan) referenced
// from sibling files remain in scope without re-export.
//
// Migration note: after the shared/contract/deliveryplan extraction,
// every `&validationError{field: ..., message: ...}` literal in this
// file now routes through deliveryplan.NewValidationError (or
// NewValidationErrorWrapped when an unwrapped cause is needed). The
// canonical `<FieldPath>: <Msg>` envelope is preserved verbatim, so
// the existing test gold path (substring matches on the rejection
// messages) stays green without test-side changes.
package enqueue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/routing"
	"velox-shared/contract"
	"velox-shared/contract/deliveryplan"
	"velox-shared/payload"
)

// validatePlanPayload enforces the precondition invariants on an
// already-resolved plan (no DB hit). Used by enforceDeliveryPlanPrecondition;
// *Tx callers (AtomicForwardAndEnqueue) get the analogous gate via
// store.validateDeliveryDestinationTx inside CreateJobWithTaskTx.
//
// Invariants: plan must be non-nil and carry >=1 destination; every
// destination's retry_budget >= 0. On success, writes the MAX
// retry_budget into job.MaxRetries (maxRetry=0 when every entry opts out
// of retries — preserved as 0 so the worker terminal-fails on the first
// hard error, matching the explicit-zero contract from
// openapi.yaml:SubmitDeliveryPlanEntry.retry_budget.minimum).
func validatePlanPayload(plan *ResolvedPlan, job *jobs.Job) error {
	if plan == nil || len(plan.Destinations) == 0 {
		return deliveryplan.NewValidationError(
			"delivery_plan",
			"no explicit delivery plan; create job_delivery_plans rows for this job before enqueueing",
		)
	}
	maxRetry := 0
	for i, d := range plan.Destinations {
		if d.RetryBudget < 0 {
			return deliveryplan.NewValidationError(
				fmt.Sprintf("delivery_plan.%d.retry_budget", i),
				"must be >= 0",
			)
		}
		if d.RetryBudget > maxRetry {
			maxRetry = d.RetryBudget
		}
	}
	if job != nil {
		job.MaxRetries = maxRetry
	}
	return nil
}

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	// Build the canonical typed envelope, then project to the downstream
	// map. No `parameters` sub-map, no legacy alias keys. Single source
	// of truth is the contract.JobPayloadV2 struct.
	base := contract.NewJobPayloadV2(payloadMap)

	title := strings.TrimSpace(base.VideoName)
	if title == "" {
		return nil, deliveryplan.NewValidationError("video_name", "is required")
	}
	base.VideoName = title
	if rawMetadata, present := payloadMap["video_metadata"]; present && rawMetadata != nil {
		metadata, ok := rawMetadata.(map[string]interface{})
		if !ok {
			return nil, deliveryplan.NewValidationError("video_metadata", "must be an object")
		}
		if err := validateVideoMetadata(metadata); err != nil {
			return nil, err
		}
		base.VideoMetadata = cloneMetadataMap(metadata)
	}

	scriptText := strings.TrimSpace(base.ScriptText)
	if scriptText == "" {
		scriptText = title
	}
	if scriptText == "" {
		return nil, deliveryplan.NewValidationError("script_text", "is required")
	}
	base.ScriptText = scriptText

	scenesValue, scenesJSON, err := normalizeScenes(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(scenesValue) == 0 {
		return nil, deliveryplan.NewValidationError("scenes", "at least one scene is required")
	}
	base.Scenes = scenesValue
	base.ScenesJSON = scenesJSON
	base.SceneCount = len(scenesValue)

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 && !hasClipTimelinePayload(payloadMap) && !hasRenderableMedia(payloadMap) {
		return nil, deliveryplan.NewValidationError("voiceover_paths", "at least one voiceover path is required")
	}
	base.VoiceoverPaths = voiceovers
	base.VoiceoverCount = len(voiceovers)

	// Identity enrichment — prefer explicit caller-provided IDs/new
	// UUIDs over the constructor's defaults so the typed struct always
	// ends with concrete, non-empty lifecycle fields.
	jobID := strings.TrimSpace(payload.FirstString(payloadMap, "job_id", "id"))
	jobRunID := strings.TrimSpace(payload.FirstString(payloadMap, "job_run_id", "run_id"))
	correlationID := strings.TrimSpace(payload.FirstString(payloadMap, "correlation_id"))
	base.SetIdentity(jobID, jobRunID, correlationID)

	if base.SubmittedVia == "" {
		base.SubmittedVia = "api_v1_scene_video"
	}
	if base.Source == "" {
		base.Source = "scene_video_api"
	}
	base.Status = "PENDING"
	base.Version = "v2"

	// Apply the fingerprint AFTER all identity + business fields are
	// finalized, so the hash reflects the canonical V2 shape.
	base.JobFingerprint = sceneVideoFingerprint(
		base.JobID,
		base.VideoName,
		base.ScriptText,
		base.ScenesJSON,
		base.VoiceoverPaths,
		base.OutputPath,
		base.AudioLanguage,
	)

	if v := strings.TrimSpace(payload.FirstString(payloadMap, "output_video_id")); v != "" {
		base.OutputVideoID = v
	}

	// Spread to a canonical map for downstream consumers. NO
	// `parameters` sub-map mirror; legacy alias keys NOT emitted.
	out, err := base.ToMap()
	if err != nil {
		return nil, err
	}
	if err := attachVideoMetadataToDeliveryPlan(out); err != nil {
		return nil, err
	}
	copyTimelinePayloadFields(out, payloadMap)
	attachLegacySceneClipTimeline(out)
	return out, nil
}

// attachVideoMetadataToDeliveryPlan copies the canonical video metadata into
// each destination's persisted metadata envelope. The top-level
// video_metadata remains the worker-facing contract; this nested copy is the
// delivery-facing snapshot stored in job_delivery_plans.metadata_json so a
// later DeliveryRunner process can reconstruct the upload without reading a
// volatile request object.
func attachVideoMetadataToDeliveryPlan(payloadMap map[string]interface{}) error {
	metadata, ok := payloadMap["video_metadata"].(map[string]interface{})
	if !ok || len(metadata) == 0 {
		return nil
	}
	rawPlan, ok := payloadMap["delivery_plan"]
	if !ok || rawPlan == nil {
		return nil
	}
	attach := func(entry map[string]interface{}) {
		entryMetadata, ok := entry["metadata"].(map[string]interface{})
		if !ok {
			entryMetadata = map[string]interface{}{}
			entry["metadata"] = entryMetadata
		}
		if _, exists := entryMetadata["video_metadata"]; !exists {
			entryMetadata["video_metadata"] = cloneMetadataMap(metadata)
		}
	}
	switch plan := rawPlan.(type) {
	case []interface{}:
		for i, item := range plan {
			entry, ok := item.(map[string]interface{})
			if !ok {
				return deliveryplan.NewValidationError(
					fmt.Sprintf("delivery_plan.%d", i),
					"must be an object",
				)
			}
			attach(entry)
		}
	case []map[string]interface{}:
		for _, entry := range plan {
			attach(entry)
		}
	case map[string]interface{}:
		attach(plan)
	}
	return nil
}

func validateVideoMetadata(metadata map[string]interface{}) error {
	if title, ok := metadata["title"]; ok {
		if value, ok := title.(string); !ok || strings.TrimSpace(value) == "" {
			return deliveryplan.NewValidationError("video_metadata.title", "must be a non-empty string")
		}
	}
	if description, ok := metadata["description"]; ok {
		if _, ok := description.(string); !ok {
			return deliveryplan.NewValidationError("video_metadata.description", "must be a string")
		}
	}
	if tags, ok := metadata["tags"]; ok && tags != nil {
		switch values := tags.(type) {
		case []interface{}:
			for i, value := range values {
				if _, ok := value.(string); !ok {
					return deliveryplan.NewValidationError(
						fmt.Sprintf("video_metadata.tags[%d]", i),
						"must be a string",
					)
				}
			}
		case []string:
		default:
			return deliveryplan.NewValidationError("video_metadata.tags", "must be an array of strings")
		}
	}
	if privacy, ok := metadata["privacy_status"]; ok {
		value, ok := privacy.(string)
		if !ok || !isValidPrivacyStatus(value) {
			return deliveryplan.NewValidationError("video_metadata.privacy_status", "must be private, unlisted, or public")
		}
	}
	if publishAt, ok := metadata["publish_at"]; ok && publishAt != nil {
		value, ok := publishAt.(string)
		if !ok || strings.TrimSpace(value) == "" {
			return deliveryplan.NewValidationError("video_metadata.publish_at", "must be an RFC3339 timestamp")
		}
		if _, err := time.Parse(time.RFC3339, value); err != nil {
			return deliveryplan.NewValidationError("video_metadata.publish_at", "must be an RFC3339 timestamp")
		}
	}
	return nil
}

func isValidPrivacyStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "private", "unlisted", "public":
		return true
	default:
		return false
	}
}

func cloneMetadataMap(metadata map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

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

func hasClipTimelinePayload(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	for _, key := range []string{"clips", "items", "clip_segments", "intro_clip_paths", "stock_clip_paths"} {
		switch v := payloadMap[key].(type) {
		case []string:
			if len(v) > 0 {
				return true
			}
		case []interface{}:
			if len(v) > 0 {
				return true
			}
		}
	}
	return false
}

func copyTimelinePayloadFields(out, src map[string]interface{}) {
	if out == nil || src == nil {
		return
	}
	for _, key := range []string{
		"images",
		"clips",
		"items",
		"audio_tracks",
		"subtitle_tracks",
		"layers",
		"clip_segments",
		"intro_clip_paths",
		"stock_clip_paths",
		"fit",
		"effect",
		"orientation",
		// Preserve legacy delivery keys through normalization so
		// taskSpec.Payload still satisfies AtomicJobTaskCreator's parse-time
		// delivery-plan requirement.
		"delivery_destination_ids",
		"delivery_destination_id",
		"destination_ids",
		"destination_id",
	} {
		if value, ok := src[key]; ok && value != nil {
			out[key] = value
		}
	}
	meta := routing.FromPayload(src)
	if meta.PipelineID != "" {
		out["pipeline_id"] = meta.PipelineID.String()
	}
	if audioURL := strings.TrimSpace(payload.FirstString(src, "audio_url")); audioURL != "" {
		out["audio_url"] = audioURL
	}
	// Preserve the forwarding metadata so normalizeSceneVideoPayload
	// carries it into the normalized payload consumed by
	// Enqueue → DeriveForwardingJobID.
	if meta.ForwardingKey != "" {
		out[routing.KeyForwardingKey] = meta.ForwardingKey.String()
	}
}

func attachLegacySceneClipTimeline(out map[string]interface{}) {
	if out == nil || hasClipTimelinePayload(out) {
		return
	}
	scenes := normalizeSceneArray(out["scenes"])
	if len(scenes) == 0 {
		return
	}

	voiceovers := normalizeVoiceoverList(out)
	items := make([]map[string]interface{}, 0, len(scenes))
	clips := make([]string, 0, len(scenes))
	audioTracks := make([]map[string]interface{}, 0, len(scenes))
	subtitleTracks := normalizeSubtitleTracks(out["subtitle_tracks"])
	offsetSeconds := 0.0

	for i, scene := range scenes {
		clipURL := firstClipURL(scene)
		if clipURL == "" {
			return
		}
		voiceoverDuration := sceneVoiceoverDurationSeconds(scene)
		clipDuration := sceneClipDurationSeconds(scene)
		duration := payload.NormalizedDuration(scene["duration_seconds"])
		if voiceoverDuration > 0 {
			duration = voiceoverDuration
			if clipDuration > duration {
				duration = clipDuration
			}
		}
		if duration <= 0 {
			if clipDuration > 0 {
				duration = clipDuration
			} else {
				duration = 4.0
			}
		}

		normalized := make(map[string]interface{}, len(scene)+3)
		for k, v := range scene {
			normalized[k] = v
		}
		normalized["clip_link"] = clipURL
		normalized["clip_links"] = []string{clipURL}
		normalized["duration_seconds"] = duration
		scenes[i] = normalized

		items = append(items, map[string]interface{}{
			"type":          "video",
			"url":           clipURL,
			"duration":      duration,
			"fit":           "contain",
			"include_audio": true,
		})
		clips = append(clips, clipURL)

		if i < len(voiceovers) && strings.TrimSpace(voiceovers[i]) != "" {
			trackDuration := duration
			if voiceoverDuration > 0 {
				trackDuration = voiceoverDuration
			}
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url":        strings.TrimSpace(voiceovers[i]),
				"volume":            1.0,
				"start_time_offset": offsetSeconds,
				"duration_seconds":  trackDuration,
				"role":              "voiceover",
			})
		}
		if len(subtitleTracks) == 0 {
			if track := sceneSubtitleTrack(scene); len(track) > 0 {
				subtitleTracks = append(subtitleTracks, track)
			}
		}
		offsetSeconds += duration
	}

	out["scenes"] = scenes
	if data, err := json.Marshal(scenes); err == nil {
		out["scenes_json"] = string(data)
	}
	out["items"] = items
	out["clips"] = payload.DedupeStrings(clips)
	if len(audioTracks) > 0 {
		out["audio_tracks"] = audioTracks
	}
	if len(subtitleTracks) > 0 {
		out["subtitle_tracks"] = subtitleTracks
	}
	out["video_mode"] = "clip_stock"
}

func normalizeSubtitleTracks(raw interface{}) []map[string]interface{} {
	var tracks []map[string]interface{}
	switch values := raw.(type) {
	case []map[string]interface{}:
		for _, value := range values {
			if len(value) > 0 {
				tracks = append(tracks, value)
			}
		}
	case []interface{}:
		for _, value := range values {
			if track, ok := value.(map[string]interface{}); ok && len(track) > 0 {
				tracks = append(tracks, track)
			}
		}
	}
	return tracks
}

func sceneSubtitleTrack(scene map[string]interface{}) map[string]interface{} {
	if scene == nil {
		return nil
	}
	nested, ok := scene["subtitles"].(map[string]interface{})
	if !ok {
		return nil
	}
	source := strings.TrimSpace(payload.FirstString(nested, "source", "url"))
	if source == "" {
		return nil
	}
	track := map[string]interface{}{"source": source}
	if preset := strings.TrimSpace(payload.FirstString(nested, "preset")); preset != "" {
		track["preset"] = preset
	}
	if font := strings.TrimSpace(payload.FirstString(nested, "font")); font != "" {
		track["font"] = font
	}
	if format := strings.TrimSpace(payload.FirstString(nested, "format")); format != "" {
		track["format"] = format
	}
	if language := strings.TrimSpace(payload.FirstString(nested, "language")); language != "" {
		track["language"] = language
	}
	return track
}

func sceneVoiceoverDurationSeconds(scene map[string]interface{}) float64 {
	if scene == nil {
		return 0
	}
	if duration := payload.NormalizedDuration(scene["voiceover_duration_seconds"]); duration > 0 {
		return duration
	}
	if nested, ok := scene["voiceover"].(map[string]interface{}); ok {
		if duration := payload.NormalizedDuration(nested["duration_seconds"]); duration > 0 {
			return duration
		}
		if ms := payload.NormalizedDuration(nested["duration_ms"]); ms > 0 {
			return ms / 1000.0
		}
	}
	return 0
}

func sceneClipDurationSeconds(scene map[string]interface{}) float64 {
	if scene == nil {
		return 0
	}
	if duration := payload.NormalizedDuration(scene["final_clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if duration := payload.NormalizedDuration(scene["clip_duration_seconds"]); duration > 0 {
		return duration
	}
	if nested, ok := scene["clip"].(map[string]interface{}); ok {
		if duration := payload.NormalizedDuration(nested["duration_seconds"]); duration > 0 {
			return duration
		}
		if ms := payload.NormalizedDuration(nested["duration_ms"]); ms > 0 {
			return ms / 1000.0
		}
	}
	return 0
}

func syncAudioURLFromVoiceover(payloadMap map[string]interface{}) {
	if payloadMap == nil {
		return
	}
	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 {
		return
	}
	if strings.TrimSpace(payload.FirstString(payloadMap, "audio_url")) == "" || hasClipTimelinePayload(payloadMap) || strings.TrimSpace(payload.FirstString(payloadMap, "pipeline_id", routing.KeyPipelineID)) != "" {
		payloadMap["audio_url"] = voiceovers[0]
	}
}

func resolveInternalExecutorID(payloadMap map[string]interface{}) string {
	if payloadMap == nil {
		return ""
	}
	meta := routing.FromPayload(payloadMap)
	if meta.Executor.ID == "" {
		return ""
	}
	if meta.Executor.Version > 0 && !strings.Contains(meta.Executor.ID, "@") {
		return fmt.Sprintf("%s@%d", meta.Executor.ID, meta.Executor.Version)
	}
	return meta.Executor.ID
}

// resolveRequiredCapabilities returns the capability strings a task requires
// based on its executor. These are stored in task_requirements and consumed
// by the placement matcher's capability gate (matcher.go Select).
//
// For now the mapping is executor-driven:
//   - scene.composite.* → artifact.commit.v1
//   - All other executors → nil (no extra capabilities yet)
func resolveRequiredCapabilities(executorID string) []string {
	if strings.HasPrefix(executorID, "scene.composite") {
		return []string{"artifact.commit.v1"}
	}
	return nil
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

// extractPlanMaxRetry computes the maximum retry_budget across the
// payload's delivery_plan entries. The single writer of job.MaxRetries
// on the insert path.
func extractPlanMaxRetry(payload map[string]interface{}) int {
	if payload == nil {
		return 0
	}
	planRaw, ok := payload["delivery_plan"]
	if !ok {
		return 0
	}
	arr, ok := planRaw.([]interface{})
	if !ok {
		return 0
	}
	maxRetry := 0
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		switch v := m["retry_budget"].(type) {
		case int:
			if v > maxRetry {
				maxRetry = v
			}
		case int64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		case float64:
			if int(v) > maxRetry {
				maxRetry = int(v)
			}
		}
	}
	return maxRetry
}
