// Package enqueue - core payload normalization and canonical projection.
package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"velox-server/internal/routing"
	"velox-server/internal/telemetry"
	"velox-shared/contract"
	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendercompiler"
	"velox-shared/payload"
)

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	return normalizeSceneVideoPayloadContext(context.Background(), payloadMap)
}

// normalizeSceneVideoPayloadContext is the request-aware normalizer used by
// the enqueue path. The compatibility wrapper above keeps existing package
// callers and tests on the historical signature.
func normalizeSceneVideoPayloadContext(ctx context.Context, payloadMap map[string]interface{}) (map[string]interface{}, error) {
	// Build the canonical typed envelope, then project to the downstream
	// map. No `parameters` sub-map, no legacy alias keys. Single source
	// of truth is the contract.JobPayloadV2 struct.
	rawManifest, manifestPresent := payloadMap["render_manifest"]
	strictManifest := manifestPresent
	if strictManifest {
		if rawManifest == nil {
			return nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		manifest, ok := rawManifest.(map[string]interface{})
		if !ok {
			return nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		if len(manifest) == 0 {
			return nil, deliveryplan.NewValidationError("render_manifest", "must not be empty")
		}
	}
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
		// Keep only renderer-owned technical settings. Publication fields
		// are validated at intake but never enter the typed render payload,
		// render-plan compiler, persisted TaskSpec, or worker wire payload.
		base.VideoMetadata = rendererVideoMetadata(metadata)
	}

	scriptText := strings.TrimSpace(base.ScriptText)
	if scriptText == "" {
		scriptText = title
	}
	if scriptText == "" {
		return nil, deliveryplan.NewValidationError("script_text", "is required")
	}
	base.ScriptText = scriptText

	if !strictManifest {
		scenesValue, scenesJSON, err := normalizeScenesContext(ctx, payloadMap)
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
		if len(voiceovers) == 0 && !hasClipTimelinePayload(payloadMap) && !hasRenderableMedia(payloadMap) && !hasAudioTracks(payloadMap) {
			return nil, deliveryplan.NewValidationError("voiceover_paths", "at least one voiceover path is required (or audio_tracks, or renderable media)")
		}
		base.VoiceoverPaths = voiceovers
		base.VoiceoverCount = len(voiceovers)
	}

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
	base.JobFingerprint = sceneVideoFingerprintContext(ctx,
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
	// A strict render_manifest is the sole timeline source. Legacy timeline
	// projection is intentionally skipped so raw scenes/layers cannot shadow
	// the compiled immutable plan. Legacy payloads retain the old pass-through
	// behavior.
	if !strictManifest {
		copyTimelinePayloadFields(out, payloadMap)
	}

	// Strict V2 manifests are compiled master-side before the task is
	// created. Legacy payloads remain on their existing path because they
	// do not necessarily carry verified asset size/hash metadata.
	if strictManifest {
		plan, compileErr := rendercompiler.DefaultRegistry().Compile(ctx, base)
		if compileErr != nil {
			return nil, deliveryplan.NewValidationErrorWrapped("render_manifest", "compile failed", compileErr)
		}
		out["render_plan_json"] = string(plan.JSON())
		out["render_plan_sha256"] = plan.SHA256()
	}
	return out, nil
}
func CopyTimelinePayloadFields(out, src map[string]interface{}) {
	copyTimelinePayloadFields(out, src)
}
func copyTimelinePayloadFields(out, src map[string]interface{}) {
	if out == nil || src == nil {
		return
	}
	for _, key := range []string{
		// Canonical timeline fields only. Legacy images/clips/items
		// and clip-pool aliases are projected at the worker offer
		// boundary, never persisted in the master payload.
		"audio_tracks",
		"subtitle_tracks",
		"layers",
		// Preserve legacy delivery keys through normalization so
		// taskSpec.Payload still satisfies AtomicJobTaskCreator's parse-time
		// delivery-plan requirement.
		"delivery_destination_ids",
		"delivery_destination_id",
		"destination_ids",
		"destination_id",
		// Preserve per-job placement pin through normalization so
		// the placement matcher sees _placement_pin_worker_id in
		// the task spec payload and routes to the pinned worker.
		"_placement_pin_worker_id",
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
func resolveRequiredCapabilities(executorID string) []string {
	if strings.HasPrefix(executorID, "scene.composite") {
		return []string{"artifact.commit.v1"}
	}
	return nil
}

func sceneVideoFingerprintContext(ctx context.Context, parts ...interface{}) string {
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
			telemetry.RecordEnqueueJSONMarshal(ctx)
			if data, err := json.Marshal(part); err == nil {
				h.Write(data)
			}
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
