// Package enqueue - core payload normalization and canonical projection.
package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	compiledV2Present := compiledRenderPlanV2Present(payloadMap)
	if compiledV2Present {
		// PipelineGen owns V2 compilation. At this boundary the master only
		// admits the exact producer bytes: verify the supplied SHA first,
		// then strict-decode and validate the plan. No manifest compiler or
		// timeline reconstruction is allowed on this path.
		if err := contract.ValidateCompiledRenderPlanV2Payload(payloadMap); err != nil {
			return nil, deliveryplan.NewValidationErrorWrapped("compiled_render_plan_v2", "pass-through validation failed", err)
		}
	}

	rawManifest, manifestPresent := payloadMap["render_manifest"]
	strictManifest := manifestPresent
	var strictManifestMap map[string]interface{}
	if strictManifest {
		if rawManifest == nil {
			return nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		manifest, ok := rawManifest.(map[string]interface{})
		if !ok {
			return nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		strictManifestMap = manifest
		if len(manifest) == 0 {
			return nil, deliveryplan.NewValidationError("render_manifest", "must not be empty")
		}
	}
	// visual_replacements[] is only resolvable when the master compiles a
	// strict render_manifest with a verified final_audio asset into a
	// CompiledRenderPlanV2. Any other intake (scene-based jobs, pre-compiled
	// V2 pass-through) would silently drop the replacements, so we fail
	// closed instead of accepting an ambiguous request.
	if visualReplacementsPresent(payloadMap) && !(strictManifest && !compiledV2Present && renderManifestHasFinalAudio(strictManifestMap)) {
		return nil, deliveryplan.NewValidationError("visual_replacements", "requires a render_manifest with a verified final_audio asset (scene-based and pre-compiled V2 jobs are not supported)")
	}
	base, err := contract.NewJobPayloadV2Checked(payloadMap)
	if err != nil {
		return nil, err
	}
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

	if !strictManifest && !compiledV2Present {
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
		if len(voiceovers) == 0 && !hasClipTimelinePayload(payloadMap) && !hasRenderableMedia(payloadMap) {
			return nil, deliveryplan.NewValidationError("voiceover_paths", "at least one voiceover path is required (or renderable media)")
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
	base.Status = contract.InputAssemblyPending
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
	if !strictManifest && !compiledV2Present {
		copyTimelinePayloadFields(out, payloadMap)
	}

	// Strict V2 manifests are compiled master-side before the task is
	// created. Legacy payloads remain on their existing path because they
	// do not necessarily carry verified asset size/hash metadata.
	if strictManifest && !compiledV2Present {
		plan, compileErr := rendercompiler.DefaultRegistry().Compile(ctx, base)
		if compileErr != nil {
			return nil, deliveryplan.NewValidationErrorWrapped("render_manifest", "compile failed", compileErr)
		}
		out["render_plan_json"] = string(plan.JSON())
		out["render_plan_sha256"] = plan.SHA256()

		// A manifest carrying the verified final_audio asset opts into the
		// strict V2 receiver. Legacy strict manifests without final_audio
		// remain on the existing scene-composite/V1 path until their audio
		// compiler output is registered.
		if renderManifestHasFinalAudio(strictManifestMap) {
			replacements, parseErr := contract.ParseVisualReplacements(payloadMap["visual_replacements"])
			if parseErr != nil {
				return nil, deliveryplan.NewValidationErrorWrapped("visual_replacements", "parse failed", parseErr)
			}
			compiledJSON, compiledSHA, v2Err := contract.CompileRenderPlanV2JSONWithReplacements(strictManifestMap, replacements)
			if v2Err != nil {
				// Overlap / invalid-range / out-of-bounds / asset-identity
				// rejections carry a machine-readable VISUAL_REPLACEMENT_*
				// code. Surface it as the 422 details[].issue (instead of a
				// generic "render_manifest compile failed") so invalid jobs
				// are refused with a stable code BEFORE any worker offer is
				// produced. Non-replacement compile failures keep the generic
				// render_manifest classification.
				var vre *contract.VisualReplacementError
				if errors.As(v2Err, &vre) && vre != nil {
					return nil, deliveryplan.NewValidationErrorCode("visual_replacements", vre.Code, vre.Error())
				}
				return nil, deliveryplan.NewValidationErrorWrapped("render_manifest", "CompiledRenderPlanV2 compile failed", v2Err)
			}
			out[contract.PayloadKeyCompiledRenderPlanJSON] = string(compiledJSON)
			out[contract.PayloadKeyCompiledRenderPlanSHA] = compiledSHA
		}
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
		"layers",
		// Explicit opt-in for the worker's strict packet-copy path. The
		// worker validates stream identity, keyframe boundaries and audio
		// compatibility before using it; it must therefore survive the
		// master normalization boundary unchanged.
		"copy_only",
		// Preserve control-plane inputs through normalization so the
		// compiler can move them into TaskSpec.DeliveryPlan and
		// TaskSpec.PublicationSpecs. They are removed again by
		// RenderOnlyPayload before the renderer-facing TaskSpec payload is
		// persisted or offered to a worker.
		"project_id",
		"render_spec",
		"render_only",
		"publications",
		"publication_specs",
		// Preserve legacy delivery keys through normalization so
		// taskSpec.DeliveryPlan can satisfy the atomic creator's parse-time
		// delivery-plan requirement without putting them in Payload.
		"delivery_plan",
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
func compiledRenderPlanV2Present(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	_, hasJSON := payloadMap[contract.PayloadKeyCompiledRenderPlanJSON]
	_, hasSHA := payloadMap[contract.PayloadKeyCompiledRenderPlanSHA]
	return hasJSON || hasSHA
}

func visualReplacementsPresent(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	raw, ok := payloadMap["visual_replacements"]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case []interface{}:
		return len(v) > 0
	case []map[string]interface{}:
		return len(v) > 0
	default:
		return true
	}
}

func resolveInternalExecutorID(payloadMap map[string]interface{}) string {
	if payloadMap == nil {
		return ""
	}
	meta := routing.FromPayload(payloadMap)
	if meta.Executor.ID == "" {
		if _, hasJSON := payloadMap[contract.PayloadKeyCompiledRenderPlanJSON]; hasJSON {
			if _, hasSHA := payloadMap[contract.PayloadKeyCompiledRenderPlanSHA]; hasSHA {
				return "render_batch@1"
			}
		}
		return ""
	}
	if meta.Executor.Version > 0 && !strings.Contains(meta.Executor.ID, "@") {
		return fmt.Sprintf("%s@%d", meta.Executor.ID, meta.Executor.Version)
	}
	return meta.Executor.ID
}

func renderManifestHasFinalAudio(raw map[string]interface{}) bool {
	assets, ok := raw["assets"].([]interface{})
	if !ok {
		if typed, typedOK := raw["assets"].([]map[string]interface{}); typedOK {
			for _, asset := range typed {
				if kind, _ := asset["kind"].(string); kind == "final_audio" {
					return true
				}
			}
		}
		return false
	}
	for _, value := range assets {
		asset, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		if kind, _ := asset["kind"].(string); kind == "final_audio" {
			return true
		}
	}
	return false
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
