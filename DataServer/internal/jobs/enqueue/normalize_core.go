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
	compiledV2Present, strictManifest, strictManifestMap, err := validateSceneVideoInputs(payloadMap)
	if err != nil {
		return nil, err
	}

	base, err := contract.NewJobPayloadV2Checked(payloadMap)
	if err != nil {
		return nil, err
	}
	if err := normalizeSceneVideoFields(ctx, payloadMap, base, strictManifest, compiledV2Present); err != nil {
		return nil, err
	}

	return projectNormalizedSceneVideoPayload(
		ctx,
		payloadMap,
		base,
		strictManifest,
		compiledV2Present,
		strictManifestMap,
	)
}

// validateSceneVideoInputs owns the mutually exclusive compiled-plan and
// render-manifest preflight rules. Keeping these gates together prevents a
// later normalization branch from accidentally accepting an unsupported
// visual-replacement combination.
func validateSceneVideoInputs(payloadMap map[string]interface{}) (bool, bool, map[string]interface{}, error) {
	compiledV2Present := compiledRenderPlanV2Present(payloadMap)
	if compiledV2Present {
		// PipelineGen owns V2 compilation. At this boundary the master only
		// admits the exact producer bytes: verify the supplied SHA first,
		// then strict-decode and validate the plan. No manifest compiler or
		// timeline reconstruction is allowed on this path.
		if err := contract.ValidateCompiledRenderPlanV2Payload(payloadMap); err != nil {
			return false, false, nil, deliveryplan.NewValidationErrorWrapped("compiled_render_plan_v2", "pass-through validation failed", err)
		}
	}

	rawManifest, manifestPresent := payloadMap["render_manifest"]
	strictManifest := manifestPresent
	var strictManifestMap map[string]interface{}
	if strictManifest {
		if rawManifest == nil {
			return false, false, nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		manifest, ok := rawManifest.(map[string]interface{})
		if !ok {
			return false, false, nil, deliveryplan.NewValidationError("render_manifest", "must be an object")
		}
		strictManifestMap = manifest
		if len(manifest) == 0 {
			return false, false, nil, deliveryplan.NewValidationError("render_manifest", "must not be empty")
		}
	}
	// visual_replacements[] is only resolvable when the master compiles a
	// strict render_manifest with a verified final_audio asset into a
	// CompiledRenderPlanV2. Any other intake would silently drop the
	// replacements, so fail closed instead of accepting an ambiguous request.
	if visualReplacementsPresent(payloadMap) && !(strictManifest && !compiledV2Present && renderManifestHasFinalAudio(strictManifestMap)) {
		return false, false, nil, deliveryplan.NewValidationError("visual_replacements", "requires a render_manifest with a verified final_audio asset (scene-based and pre-compiled V2 jobs are not supported)")
	}
	return compiledV2Present, strictManifest, strictManifestMap, nil
}

// normalizeSceneVideoFields owns the ordered typed-field normalization. The
// order intentionally matches the previous single function so validation
// errors remain stable for callers and tests.
func normalizeSceneVideoFields(ctx context.Context, payloadMap map[string]interface{}, base *contract.JobPayloadV2, strictManifest, compiledV2Present bool) error {
	title := strings.TrimSpace(base.VideoName)
	if title == "" {
		return deliveryplan.NewValidationError("video_name", "is required")
	}
	base.VideoName = title
	if rawMetadata, present := payloadMap["video_metadata"]; present && rawMetadata != nil {
		metadata, ok := rawMetadata.(map[string]interface{})
		if !ok {
			return deliveryplan.NewValidationError("video_metadata", "must be an object")
		}
		if err := validateVideoMetadata(metadata); err != nil {
			return err
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
		return deliveryplan.NewValidationError("script_text", "is required")
	}
	base.ScriptText = scriptText

	if strictManifest || compiledV2Present {
		return nil
	}
	return normalizeLegacySceneInputs(ctx, payloadMap, base)
}

func normalizeLegacySceneInputs(ctx context.Context, payloadMap map[string]interface{}, base *contract.JobPayloadV2) error {
	scenesValue, scenesJSON, err := normalizeScenesContext(ctx, payloadMap)
	if err != nil {
		return err
	}
	if len(scenesValue) == 0 {
		return deliveryplan.NewValidationError("scenes", "at least one scene is required")
	}
	base.Scenes = scenesValue
	base.ScenesJSON = scenesJSON
	base.SceneCount = len(scenesValue)

	voiceovers := normalizeVoiceoverList(payloadMap)
	if len(voiceovers) == 0 && !hasClipTimelinePayload(payloadMap) && !hasRenderableMedia(payloadMap) {
		return deliveryplan.NewValidationError("voiceover_paths", "at least one voiceover path is required (or renderable media)")
	}
	base.VoiceoverPaths = voiceovers
	base.VoiceoverCount = len(voiceovers)
	return nil
}

func projectNormalizedSceneVideoPayload(
	ctx context.Context,
	payloadMap map[string]interface{},
	base *contract.JobPayloadV2,
	strictManifest, compiledV2Present bool,
	strictManifestMap map[string]interface{},
) (map[string]interface{}, error) {
	// Identity enrichment — prefer explicit caller-provided IDs/new UUIDs
	// over constructor defaults so the typed struct ends with concrete IDs.
	base.SetIdentity(
		strings.TrimSpace(payload.FirstString(payloadMap, "job_id", "id")),
		strings.TrimSpace(payload.FirstString(payloadMap, "job_run_id", "run_id")),
		strings.TrimSpace(payload.FirstString(payloadMap, "correlation_id")),
	)
	if base.SubmittedVia == "" {
		base.SubmittedVia = "api_v1_scene_video"
	}
	if base.Source == "" {
		base.Source = "scene_video_api"
	}
	base.Status = contract.InputAssemblyPending
	base.Version = "v2"

	// Apply the fingerprint after all identity and business fields are final.
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

	out, err := base.ToMap()
	if err != nil {
		return nil, err
	}
	if err := attachVideoMetadataToDeliveryPlan(out); err != nil {
		return nil, err
	}
	if !strictManifest && !compiledV2Present {
		copyTimelinePayloadFields(out, payloadMap)
	}
	if err := compileStrictScenePlan(ctx, payloadMap, base, out, strictManifest, compiledV2Present, strictManifestMap); err != nil {
		return nil, err
	}
	return out, nil
}

func compileStrictScenePlan(
	ctx context.Context,
	payloadMap map[string]interface{},
	base *contract.JobPayloadV2,
	out map[string]interface{},
	strictManifest, compiledV2Present bool,
	strictManifestMap map[string]interface{},
) error {
	if !strictManifest || compiledV2Present {
		return nil
	}
	plan, compileErr := rendercompiler.DefaultRegistry().Compile(ctx, base)
	if compileErr != nil {
		return deliveryplan.NewValidationErrorWrapped("render_manifest", "compile failed", compileErr)
	}
	out["render_plan_json"] = string(plan.JSON())
	out["render_plan_sha256"] = plan.SHA256()

	if !renderManifestHasFinalAudio(strictManifestMap) {
		return nil
	}
	replacements, parseErr := contract.ParseVisualReplacements(payloadMap["visual_replacements"])
	if parseErr != nil {
		return deliveryplan.NewValidationErrorWrapped("visual_replacements", "parse failed", parseErr)
	}
	compiledJSON, compiledSHA, v2Err := contract.CompileRenderPlanV2JSONWithReplacements(strictManifestMap, replacements)
	if v2Err != nil {
		// Replacement validation errors retain their machine-readable code;
		// other compiler failures keep the render_manifest classification.
		var vre *contract.VisualReplacementError
		if errors.As(v2Err, &vre) && vre != nil {
			return deliveryplan.NewValidationErrorCode("visual_replacements", vre.Code, vre.Error())
		}
		return deliveryplan.NewValidationErrorWrapped("render_manifest", "CompiledRenderPlanV2 compile failed", v2Err)
	}
	out[contract.PayloadKeyCompiledRenderPlanJSON] = string(compiledJSON)
	out[contract.PayloadKeyCompiledRenderPlanSHA] = compiledSHA
	return nil
}
func CopyTimelinePayloadFields(out, src map[string]interface{}) {
	copyTimelinePayloadFields(out, src)
}
func copyTimelinePayloadFields(out, src map[string]interface{}) {
	if out == nil || src == nil {
		return
	}
	for _, key := range []string{
		// Canonical timeline fields plus the explicit clips.v1 renderer
		// input. clips is no longer a legacy alias for clip pipelines: the
		// worker validator requires it to survive into TaskSpec.Payload.
		"layers",
		"clips",
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
	if !hasNonEmptySlice(out["clips"]) {
		if scenesValue, ok := src["scenes"]; ok {
			if data, marshalErr := json.Marshal(scenesValue); marshalErr == nil {
				if clips := clipsFromScenesJSON(string(data)); len(clips) > 0 {
					out["clips"] = clips
				}
			}
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
