// Package enqueue — V2-only payload normalization and routing.
package enqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"velox-server/internal/telemetry"
	"velox-shared/contract"
	"velox-shared/contract/deliveryplan"
)

const canonicalCompiledPlanExecutor = "video.assemble.copy.v1@1"
const canonicalDriveSliceExecutor = "video.assemble.slice.v1@1"

func normalizeSceneVideoPayload(payloadMap map[string]interface{}) (map[string]interface{}, error) {
	return normalizeSceneVideoPayloadContext(context.Background(), payloadMap)
}

// normalizeSceneVideoPayloadContext validates and wraps producer-owned V2
// bytes. It deliberately has no scene, overlay, media, or manifest compiler.
func normalizeSceneVideoPayloadContext(ctx context.Context, payloadMap map[string]interface{}) (map[string]interface{}, error) {
	if payloadMap == nil {
		return nil, errors.New("compiled_render_plan_v2: payload is required")
	}
	for _, key := range []string{"scenes", "scenes_json", "render_manifest", "manifest_ref", "layers", "overlays", "visual_replacements", "items", "clips", "images", "voiceover_paths", "voiceover_path", "audio_path"} {
		if _, exists := payloadMap[key]; exists {
			return nil, deliveryplan.NewValidationError(key, "is retired; submit the producer-compiled CompiledRenderPlanV2")
		}
	}
	base, err := contract.NewJobPayloadV2Checked(payloadMap)
	if err != nil {
		return nil, err
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(payloadMap); err != nil {
		return nil, deliveryplan.NewValidationErrorWrapped("compiled_render_plan_v2", "producer plan validation failed", err)
	}
	if !compiledRenderPlanV2Present(payloadMap) {
		return nil, deliveryplan.NewValidationError("compiled_render_plan_v2", "producer-compiled plan and SHA are required")
	}
	base.VideoName = strings.TrimSpace(base.VideoName)
	if base.VideoName == "" {
		return nil, deliveryplan.NewValidationError("video_name", "is required")
	}
	base.ScriptText = strings.TrimSpace(base.ScriptText)
	if base.ScriptText == "" {
		base.ScriptText = base.VideoName
	}
	base.SetIdentity(base.JobID, base.JobRunID, base.CorrelationID)
	base.Version = "v2"
	base.Status = contract.InputAssemblyPending
	base.JobFingerprint = sceneVideoFingerprintContext(ctx, base.JobID, base.VideoName, base.ScriptText, base.CompiledRenderPlanSHA256)

	out, err := base.ToMap()
	if err != nil {
		return nil, err
	}
	// These fields are control-plane or runtime binding metadata. Preserve
	// them without interpreting or modifying the compiled timeline.
	for key, value := range payloadMap {
		if strings.HasPrefix(key, "runtime_") || key == "publications" || key == "assembly" ||
			key == "_internal_forwarding_key" || key == "_placement_pin_worker_id" ||
			key == "source_provider" || key == "source_job_id" || key == "target_executor_id" || key == "delivery_plan" || key == "render_only" {
			if key == "_placement_pin_worker_id" {
				if pin, ok := value.(string); ok {
					value = strings.TrimSpace(pin)
				}
			}
			out[key] = value
		}
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(out); err != nil {
		return nil, deliveryplan.NewValidationErrorWrapped("compiled_render_plan_v2", "normalized plan validation failed", err)
	}
	return out, nil
}

func compiledRenderPlanV2Present(payloadMap map[string]interface{}) bool {
	if payloadMap == nil {
		return false
	}
	_, hasJSON := payloadMap[contract.PayloadKeyCompiledRenderPlanJSON]
	_, hasSHA := payloadMap[contract.PayloadKeyCompiledRenderPlanSHA]
	return hasJSON || hasSHA
}

func resolveInternalExecutorID(payloadMap map[string]interface{}) string {
	if !compiledRenderPlanV2Present(payloadMap) {
		return ""
	}
	// Final-job Producers may keep the immutable compiled plan free of
	// transport locators and attach Drive file IDs in runtime_assets instead.
	// Join those locators by the stable asset_id before selecting the executor;
	// otherwise stock-sha256-* assets are misrouted to the copy-only executor.
	runtimeDriveIDs := runtimeDriveFileIDs(payloadMap["runtime_assets"])
	if raw, ok := payloadMap[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
		var plan struct {
			Assets []struct {
				SourceKind  string `json:"source_kind"`
				DriveFileID string `json:"drive_file_id"`
				AssetID     string `json:"asset_id"`
				AssetKey    string `json:"asset_key"`
				Kind        string `json:"kind"`
			} `json:"assets"`
		}
		if json.Unmarshal([]byte(raw), &plan) == nil {
			for _, asset := range plan.Assets {
				driveFileID := strings.TrimSpace(asset.DriveFileID)
				if driveFileID == "" {
					driveFileID = runtimeDriveIDs[strings.TrimSpace(asset.AssetID)]
				}
				if driveFileID == "" {
					driveFileID = runtimeDriveIDs[strings.TrimSpace(asset.AssetKey)]
				}
				if asset.Kind == "video" && (asset.SourceKind == "drive" || driveFileID != "" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:")) {
					return canonicalDriveSliceExecutor
				}
			}
		}
	}
	return canonicalCompiledPlanExecutor
}

func runtimeDriveFileIDs(raw interface{}) map[string]string {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return nil
	}
	var assets []struct {
		AssetID     string `json:"asset_id"`
		AssetKey    string `json:"asset_key"`
		DriveFileID string `json:"drive_file_id"`
	}
	if err := json.Unmarshal(encoded, &assets); err != nil {
		return nil
	}
	byIdentity := make(map[string]string, len(assets)*2)
	for _, asset := range assets {
		fileID := strings.TrimSpace(asset.DriveFileID)
		if fileID == "" {
			continue
		}
		for _, identity := range []string{asset.AssetID, asset.AssetKey} {
			if identity = strings.TrimSpace(identity); identity != "" {
				byIdentity[identity] = fileID
			}
		}
	}
	return byIdentity
}

func resolveRequiredCapabilities(executorID string) []string {
	if strings.HasPrefix(executorID, "video.assemble.copy.v1") || strings.HasPrefix(executorID, "video.assemble.slice.v1") {
		return []string{"artifact.commit.v1"}
	}
	return nil
}

func sceneVideoFingerprintContext(ctx context.Context, parts ...interface{}) string {
	h := sha256.New()
	for _, part := range parts {
		switch value := part.(type) {
		case string:
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				h.Write([]byte(trimmed))
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
