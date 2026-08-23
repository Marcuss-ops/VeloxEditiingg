package contract

import (
	"fmt"

	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
)

// ToMap returns the canonical map representation of the payload for
// downstream consumers. It preserves Go value types and omits optional fields
// using the same rules as the original implementation.
func (p *JobPayloadV2) ToMap() (map[string]any, error) {
	if p == nil {
		return map[string]any{}, nil
	}
	out := map[string]any{
		"contract_version":         p.ContractVersion,
		"payload_contract_version": p.PayloadContractVersion,
		"job_id":                   p.JobID,
		"job_run_id":               p.JobRunID,
		"correlation_id":           p.CorrelationID,
		"job_type":                 p.JobType,
		"version":                  p.Version,
		"created_at":               p.CreatedAt,
		"updated_at":               p.UpdatedAt,
		"video_name":               p.VideoName,
		"script_text":              p.ScriptText,
		"priority":                 p.Priority,
		"timeout_secs":             p.TimeoutSecs,
	}
	if p.ScenesJSON != "" {
		out["scenes_json"] = p.ScenesJSON
	}
	if p.TemplateID != "" {
		out["template_id"] = p.TemplateID
	}
	if p.TemplateVersion > 0 {
		out["template_version"] = p.TemplateVersion
	}
	if len(p.RenderManifest) > 0 {
		out["render_manifest"] = cloneObject(p.RenderManifest)
	}
	if len(p.ManifestRef) > 0 {
		out["manifest_ref"] = cloneObject(p.ManifestRef)
	}
	if p.ManifestSHA256 != "" {
		out["manifest_sha256"] = p.ManifestSHA256
	}
	if p.RenderPlanJSON != "" {
		out["render_plan_json"] = p.RenderPlanJSON
	}
	if p.RenderPlanSHA256 != "" {
		out["render_plan_sha256"] = p.RenderPlanSHA256
	}
	if p.CompiledRenderPlanJSON != "" {
		out[PayloadKeyCompiledRenderPlanJSON] = p.CompiledRenderPlanJSON
	}
	if p.CompiledRenderPlanSHA256 != "" {
		out[PayloadKeyCompiledRenderPlanSHA] = p.CompiledRenderPlanSHA256
	}
	if len(p.Scenes) > 0 {
		out["scenes"] = p.Scenes
	}
	if len(p.Clips) > 0 {
		out["clips"] = p.Clips
	}
	out["copy_only"] = p.CopyOnly
	if len(p.Layers) > 0 {
		layers, err := rendermanifest.LayersToMaps(p.Layers)
		if err != nil {
			return nil, fmt.Errorf("contract: layers: %w", err)
		}
		out["layers"] = layers
	}
	if len(p.Items) > 0 {
		out["items"] = p.Items
	}
	if len(p.VoiceoverPaths) > 0 {
		out["voiceover_paths"] = p.VoiceoverPaths
	}
	if p.AudioLanguage != "" {
		out["audio_language_for_srt"] = p.AudioLanguage
	}
	if p.VideoMode != "" {
		out["video_mode"] = p.VideoMode
	}
	if p.Effect != "" {
		out["effect"] = p.Effect
	}
	if p.Orientation != "" {
		out["orientation"] = p.Orientation
	}
	if p.OutputPath != "" {
		out["output_path"] = p.OutputPath
	}
	if p.DriveOutput != "" {
		out["drive_output_folder"] = p.DriveOutput
	}
	if p.ChannelID != "" {
		out["channel_id"] = p.ChannelID
	}
	if p.OutputVideoID != "" {
		out["output_video_id"] = p.OutputVideoID
	}
	if len(p.SceneImagePaths) > 0 {
		out["scene_image_paths"] = p.SceneImagePaths
	}
	if p.ImageSourceMap != "" {
		out["image_source_map"] = p.ImageSourceMap
	}
	if len(p.VideoMetadata) > 0 {
		out["video_metadata"] = cloneObject(p.VideoMetadata)
	}
	if p.SceneCount > 0 {
		out["scene_count"] = p.SceneCount
	}
	if p.VoiceoverCount > 0 {
		out["voiceover_count"] = p.VoiceoverCount
	}
	if p.TotalDurationSecs > 0 {
		out["total_duration_secs"] = p.TotalDurationSecs
	}
	if p.SceneDurationSecs > 0 {
		out["scene_duration_secs"] = p.SceneDurationSecs
	}
	if p.SubmittedVia != "" {
		out["submitted_via"] = p.SubmittedVia
	}
	if p.Source != "" {
		out["source"] = p.Source
	}
	if p.JobFingerprint != "" {
		out["job_fingerprint"] = p.JobFingerprint
	}
	if p.Status != "" {
		if !p.Status.Valid() {
			return nil, fmt.Errorf("invalid input assembly status %q", p.Status)
		}
		out["status"] = p.Status.WireValue()
	}
	if len(p.DeliveryPlan) > 0 || p.deliveryPlanPresent {
		entries := p.DeliveryPlan
		if entries == nil {
			entries = []deliveryplan.Entry{}
		}
		out["delivery_plan"] = deliveryplan.EntriesToMaps(entries)
	}
	return out, nil
}

func deliveryPlanInputPresent(raw map[string]any) bool {
	if raw == nil {
		return false
	}
	for _, key := range []string{"delivery_plan", "delivery_destination_ids", "destination_ids", "delivery_destination_id", "destination_id"} {
		if value, ok := raw[key]; ok && value != nil {
			return true
		}
	}
	if nested, ok := raw["payload"].(map[string]any); ok {
		return deliveryPlanInputPresent(nested)
	}
	return false
}

func isRenderOnlyEmptyDeliveryPlan(raw map[string]any) bool {
	if raw == nil || raw["render_only"] != true {
		return false
	}
	value, ok := raw["delivery_plan"]
	if !ok || value == nil {
		return false
	}
	switch plan := value.(type) {
	case []any:
		return len(plan) == 0
	case []map[string]any:
		return len(plan) == 0
	case []deliveryplan.Entry:
		return len(plan) == 0
	default:
		return false
	}
}
