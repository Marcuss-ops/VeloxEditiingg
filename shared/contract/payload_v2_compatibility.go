package contract

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/rendermanifest"
	"velox-shared/payload"
)

// NewJobPayloadV2 reads a raw map and returns a populated JobPayloadV2.
// Legacy aliases remain read-only fallbacks and are never emitted by ToMap.
func NewJobPayloadV2(raw map[string]any) *JobPayloadV2 {
	if raw == nil {
		raw = map[string]any{}
	}
	now := time.Now().UTC().Format(time.RFC3339)
	p := &JobPayloadV2{
		ContractVersion:        ContractVersionV2,
		PayloadContractVersion: PayloadContractVersionCanonical,
		JobID:                  payload.FirstString(raw, "job_id", "script_id"),
		JobRunID:               payload.FirstString(raw, "job_run_id", "run_id"),
		CorrelationID:          payload.FirstString(raw, "correlation_id"),
		JobType:                payload.FirstString(raw, "job_type"),
		TemplateID:             payload.FirstString(raw, "template_id"),
		TemplateVersion:        payload.EnsureInt(raw["template_version"], 0),
		Version:                "v2",
		CreatedAt:              payload.EnsureRFC3339(payload.FirstString(raw, "created_at"), now),
		UpdatedAt:              payload.EnsureRFC3339(payload.FirstString(raw, "updated_at"), now),
		VideoName:              payload.FirstString(raw, "video_name", "title", "project_name"),
		ScriptText:             payload.FirstString(raw, "script_text", "script", "source_text"),
		ScenesJSON:             payload.FirstString(raw, "scenes_json"),
		VoiceoverPaths:         append([]string{}, readCanonicalVoiceoverPaths(raw)...),
		AudioLanguage:          payload.FirstString(raw, "audio_language_for_srt", "audio_lang", "language"),
		VideoMode:              payload.FirstString(raw, "video_mode"),
		Effect:                 payload.FirstString(raw, "effect"),
		Orientation:            payload.FirstString(raw, "orientation"),
		OutputPath:             payload.FirstString(raw, "output_path"),
		DriveOutput:            payload.FirstString(raw, "drive_output_folder", "output_directory"),
		ChannelID:              payload.FirstString(raw, "channel_id"),
		SceneImagePaths:        append([]string{}, payload.NormalizeStringList(raw, "scene_image_paths")...),
		Priority:               payload.EnsureInt(raw["priority"], 1),
		TimeoutSecs:            payload.EnsureInt(raw["timeout_secs"], 3600),
		SubmittedVia:           payload.FirstString(raw, "submitted_via"),
		Source:                 payload.FirstString(raw, "source"),
		Status:                 parseInputAssemblyOrLegacy(raw["status"]),
	}
	if value, ok := raw["delivery_plan"]; ok && value != nil {
		p.deliveryPlanPresent = true
	}
	if deliveryPlanInputPresent(raw) {
		if entries, err := deliveryplan.Parse(raw); err == nil {
			p.DeliveryPlan = entries
		} else if isRenderOnlyEmptyDeliveryPlan(raw) {
			p.DeliveryPlan = []deliveryplan.Entry{}
		}
	}
	if p.Status == "" {
		p.Status = InputAssemblyPending
	}
	if p.JobType == "" {
		p.JobType = "process_video"
	}
	if metadata, ok := raw["video_metadata"].(map[string]any); ok {
		p.VideoMetadata = cloneObject(metadata)
	}
	p.Canvas = rendermanifest.CanvasFromMap(objectMap(raw["video_metadata"]), rendermanifest.DefaultCanvas())
	if manifest, ok := raw["render_manifest"].(map[string]any); ok {
		p.RenderManifest = cloneObject(manifest)
	}
	if manifestRef, ok := raw["manifest_ref"].(map[string]any); ok {
		p.ManifestRef = cloneObject(manifestRef)
	}
	p.ManifestSHA256 = payload.FirstString(raw, "manifest_sha256")
	p.RenderPlanJSON = payload.FirstString(raw, "render_plan_json")
	p.RenderPlanSHA256 = payload.FirstString(raw, "render_plan_sha256")
	p.CompiledRenderPlanJSON = payload.FirstString(raw, PayloadKeyCompiledRenderPlanJSON)
	p.CompiledRenderPlanSHA256 = payload.FirstString(raw, PayloadKeyCompiledRenderPlanSHA)
	if clipsVal, ok := raw["clips"]; ok {
		p.Clips = normalizeObjectList(clipsVal)
	}
	if copyOnly, ok := raw["copy_only"].(bool); ok {
		p.CopyOnly = copyOnly
	}
	if scenesVal, ok := raw["scenes"]; ok {
		switch s := scenesVal.(type) {
		case []map[string]any:
			p.Scenes = append([]map[string]any{}, s...)
		case []any:
			out := make([]map[string]any, 0, len(s))
			for _, item := range s {
				if m, ok := item.(map[string]any); ok {
					out = append(out, m)
				}
			}
			p.Scenes = out
		}
	}
	if layersVal, ok := raw["layers"]; ok {
		p.Layers = normalizeLayers(layersVal)
	}
	if itemsVal, ok := raw["items"]; ok {
		p.Items = normalizeObjectList(itemsVal)
	}
	if p.JobID == "" {
		p.JobID = "scriptimg_" + uuid.NewString()
	}
	if p.JobRunID == "" {
		p.JobRunID = "run_" + uuid.NewString()
	}
	if p.CorrelationID == "" {
		p.CorrelationID = "corr_" + uuid.NewString()
	}
	if p.SceneCount == 0 && len(p.Scenes) > 0 {
		p.SceneCount = len(p.Scenes)
	}
	if p.VoiceoverCount == 0 && len(p.VoiceoverPaths) > 0 {
		p.VoiceoverCount = len(p.VoiceoverPaths)
	}
	return p
}

func parseInputAssemblyOrLegacy(value any) InputAssemblyStatus {
	raw, ok := value.(string)
	if !ok {
		return ""
	}
	status, ok := ParseInputAssemblyStatus(raw)
	if !ok {
		return InputAssemblyStatus(strings.TrimSpace(raw))
	}
	return status
}

func cloneObject(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func objectMap(value any) map[string]any { m, _ := value.(map[string]any); return m }

func normalizeObjectList(value any) []map[string]any {
	switch items := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			out = append(out, cloneObject(item))
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			if object, ok := item.(map[string]any); ok {
				out = append(out, cloneObject(object))
			}
		}
		return out
	default:
		return nil
	}
}

func normalizeLayers(value any) []rendermanifest.Layer {
	switch items := value.(type) {
	case []rendermanifest.Layer:
		return append([]rendermanifest.Layer(nil), items...)
	case []map[string]any:
		out := make([]rendermanifest.Layer, 0, len(items))
		for index, item := range items {
			out = append(out, rendermanifest.LayerFromMap(item, index))
		}
		return out
	case []any:
		out := make([]rendermanifest.Layer, 0, len(items))
		for index, item := range items {
			if object, ok := item.(map[string]any); ok {
				out = append(out, rendermanifest.LayerFromMap(object, index))
			}
		}
		return out
	default:
		return nil
	}
}

// UnmarshalJSON keeps direct encoding/json readers compatible with legacy rows.
func (p *JobPayloadV2) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw == nil {
		*p = JobPayloadV2{}
		return nil
	}
	*p = *NewJobPayloadV2(raw)
	return nil
}

// JobPayloadV2FromJSON parses JSON into a typed payload using legacy-tolerant reads.
func JobPayloadV2FromJSON(data []byte) (*JobPayloadV2, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, err
	}
	return NewJobPayloadV2(raw), nil
}
