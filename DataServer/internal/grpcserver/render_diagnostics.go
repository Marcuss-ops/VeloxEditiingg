package grpcserver

import (
	"encoding/json"
	"fmt"
	"strings"
)

type renderInputDiagnostic struct {
	Scenes                  int
	DeclaredSceneDurationS  float64
	DuplicateClipStockCount int
	DuplicateSceneIndexes   []int
	RuntimeAssets           int
	RuntimeAudioPresent     bool
	Overlays                int
	CopyOnly                bool
	RuntimeAssetsPending    bool
	// KindClipWithoutClip counts scenes declared kind="clip" without a
	// clip asset. The worker compiles them as mute stock backgrounds
	// (soft-deprecated at intake since 2026-10-02; see
	// shared/contract/scene_kind_clip.go). Non-zero here on a legacy
	// job means testimony audio never reached the montage.
	KindClipWithoutClip int
	// ClipAudioSelected counts scenes carrying a clip asset that are
	// listed in runtime_audio.clip_audio_scene_ids: the worker mixes
	// their original audio with the final narration (editorial render).
	ClipAudioSelected int
	// ClipAudioDropped counts scenes carrying a clip asset while a
	// final runtime mix is present WITHOUT being selected in
	// clip_audio_scene_ids: the worker drops their original audio by
	// design (the mix owns the timeline), so these sources stay
	// video-only in the output.
	ClipAudioDropped int
	Diagnosis        string
}

// diagnoseRenderInput summarizes renderer-relevant fields without logging
// asset URLs, credentials, script text, or other payload contents.
func diagnoseRenderInput(payload map[string]interface{}) renderInputDiagnostic {
	d := renderInputDiagnostic{
		CopyOnly:             boolField(payload, "copy_only"),
		RuntimeAudioPresent:  runtimeAudioPresent(payload),
		RuntimeAssets:        listLength(payload["runtime_assets"]),
		Overlays:             listLength(payload["overlays"]),
		RuntimeAssetsPending: boolField(payload, "runtime_assets_pending"),
	}
	if raw, ok := payload["scenes_json"].(string); ok {
		var scenes []map[string]interface{}
		if json.Unmarshal([]byte(raw), &scenes) == nil {
			d.inspectScenes(scenes, clipAudioSelectedIDs(runtimeAudioOf(payload)))
		}
	} else if raw, ok := payload["scenes"].([]interface{}); ok {
		scenes := make([]map[string]interface{}, 0, len(raw))
		for _, item := range raw {
			if scene, ok := item.(map[string]interface{}); ok {
				scenes = append(scenes, scene)
			}
		}
		d.inspectScenes(scenes, clipAudioSelectedIDs(runtimeAudioOf(payload)))
	}
	if d.DuplicateClipStockCount > 0 {
		d.Diagnosis = "clip_and_stock_are_additive; identical_asset_will_be_rendered_twice"
	}
	return d
}

func runtimeAudioPresent(payload map[string]interface{}) bool {
	if objectPresent(payload["runtime_audio"]) {
		return true
	}
	runtimePayload, _ := payload["runtime_payload"].(map[string]interface{})
	return objectPresent(runtimePayload["runtime_audio"])
}

func (d *renderInputDiagnostic) inspectScenes(scenes []map[string]interface{}, selected map[string]bool) {
	d.Scenes = len(scenes)
	for index, scene := range scenes {
		d.DeclaredSceneDurationS += numericField(scene, "duration_seconds")
		clip := scene["clip"]
		if !objectPresent(clip) {
			if kind, _ := scene["kind"].(string); strings.EqualFold(strings.TrimSpace(kind), "clip") {
				d.KindClipWithoutClip++
			}
			continue
		}
		if !d.RuntimeAudioPresent {
			continue
		}
		if id, _ := scene["scene_id"].(string); selected[strings.TrimSpace(id)] {
			d.ClipAudioSelected++
		} else {
			d.ClipAudioDropped++
		}
		clipIdentity := mediaIdentity(clip)
		if clipIdentity == "" {
			continue
		}
		for _, stock := range mediaList(scene["stock"]) {
			if mediaIdentity(stock) == clipIdentity {
				d.DuplicateClipStockCount++
				d.DuplicateSceneIndexes = append(d.DuplicateSceneIndexes, index)
				break
			}
		}
	}
}

func runtimeAudioOf(payload map[string]interface{}) map[string]interface{} {
	if audio, ok := payload["runtime_audio"].(map[string]interface{}); ok {
		return audio
	}
	if nested, ok := payload["runtime_payload"].(map[string]interface{}); ok {
		if audio, ok := nested["runtime_audio"].(map[string]interface{}); ok {
			return audio
		}
	}
	return nil
}

// clipAudioSelectedIDs reads runtime_audio.clip_audio_scene_ids: the
// scene IDs whose original clip audio the worker mixes with the final
// narration. Unselected clip scenes coexisting with a final mix stay
// video-only by design.
func clipAudioSelectedIDs(runtimeAudio map[string]interface{}) map[string]bool {
	selected := make(map[string]bool)
	if runtimeAudio == nil {
		return selected
	}
	switch values := runtimeAudio["clip_audio_scene_ids"].(type) {
	case []interface{}:
		for _, value := range values {
			if id, ok := value.(string); ok && strings.TrimSpace(id) != "" {
				selected[strings.TrimSpace(id)] = true
			}
		}
	case []string:
		for _, id := range values {
			if strings.TrimSpace(id) != "" {
				selected[strings.TrimSpace(id)] = true
			}
		}
	}
	return selected
}

func mediaList(raw interface{}) []interface{} {
	switch value := raw.(type) {
	case []interface{}:
		return value
	case map[string]interface{}:
		return []interface{}{value}
	default:
		return nil
	}
}

func mediaIdentity(raw interface{}) string {
	item, ok := raw.(map[string]interface{})
	if !ok {
		return ""
	}
	for _, key := range []string{"asset_id", "drive_file_id", "url"} {
		if value, ok := item[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func objectPresent(raw interface{}) bool {
	switch value := raw.(type) {
	case map[string]interface{}:
		return len(value) > 0
	case []interface{}:
		return len(value) > 0
	case []map[string]interface{}:
		return len(value) > 0
	default:
		return false
	}
}

func listLength(raw interface{}) int {
	switch value := raw.(type) {
	case []interface{}:
		return len(value)
	case []map[string]interface{}:
		return len(value)
	default:
		return 0
	}
}

func boolField(payload map[string]interface{}, key string) bool {
	value, _ := payload[key].(bool)
	return value
}

func numericField(payload map[string]interface{}, key string) float64 {
	switch value := payload[key].(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return 0
	}
}

func renderFailureDiagnosis(detail string) (category, diagnosis, action, audioDuration string) {
	lower := strings.ToLower(detail)
	audioDuration = "unknown"
	for _, field := range strings.Fields(detail) {
		if strings.HasPrefix(field, "duration_seconds=") {
			audioDuration = strings.Trim(strings.TrimPrefix(field, "duration_seconds="), `",`)
			break
		}
	}
	switch {
	case strings.Contains(lower, "audio_duration_mismatch"):
		return "audio_duration_mismatch",
			"prepared_audio_is_shorter_than_the_compiled_video_timeline",
			"compare the final audio duration with the compiled timeline; check for scenes that repeat the same asset in both clip and stock",
			audioDuration
	case strings.Contains(lower, "mixed_audio_transform_unsupported"):
		return "mixed_audio_transform_unsupported",
			"copy_only_mixer_cannot_apply_the_requested_audio_transform",
			"prepare one verified final AAC mix or use a render mode that supports the requested transform",
			audioDuration
	case strings.Contains(lower, "mixed_audio_download_failed"):
		return "mixed_audio_download_failed",
			"worker_could_not_resolve_an_audio_asset",
			"check that every runtime audio asset is finalized, prefetched, and resolvable by the worker",
			audioDuration
	default:
		return "worker_execution_failed", fmt.Sprintf("worker returned failure: %s", compactDiagnostic(detail)), "inspect worker_detail for the failing render phase", audioDuration
	}
}

func compactDiagnostic(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 360 {
		return value[:360] + "…"
	}
	return value
}
