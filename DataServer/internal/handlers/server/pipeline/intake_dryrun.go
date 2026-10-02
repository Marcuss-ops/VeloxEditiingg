package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-shared/contract"
)

// IntakeDryRunSummary is the read-only projection returned by dry-run
// intake validation (?dry_run=true). It runs the exact parse + worker
// projection the real intake would run, then reports what the worker
// would compile — without creating forwardings, jobs, or tasks.
//
// It exists so a creator can catch declaration bugs (kind="clip" without
// a clip asset, final-audio shorter than the timeline) in one second
// instead of after a full render. Every number here is derived from the
// same canonical scene parsing the resolver consumes.
type IntakeDryRunSummary struct {
	Scenes              int      `json:"scenes"`
	DeclaredDurationS   float64  `json:"declared_duration_s"`
	ClipScenes          int      `json:"clip_scenes"`
	StockScenes         int      `json:"stock_scenes"`
	VoiceoverScenes     int      `json:"voiceover_scenes"`
	KindClipWithoutClip []string `json:"kind_clip_without_clip"`
	ClipAudioTracks     int      `json:"clip_audio_tracks"`
	// ClipAudioSelected counts clip scenes listed in
	// runtime_audio.clip_audio_scene_ids (worker mixes their original
	// audio with the final narration).
	ClipAudioSelected int `json:"clip_audio_selected"`
	// ClipAudioDropped counts clip scenes coexisting with a final mix
	// without being selected: the worker drops their original audio by
	// design (video-only in the output).
	ClipAudioDropped    int     `json:"clip_audio_dropped"`
	Overlays            int     `json:"overlays"`
	RuntimeAssets       int     `json:"runtime_assets"`
	RuntimeAudioPresent bool    `json:"runtime_audio_present"`
	FinalAudioDurationS float64 `json:"final_audio_duration_s,omitempty"`
	AudioCoverage       string  `json:"audio_coverage"`
	CopyOnly            bool    `json:"copy_only"`
	Warnings            []gin.H `json:"warnings,omitempty"`
}

// Audio coverage states reported by the dry-run summary.
const (
	// DryRunAudioOK: a final mix is present and covers the timeline
	// (the packet-mux gate needs audio >= timeline - 0.05s).
	DryRunAudioOK = "ok"
	// DryRunAudioShort: a final mix is present but shorter than the
	// timeline — the worker would fail closed at the mux gate
	// (audio_duration_mismatch). Fix before submitting.
	DryRunAudioShort = "short_final_mix_mux_would_fail"
	// DryRunAudioMixedTracks: no final mix, but scene clip/voiceover
	// tracks exist — the worker mixes them to its longest track
	// (capped, never extended, to the timeline). Ensure the longest
	// track covers the timeline or the mux will fail.
	DryRunAudioMixedTracks = "mixed_tracks_without_final_mix"
	// DryRunAudioSilent: no audio anywhere; the render would be
	// video-only.
	DryRunAudioSilent = "silent"
)

// SummarizeWorkerPayloadForDryRun builds the dry-run summary from a
// projected worker payload (the map shape ParseRemotePipelineResult +
// projection produce). rawPayload is the pre-projection submission map,
// used only for the soft-deprecation scene scan so both the "scenes"
// list form and the "scenes_json" encoded form are covered.
func SummarizeWorkerPayloadForDryRun(workerPayload, rawPayload map[string]interface{}) IntakeDryRunSummary {
	summary := IntakeDryRunSummary{
		CopyOnly: boolField(workerPayload, "copy_only"),
	}
	scenes := dryRunScenes(workerPayload)
	summary.Scenes = len(scenes)
	for _, scene := range scenes {
		summary.DeclaredDurationS += dryRunNumber(scene["duration_seconds"])
		if _, ok := scene["voiceover"].(map[string]interface{}); ok {
			summary.VoiceoverScenes++
		}
		if dryRunHasAsset(scene["clip"]) {
			summary.ClipScenes++
			summary.ClipAudioTracks++
		}
		if dryRunHasAsset(scene["stock"]) {
			summary.StockScenes++
		}
	}
	for _, anomaly := range contract.FindKindClipAnomaliesInPayload(rawPayload) {
		id := strings.TrimSpace(anomaly.SceneID)
		if id == "" {
			id = fmt.Sprintf("index:%d", anomaly.Index)
		}
		summary.KindClipWithoutClip = append(summary.KindClipWithoutClip, id)
	}
	summary.Overlays = dryRunListLength(workerPayload["overlays"])
	summary.RuntimeAssets = dryRunListLength(workerPayload["runtime_assets"])
	summary.RuntimeAudioPresent = dryRunObjectPresent(workerPayload["runtime_audio"])
	if nested, ok := workerPayload["runtime_payload"].(map[string]interface{}); ok && !summary.RuntimeAudioPresent {
		summary.RuntimeAudioPresent = dryRunObjectPresent(nested["runtime_audio"])
	}
	summary.FinalAudioDurationS = dryRunFinalAudioDuration(workerPayload)
	if summary.RuntimeAudioPresent {
		selected := dryRunClipAudioSelectedIDs(workerPayload)
		for _, scene := range scenes {
			if !dryRunHasAsset(scene["clip"]) {
				continue
			}
			if id, _ := scene["scene_id"].(string); selected[strings.TrimSpace(id)] {
				summary.ClipAudioSelected++
			} else {
				summary.ClipAudioDropped++
			}
		}
	}
	switch {
	case summary.RuntimeAudioPresent && summary.FinalAudioDurationS > 0:
		if summary.FinalAudioDurationS+0.05 >= summary.DeclaredDurationS {
			summary.AudioCoverage = DryRunAudioOK
		} else {
			summary.AudioCoverage = DryRunAudioShort
			summary.Warnings = append(summary.Warnings, gin.H{
				"code": "final_audio_shorter_than_timeline",
				"detail": fmt.Sprintf("final mix %.2fs is shorter than the %.2fs timeline; the worker mux gate would fail closed (audio_duration_mismatch).",
					summary.FinalAudioDurationS, summary.DeclaredDurationS),
			})
		}
	case summary.ClipAudioTracks+summary.VoiceoverScenes > 0:
		summary.AudioCoverage = DryRunAudioMixedTracks
		summary.Warnings = append(summary.Warnings, gin.H{
			"code":   "mixed_tracks_without_final_mix",
			"detail": "no final mix: the worker mixes scene clip/voiceover tracks to its longest track (never extended to the timeline); ensure coverage or the mux will fail.",
		})
	default:
		summary.AudioCoverage = DryRunAudioSilent
	}
	if kindWarnings := sceneKindWarningsForPayload(rawPayload); len(kindWarnings) > 0 {
		summary.Warnings = append(summary.Warnings, kindWarnings...)
	}
	return summary
}

// dryRunScenes decodes the worker payload scene array in either stored
// form (scenes_json string or scenes list), mirroring the renderer
// boundary without importing it.
func dryRunScenes(workerPayload map[string]interface{}) []map[string]interface{} {
	if workerPayload == nil {
		return nil
	}
	if raw, ok := workerPayload["scenes_json"].(string); ok && strings.TrimSpace(raw) != "" {
		var scenes []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &scenes); err == nil {
			return scenes
		}
	}
	if raw, ok := workerPayload["scenes"].([]interface{}); ok {
		scenes := make([]map[string]interface{}, 0, len(raw))
		for _, item := range raw {
			if scene, ok := item.(map[string]interface{}); ok {
				scenes = append(scenes, scene)
			}
		}
		return scenes
	}
	return nil
}

// dryRunHasAsset reports whether a clip/stock field carries at least one
// resolvable asset identity (object or single-element pool).
func dryRunHasAsset(raw interface{}) bool {
	switch value := raw.(type) {
	case map[string]interface{}:
		for _, key := range []string{"asset_id", "drive_file_id", "url"} {
			if text, ok := value[key].(string); ok && strings.TrimSpace(text) != "" {
				return true
			}
		}
	case []interface{}:
		for _, item := range value {
			if dryRunHasAsset(item) {
				return true
			}
		}
	case string:
		return strings.TrimSpace(value) != ""
	}
	return false
}

// dryRunFinalAudioDuration reads the declared final-mix duration: first
// the runtime_audio voiceover_duration_seconds, then the final_mix
// runtime asset duration_ms. Best-effort: 0 when undeclared (the worker
// probes it at render time).
func dryRunFinalAudioDuration(workerPayload map[string]interface{}) float64 {
	audio, _ := workerPayload["runtime_audio"].(map[string]interface{})
	if audio == nil {
		if nested, ok := workerPayload["runtime_payload"].(map[string]interface{}); ok {
			audio, _ = nested["runtime_audio"].(map[string]interface{})
		}
	}
	if audio != nil {
		if duration := dryRunNumber(audio["voiceover_duration_seconds"]); duration > 0 {
			return duration
		}
		if duration := dryRunNumber(audio["duration_seconds"]); duration > 0 {
			return duration
		}
	}
	if assets, ok := workerPayload["runtime_assets"].([]interface{}); ok {
		for _, item := range assets {
			asset, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			finalMix, _ := asset["final_mix"].(bool)
			role, _ := asset["role"].(string)
			if finalMix || role == "final_audio" {
				if durationMS := dryRunNumber(asset["duration_ms"]); durationMS > 0 {
					return durationMS / 1000
				}
			}
		}
	}
	return 0
}

// dryRunClipAudioSelectedIDs reads runtime_audio.clip_audio_scene_ids
// from the worker payload: scenes whose original clip audio the worker
// mixes with the final narration. Mirrors the worker-side selection
// without importing it.
func dryRunClipAudioSelectedIDs(workerPayload map[string]interface{}) map[string]bool {
	selected := make(map[string]bool)
	if workerPayload == nil {
		return selected
	}
	audio, _ := workerPayload["runtime_audio"].(map[string]interface{})
	if audio == nil {
		if nested, ok := workerPayload["runtime_payload"].(map[string]interface{}); ok {
			audio, _ = nested["runtime_audio"].(map[string]interface{})
		}
	}
	if audio == nil {
		return selected
	}
	switch values := audio["clip_audio_scene_ids"].(type) {
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

func dryRunNumber(raw interface{}) float64 {
	switch value := raw.(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case json.Number:
		if parsed, err := value.Float64(); err == nil {
			return parsed
		}
	}
	return 0
}

func dryRunListLength(raw interface{}) int {
	switch value := raw.(type) {
	case []interface{}:
		return len(value)
	case []map[string]interface{}:
		return len(value)
	default:
		return 0
	}
}

func dryRunObjectPresent(raw interface{}) bool {
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

func boolField(payload map[string]interface{}, key string) bool {
	if payload == nil {
		return false
	}
	value, _ := payload[key].(bool)
	return value
}
