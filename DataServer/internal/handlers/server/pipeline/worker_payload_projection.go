// Package pipeline — worker_payload_projection.go owns the final DTO → worker projection.
package pipeline

import (
	"fmt"
	"strings"

	"velox-server/internal/remoteengine"
	"velox-shared/compatibility"
)

// projectWorkerPayload follows the canonical remoteengine DTO path.
func projectWorkerPayload(req *SubmitJobRequest) map[string]interface{} {
	rawPayload := submitRequestToRawPayload(req)
	dto, err := remoteengine.ParseRemotePipelineResult(rawPayload)
	if err != nil {
		// This helper predates the error-returning canonical facade. Keep its
		// map return shape for compatibility, but fail closed instead of
		// silently projecting an empty DTO.
		return map[string]interface{}{"_canonical_projection_error": fmt.Sprintf("parse remote pipeline result: %v", err)}
	}
	workerPayload := dto.ToWorkerPayload()
	preserveWorkerPayloadFields(workerPayload, rawPayload, "subtitle_tracks", "audio_tracks", "layers", "_placement_pin_worker_id")
	return workerPayload
}

func preserveWorkerPayloadFields(dst, src map[string]interface{}, keys ...string) {
	if dst == nil || src == nil {
		return
	}
	for _, key := range keys {
		if value, ok := src[key]; ok && value != nil {
			dst[key] = value
		}
	}
}

// submitRequestToRawPayload builds the canonical flat-map shape consumed
// by remoteengine.ParseRemotePipelineResult. It owns the DTO boundary,
// nested scene projection, legacy aliases, and delivery retry defaults.
// Identity-bearing URLs and IDs are trimmed; content fields remain verbatim.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		"status": "completed",
		"job_id": strings.TrimSpace(req.IdempotencyKey),
	}
	if req.JobType != "" {
		m["job_type"] = strings.TrimSpace(req.JobType)
	}
	if req.TemplateID != "" {
		m["template_id"] = strings.TrimSpace(req.TemplateID)
	}
	if req.TemplateVersion > 0 {
		m["template_version"] = req.TemplateVersion
	}
	if mode := rendererModeForJobType(req.JobType); mode != "" {
		m["video_mode"] = mode
	}

	if req.VideoName != "" {
		m["video_name"] = strings.TrimSpace(req.VideoName)
	}
	if req.ScriptText != "" {
		m["script_text"] = req.ScriptText
	}
	if len(req.Spec) > 0 {
		m["spec"] = req.Spec
	}
	if req.Output != nil {
		m["output"] = map[string]interface{}{
			"width":  req.Output.Width,
			"height": req.Output.Height,
			"fps":    req.Output.FPS,
			"format": req.Output.Format,
		}
	}
	if req.ResolvedManifest != nil {
		m["render_manifest"] = req.ResolvedManifest
	}
	if req.ResolvedManifestRef != nil {
		m["manifest_ref"] = req.ResolvedManifestRef
	}
	if req.ResolvedManifestSHA256 != "" {
		m["manifest_sha256"] = req.ResolvedManifestSHA256
	}
	if len(req.VoiceoverPaths) > 0 {
		// NormalizeToStrings shape matches what
		// extractVoiceoverPathsDTO scans for.
		//
		// Phase-2 note: the per-scene voiceover.url (when present)
		// is the SOURCE OF TRUTH; this top-level array is preserved
		// for back-compat with legacy worker consumers that read
		// voiceover_paths[] directly. ToWorkerPayload (remoteengine
		// side) merges both sources into a single deduped array
		// so the legacy field stays consistent for old workers
		// even when new clients send only the per-scene form.
		m[compatibility.VoiceoverPathsKey] = req.VoiceoverPaths
	}

	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for i, s := range req.Scenes {
			scene := map[string]interface{}{
				"text":             s.Text,
				"duration_seconds": s.DurationSeconds,
			}
			if s.SceneID != "" {
				scene["scene_id"] = strings.TrimSpace(s.SceneID)
			}
			if s.Index > 0 {
				scene["index"] = s.Index
			}
			if s.Kind != "" {
				scene["kind"] = strings.TrimSpace(s.Kind)
			}
			// Legacy flat-shape alias keys: preserved verbatim when
			// supplied, so old clients that haven't migrated still
			// see a working round-trip. When the nested Clip{}
			// also carries a URL, BOTH end up in the map — the
			// worker's scenes_json consumer picks the nested form
			// (authoritative) but the legacy key remains visible
			// to any code that still reads `clip_link` directly.
			if s.ClipLink != "" {
				scene["clip_link"] = strings.TrimSpace(s.ClipLink)
			}
			if s.ImageLink != "" {
				scene["image_link"] = strings.TrimSpace(s.ImageLink)
			}
			// Per-scene nested objects (Phase 2): clip / voiceover /
			// subtitles carry their own asset references so the
			// worker reads the canonical URL directly from
			// scenes_json[i].voiceover.url (no more positional
			// coupling with top-level voiceover_paths[]).
			if s.Clip != nil {
				scene["clip"] = clipToMap(s.Clip)
			}
			if s.Stock != nil {
				scene["stock"] = clipToMap(s.Stock)
			}
			if len(s.StockLinks) > 0 {
				scene["stock_links"] = append([]string(nil), s.StockLinks...)
			}
			if s.StockFallback {
				scene["stock_fallback"] = true
			}
			if s.Voiceover != nil {
				scene["voiceover"] = voiceoverToMap(s.Voiceover)
			} else if i < len(req.VoiceoverPaths) && strings.TrimSpace(req.VoiceoverPaths[i]) != "" {
				scene["voiceover"] = map[string]interface{}{
					"url": strings.TrimSpace(req.VoiceoverPaths[i]),
				}
			}
			if s.Subtitles != nil {
				scene["subtitles"] = subtitlesToMap(s.Subtitles)
			}
			scenes = append(scenes, scene)
		}
		m["scenes"] = scenes
	}
	if len(req.Layers) > 0 {
		layers := make([]interface{}, 0, len(req.Layers))
		for _, layer := range req.Layers {
			entry := map[string]interface{}{"id": strings.TrimSpace(layer.ID), "type": strings.TrimSpace(layer.Type)}
			for key, value := range map[string]interface{}{
				"role": layer.Role, "text": layer.Text, "asset": layer.Asset, "source": layer.Source,
				"font": layer.Font, "font_size": layer.FontSize, "position": layer.Position,
				"start_seconds": layer.StartSeconds, "duration_seconds": layer.DurationSeconds,
				"preset": layer.Preset, "animation": layer.Animation,
			} {
				if value != nil && value != "" && !(key == "position" && len(layer.Position) == 0) && !(key != "position" && value == float64(0)) {
					entry[key] = value
				}
			}
			layers = append(layers, entry)
		}
		m["layers"] = layers
	}
	if len(req.SubtitleTracks) > 0 {
		subtitles := make([]interface{}, 0, len(req.SubtitleTracks))
		for _, track := range req.SubtitleTracks {
			subtitles = append(subtitles, map[string]interface{}{"source": strings.TrimSpace(track.Source), "preset": track.Preset, "font": track.Font})
		}
		m["subtitle_tracks"] = subtitles
	}
	if len(req.AudioTracks) > 0 {
		audioTracks := make([]interface{}, 0, len(req.AudioTracks))
		for _, track := range req.AudioTracks {
			audioTracks = append(audioTracks, audioTrackToMap(track))
		}
		m["audio_tracks"] = audioTracks
	}

	if req.PlacementPinWorkerID != "" {
		m["_placement_pin_worker_id"] = strings.TrimSpace(req.PlacementPinWorkerID)
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": strings.TrimSpace(d.DestinationID),
			}
			if d.Priority > 0 {
				entry["priority"] = d.Priority
			}
			if d.RetryBudget == nil {
				entry["retry_budget"] = DefaultRetryBudget
			} else {
				entry["retry_budget"] = *d.RetryBudget
			}
			// Delivery routing remains available for legacy enqueue logic,
			// but publication metadata never crosses into the renderer map.
			// Titles, descriptions, tags, privacy, and scheduling belong to
			// PublicationSpecs on the control plane.
			plan = append(plan, entry)
		}
		m["delivery_plan"] = plan
	}

	return m
}

func rendererModeForJobType(jobType string) string {
	recipe, ok := ResolveRecipe(jobType)
	if !ok {
		return ""
	}
	return recipe.RendererMode
}
