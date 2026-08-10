// Package pipeline — worker_payload_projection.go owns the final DTO → worker projection.
package pipeline

import (
	"strings"

	"velox-shared/compatibility"
	"velox-shared/contract"

	"velox-server/internal/handlers/server/pipeline/projection"
)

// projectWorkerPayload follows the canonical remoteengine DTO path.
func projectWorkerPayload(req *SubmitJobRequest) (map[string]interface{}, error) {
	rawPayload := submitRequestToRawPayload(req)
	return projection.ProjectWorkerPayload(rawPayload, rendererModeForJobType(req.JobType))
}

// submitRequestToRawPayload builds the canonical flat-map shape consumed
// by remoteengine.ParseRemotePipelineResult. It owns the DTO boundary,
// nested scene projection, and delivery retry defaults.

// Identity-bearing URLs and IDs are trimmed; content fields remain verbatim.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		// This is the producer-side input handoff state, not a terminal job
		// lifecycle state. Keep the historical wire value while making the
		// domain semantics explicit at the submission boundary.
		"status": string(contract.InputAssemblyCompleted),
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
	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for _, s := range req.Scenes {
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
			// Per-scene nested objects (Phase 2): clip / voiceover /
			// subtitles carry their own asset references so the
			// worker reads the canonical URL directly from
			// scenes_json[i].voiceover.url (no more positional
			// coupling with any top-level positional array.
			if s.Clip != nil {
				scene["clip"] = clipToMap(s.Clip)
			}
			if s.Stock != nil {
				// The renderer contract is always scene.stock[] even when
				// the recipe contains a single stock object. Keeping the
				// array shape here is essential: the canonical timeline
				// builder uses one shared stock-list path for every scene.
				scene["stock"] = []interface{}{clipToMap(s.Stock)}
			}
			if len(s.StockAssets) > 0 {
				stock := make([]interface{}, 0, len(s.StockAssets))
				for i := range s.StockAssets {
					asset := s.StockAssets[i]
					stock = append(stock, clipToMap(&asset))
				}
				scene["stock"] = stock
			}
			if s.StockFallback {
				scene["stock_fallback"] = true
			}
			if s.Voiceover != nil {
				scene["voiceover"] = voiceoverToMap(s.Voiceover)
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
	if len(req.AudioTracks) > 0 {
		audioTracks := make([]interface{}, 0, len(req.AudioTracks))
		for _, track := range req.AudioTracks {
			audioTracks = append(audioTracks, audioTrackToMap(track))
		}
		m["audio_tracks"] = audioTracks
	} else if legacyVoiceovers := compatibility.ReadStringList(req.Spec, compatibility.VoiceoverPathsKey); len(legacyVoiceovers) > 0 {
		// Compatibility aliases are read only at this boundary and are
		// immediately projected into the canonical renderer shape. They
		// must never cross the worker boundary as positional fields.
		audioTracks := make([]interface{}, 0, len(legacyVoiceovers))
		for _, sourceURL := range legacyVoiceovers {
			audioTracks = append(audioTracks, map[string]interface{}{
				"source_url": sourceURL,
				"role":       "voiceover",
			})
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
			if d.Metadata != nil {
				// Destination metadata is control-plane routing data. It must
				// survive the external DTO projection so providers can honor
				// per-job targets such as a Drive folder, while it remains
				// excluded from the renderer worker payload.
				entry["metadata"] = d.Metadata
			}
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
