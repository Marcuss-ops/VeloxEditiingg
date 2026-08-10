// Package pipeline — worker_payload_projection.go owns the final DTO → worker projection.
package pipeline

import (
	"strings"

	"velox-shared/compatibility"

	"velox-server/internal/handlers/server/pipeline/projection"
)

// projectWorkerPayload follows the canonical remoteengine DTO path.
func projectWorkerPayload(req *SubmitJobRequest) (map[string]interface{}, error) {
	rawPayload := submitRequestToRawPayload(req)
	return projection.ProjectWorkerPayload(rawPayload, rendererModeForJobType(req.JobType))
}

// submitRequestToRawPayload preserves the package-local caller contract while
// delegating top-level envelope construction to projection. Nested scenes and
// layers remain here until their independent DTO boundary is extracted.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	rawPayload := projection.BuildRawPayloadEnvelope(projection.RawPayloadInput{
		JobID:              req.IdempotencyKey,
		JobType:            req.JobType,
		TemplateID:         req.TemplateID,
		TemplateVersion:    req.TemplateVersion,
		VideoMode:          rendererModeForJobType(req.JobType),
		VideoName:          req.VideoName,
		ScriptText:         req.ScriptText,
		Spec:               req.Spec,
		Output:             submitOutputToMap(req.Output),
		RenderManifest:     req.ResolvedManifest,
		ManifestRef:        req.ResolvedManifestRef,
		ManifestSHA256:     req.ResolvedManifestSHA256,
		PlacementPin:       req.PlacementPinWorkerID,
		LegacyVoiceovers:   compatibility.ReadStringList(req.Spec, compatibility.VoiceoverPathsKey),
		DeliveryPlan:       submitDeliveryPlanEntries(req.DeliveryPlan),
		RetryBudgetDefault: DefaultRetryBudget,
	})

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
			if s.Clip != nil {
				scene["clip"] = clipToMap(s.Clip)
			}
			if s.Stock != nil {
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
		rawPayload["scenes"] = scenes
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
		rawPayload["layers"] = layers
	}
	if len(req.AudioTracks) > 0 {
		// Explicit tracks win over the legacy voiceover_paths compatibility
		// alias already projected by BuildRawPayloadEnvelope.
		audioTracks := make([]interface{}, 0, len(req.AudioTracks))
		for _, track := range req.AudioTracks {
			audioTracks = append(audioTracks, audioTrackToMap(track))
		}
		rawPayload["audio_tracks"] = audioTracks
	}
	return rawPayload
}

func submitOutputToMap(output *SubmitOutput) map[string]interface{} {
	if output == nil {
		return nil
	}
	return map[string]interface{}{
		"width": output.Width, "height": output.Height, "fps": output.FPS, "format": output.Format,
	}
}

func submitDeliveryPlanEntries(entries []SubmitDeliveryPlanEntry) []projection.RawDeliveryPlanEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]projection.RawDeliveryPlanEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, projection.RawDeliveryPlanEntry{
			DestinationID: entry.DestinationID,
			Priority:      entry.Priority,
			RetryBudget:   entry.RetryBudget,
			Metadata:      entry.Metadata,
		})
	}
	return out
}

func rendererModeForJobType(jobType string) string {
	recipe, ok := ResolveRecipe(jobType)
	if !ok {
		return ""
	}
	return recipe.RendererMode
}
