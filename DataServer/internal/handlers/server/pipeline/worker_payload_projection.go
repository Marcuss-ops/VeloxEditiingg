// Package pipeline — worker_payload_projection.go adapts intake DTOs to the
// pure projection package. The renderer mapping itself lives in projection.
package pipeline

import (
	"velox-shared/compatibility"

	"velox-server/internal/handlers/server/pipeline/projection"
)

// projectWorkerPayload follows the canonical remoteengine DTO path.
func projectWorkerPayload(req *SubmitJobRequest) (map[string]interface{}, error) {
	return projection.ProjectWorkerPayload(submitRequestToRawPayload(req), rendererModeForJobType(req.JobType))
}

// submitRequestToRawPayload is the intake adapter. It translates the HTTP DTO
// into projection-neutral values; no renderer field mapping or HTTP behavior
// belongs here.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	input := projection.SubmissionInput{
		JobID:              req.IdempotencyKey,
		JobType:            req.JobType,
		TemplateID:         req.TemplateID,
		TemplateVersion:    req.TemplateVersion,
		VideoMode:          rendererModeForJobType(req.JobType),
		VideoName:          req.VideoName,
		ScriptText:         req.ScriptText,
		Spec:               req.Spec,
		RenderManifest:     req.ResolvedManifest,
		ManifestRef:        req.ResolvedManifestRef,
		ManifestSHA256:     req.ResolvedManifestSHA256,
		PlacementPin:       req.PlacementPinWorkerID,
		LegacyVoiceovers:   compatibility.ReadStringList(req.Spec, compatibility.VoiceoverPathsKey),
		RetryBudgetDefault: DefaultRetryBudget,
	}
	if req.Output != nil {
		input.Output = &projection.OutputInput{
			Width: req.Output.Width, Height: req.Output.Height, FPS: req.Output.FPS, Format: req.Output.Format,
		}
	}
	input.DeliveryPlan = make([]projection.RawDeliveryPlanEntry, 0, len(req.DeliveryPlan))
	for _, entry := range req.DeliveryPlan {
		input.DeliveryPlan = append(input.DeliveryPlan, projection.RawDeliveryPlanEntry{
			DestinationID: entry.DestinationID,
			Priority:      entry.Priority,
			RetryBudget:   entry.RetryBudget,
			Metadata:      entry.Metadata,
		})
	}
	input.Scenes = make([]projection.SceneInput, 0, len(req.Scenes))
	for _, scene := range req.Scenes {
		input.Scenes = append(input.Scenes, projection.SceneInput{
			Text: scene.Text, SceneID: scene.SceneID, Index: scene.Index, Kind: scene.Kind,
			DurationSeconds: scene.DurationSeconds, StockFallback: scene.StockFallback,
			Clip:      projectionClip(scene.Clip),
			Stock:     projectionClip(scene.Stock),
			Voiceover: projectionVoiceover(scene.Voiceover),
			Subtitles: projectionSubtitles(scene.Subtitles),
		})
		last := &input.Scenes[len(input.Scenes)-1]
		if len(scene.StockAssets) > 0 {
			last.StockAssets = make([]projection.ClipInput, 0, len(scene.StockAssets))
			for i := range scene.StockAssets {
				last.StockAssets = append(last.StockAssets, projectionClipValue(&scene.StockAssets[i]))
			}
		}
	}
	input.Layers = make([]projection.LayerInput, 0, len(req.Layers))
	for _, layer := range req.Layers {
		input.Layers = append(input.Layers, projection.LayerInput{
			ID: layer.ID, Type: layer.Type, Role: layer.Role, Text: layer.Text, Asset: layer.Asset,
			Source: layer.Source, Font: layer.Font, FontSize: layer.FontSize, Position: layer.Position,
			StartSeconds: layer.StartSeconds, DurationSeconds: layer.DurationSeconds,
			Preset: layer.Preset, Animation: layer.Animation,
		})
	}
	input.AudioTracks = make([]projection.AudioTrackInput, 0, len(req.AudioTracks))
	for _, track := range req.AudioTracks {
		input.AudioTracks = append(input.AudioTracks, projection.AudioTrackInput{
			AssetID: track.AssetID, SourceURL: track.SourceURL, Role: track.Role, Volume: track.Volume,
			StartTimeOffset: track.StartTimeOffset, DurationSeconds: track.DurationSeconds, Loop: track.Loop,
			FadeInSeconds: track.FadeInSeconds, FadeOutSeconds: track.FadeOutSeconds,
			DuckingEnabled: track.DuckingEnabled,
		})
	}
	return projection.BuildRawPayload(input)
}

func projectionClip(input *SubmitClip) *projection.ClipInput {
	if input == nil {
		return nil
	}
	value := projectionClipValue(input)
	return &value
}

func projectionClipValue(input *SubmitClip) projection.ClipInput {
	return projection.ClipInput{
		AssetID: input.AssetID, DriveFileID: input.DriveFileID, URL: input.URL, SHA256: input.SHA256,
		StartMS: input.StartMS, EndMS: input.EndMS, DurationMS: input.DurationMS,
	}
}

func projectionVoiceover(input *SubmitVoiceover) *projection.VoiceoverInput {
	if input == nil {
		return nil
	}
	return &projection.VoiceoverInput{
		AssetID: input.AssetID, DriveFileID: input.DriveFileID, URL: input.URL, SHA256: input.SHA256,
		SizeBytes: input.SizeBytes, DurationMS: input.DurationMS, Language: input.Language,
	}
}

func projectionSubtitles(input *SubmitSubtitles) *projection.SubtitlesInput {
	if input == nil {
		return nil
	}
	return &projection.SubtitlesInput{
		AssetID: input.AssetID, Format: input.Format, URL: input.URL, SHA256: input.SHA256,
		Language: input.Language,
	}
}

func rendererModeForJobType(jobType string) string {
	recipe, ok := ResolveRecipe(jobType)
	if !ok {
		return ""
	}
	return recipe.RendererMode
}
