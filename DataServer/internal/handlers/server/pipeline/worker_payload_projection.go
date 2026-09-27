// Package pipeline — worker_payload_projection.go adapts intake DTOs to the
// pure projection package. The renderer mapping itself lives in projection.
package pipeline

import (
	"strings"

	"velox-shared/assetref"
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
		JobID:                    req.IdempotencyKey,
		JobType:                  req.JobType,
		TemplateID:               req.TemplateID,
		TemplateVersion:          req.TemplateVersion,
		VideoMode:                rendererModeForJobType(req.JobType),
		VideoName:                req.VideoName,
		ScriptText:               req.ScriptText,
		AudioURL:                 req.AudioURL,
		CopyOnly:                 req.CopyOnly,
		CompiledRenderPlanJSON:   req.CompiledRenderPlanJSON,
		CompiledRenderPlanSHA256: req.CompiledRenderPlanSHA256,
		RenderManifest:           req.ResolvedManifest,
		ManifestRef:              req.ResolvedManifestRef,
		ManifestSHA256:           req.ResolvedManifestSHA256,
		PlacementPin:             req.PlacementPinWorkerID,
		LegacyVoiceovers:         compatibility.ReadStringList(req.Spec, compatibility.VoiceoverPathsKey),
		RetryBudgetDefault:       DefaultRetryBudget,
		RuntimeAssets:            req.RuntimeAssets,
		RuntimePayload:           req.RuntimePayload,
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
	input.Overlays = make([]projection.OverlayInput, 0, len(req.Overlays))
	for _, overlay := range req.Overlays {
		overlayURL := overlay.URL
		if overlayURL == "" {
			overlayURL = overlay.DriveLink
		}
		overlayDriveFileID := overlay.DriveFileID
		if overlayDriveFileID == "" {
			if id, err := assetref.ParseDriveFileID(overlayURL); err == nil {
				overlayDriveFileID = id.String()
			}
		}
		overlaySourceURI := overlay.SourceURI
		if overlaySourceURI == "" {
			overlaySourceURI = driveDownloadURI(overlayURL)
		}
		input.Overlays = append(input.Overlays, projection.OverlayInput{
			ID: overlay.ID, AssetID: overlay.AssetID, DriveFileID: overlayDriveFileID, URL: overlayURL, SourceURI: overlaySourceURI, SHA256: overlay.SHA256, SizeBytes: overlay.SizeBytes,
			StartFrame: overlay.StartFrame, EndFrame: overlay.EndFrame, FrameCount: overlay.FrameCount, Mode: overlay.Mode,
			ZIndex: overlay.ZIndex, AudioMode: overlay.AudioMode,
		})
	}
	input.VisualReplacements = make([]projection.VisualReplacementInput, 0, len(req.VisualReplacements))
	for _, replacement := range req.VisualReplacements {
		vr := projection.VisualReplacementInput{
			ReplacementID:   replacement.ReplacementID,
			TimelineStartUS: replacement.TimelineStartUS,
			TimelineEndUS:   replacement.TimelineEndUS,
			ProfileID:       replacement.ProfileID,
		}
		if replacement.Asset != nil {
			vr.AssetID = replacement.Asset.AssetID
			vr.URL = replacement.Asset.URL
			vr.SHA256 = replacement.Asset.SHA256
		}
		input.VisualReplacements = append(input.VisualReplacements, vr)
	}
	raw := projection.BuildRawPayload(input)
	if req.RuntimeAssetsPending {
		raw["runtime_assets_pending"] = true
	}
	return raw
}

func projectionClip(input *SubmitClip) *projection.ClipInput {
	if input == nil {
		return nil
	}
	value := projectionClipValue(input)
	return &value
}

func projectionClipValue(input *SubmitClip) projection.ClipInput {
	url := input.URL
	if url == "" {
		url = input.DriveLink
	}
	driveFileID := input.DriveFileID
	if driveFileID == "" {
		if id, err := assetref.ParseDriveFileID(url); err == nil {
			driveFileID = id.String()
		}
	}
	sourceURI := input.SourceURI
	if sourceURI == "" {
		sourceURI = driveDownloadURI(url)
	}
	return projection.ClipInput{
		AssetID: input.AssetID, DriveFileID: driveFileID, URL: url, SourceURI: sourceURI, SHA256: input.SHA256, SizeBytes: input.SizeBytes,
		StartMS: input.StartMS, EndMS: input.EndMS, DurationMS: input.DurationMS,
	}
}

func driveDownloadURI(reference string) string {
	if id, ok := assetref.WireAssetID(reference); ok && strings.HasPrefix(strings.ToLower(strings.TrimSpace(reference)), assetref.SchemeVeloxDrive+"://") {
		return "https://drive.google.com/uc?export=download&id=" + id
	}
	if id, err := assetref.ParseDriveFileID(reference); err == nil {
		return "https://drive.google.com/uc?export=download&id=" + id.String()
	}
	return ""
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
