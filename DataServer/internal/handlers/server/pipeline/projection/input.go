package projection

import (
	"strings"

	"velox-shared/assetref"
)

// SubmissionInput is the projection boundary between intake DTOs and the
// renderer payload. It contains no HTTP, Gin, publishing, security, or
// pipeline-package types.
type SubmissionInput struct {
	JobID              string
	JobType            string
	TemplateID         string
	TemplateVersion    int
	VideoMode          string
	VideoName          string
	ScriptText         string
	AudioURL           string
	CopyOnly           bool
	RenderManifest     map[string]interface{}
	ManifestRef        map[string]interface{}
	ManifestSHA256     string
	PlacementPin       string
	LegacyVoiceovers   []string
	Scenes             []SceneInput
	Layers             []LayerInput
	VisualReplacements []VisualReplacementInput
	DeliveryPlan       []RawDeliveryPlanEntry
	RetryBudgetDefault int
}

type SceneInput struct {
	Text            string
	SceneID         string
	Index           int64
	Kind            string
	DurationSeconds float64
	Clip            *ClipInput
	Stock           *ClipInput
	StockAssets     []ClipInput
	StockFallback   bool
	Voiceover       *VoiceoverInput
	Subtitles       *SubtitlesInput
}

type ClipInput struct {
	AssetID     string
	DriveFileID string
	URL         string
	SHA256      string
	StartMS     int64
	EndMS       int64
	DurationMS  int64
}

type VoiceoverInput struct {
	AssetID     string
	DriveFileID string
	URL         string
	SHA256      string
	SizeBytes   int64
	DurationMS  int64
	Language    string
}

type SubtitlesInput struct {
	AssetID  string
	Format   string
	URL      string
	SHA256   string
	Language string
}

type LayerInput struct {
	ID              string
	Type            string
	Role            string
	Text            string
	Asset           string
	Source          string
	Font            string
	FontSize        float64
	Position        []float64
	StartSeconds    float64
	DurationSeconds float64
	Preset          string
	Animation       string
}

// VisualReplacementInput is one already-composited video segment that
// replaces the base visual timeline over an absolute interval. It carries
// the replacement's asset identity (asset_id / url / sha256) and its
// timeline binding; no compositing semantics reach the renderer.
type VisualReplacementInput struct {
	ReplacementID   string
	AssetID         string
	URL             string
	SHA256          string
	TimelineStartUS int64
	TimelineEndUS   int64
	ProfileID       string
}

// BuildRawPayload constructs the complete raw canonical payload from the
// neutral input. Envelope construction and nested render mapping are kept in
// one pure projection boundary so HTTP handlers only adapt request DTOs.
func BuildRawPayload(input SubmissionInput) map[string]interface{} {
	payload := BuildRawPayloadEnvelope(RawPayloadInput{
		JobID:              input.JobID,
		JobType:            input.JobType,
		TemplateID:         input.TemplateID,
		TemplateVersion:    input.TemplateVersion,
		VideoMode:          input.VideoMode,
		VideoName:          input.VideoName,
		ScriptText:         input.ScriptText,
		AudioURL:           input.AudioURL,
		CopyOnly:           input.CopyOnly,
		RenderManifest:     input.RenderManifest,
		ManifestRef:        input.ManifestRef,
		ManifestSHA256:     input.ManifestSHA256,
		PlacementPin:       input.PlacementPin,
		LegacyVoiceovers:   input.LegacyVoiceovers,
		DeliveryPlan:       input.DeliveryPlan,
		RetryBudgetDefault: input.RetryBudgetDefault,
	})

	if len(input.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(input.Scenes))
		for _, sceneInput := range input.Scenes {
			scene := map[string]interface{}{
				"text":             sceneInput.Text,
				"duration_seconds": sceneInput.DurationSeconds,
			}
			if sceneInput.SceneID != "" {
				scene["scene_id"] = strings.TrimSpace(sceneInput.SceneID)
			}
			if sceneInput.Index > 0 {
				scene["index"] = sceneInput.Index
			}
			if sceneInput.Kind != "" {
				scene["kind"] = strings.TrimSpace(sceneInput.Kind)
			}
			if sceneInput.Clip != nil {
				scene["clip"] = clipToMap(sceneInput.Clip)
			}
			if sceneInput.Stock != nil {
				scene["stock"] = []interface{}{clipToMap(sceneInput.Stock)}
			}
			if len(sceneInput.StockAssets) > 0 {
				stock := make([]interface{}, 0, len(sceneInput.StockAssets))
				for i := range sceneInput.StockAssets {
					stock = append(stock, clipToMap(&sceneInput.StockAssets[i]))
				}
				scene["stock"] = stock
			}
			if sceneInput.StockFallback {
				scene["stock_fallback"] = true
			}
			if sceneInput.Voiceover != nil {
				scene["voiceover"] = voiceoverToMap(sceneInput.Voiceover)
			}
			if sceneInput.Subtitles != nil {
				scene["subtitles"] = subtitlesToMap(sceneInput.Subtitles)
			}
			scenes = append(scenes, scene)
		}
		payload["scenes"] = scenes
		clips := make([]interface{}, 0, len(input.Scenes))
		for _, sceneInput := range input.Scenes {
			if sceneInput.Clip == nil {
				continue
			}
			clip := clipToMap(sceneInput.Clip)
			url, _ := clip["url"].(string)
			if url == "" {
				if assetID, ok := clip["asset_id"].(string); ok && assetID != "" {
					if ref, err := assetref.NewDeferredDrive(assetID); err == nil {
						url = ref.Wire()
					}
				}
			}
			if url == "" {
				continue
			}
			clips = append(clips, map[string]interface{}{
				"url": url, "duration": sceneInput.DurationSeconds,
			})
		}
		if len(clips) > 0 {
			payload["clips"] = clips
		}
	}
	if len(input.Layers) > 0 {
		layers := make([]interface{}, 0, len(input.Layers))
		for _, layer := range input.Layers {
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
		payload["layers"] = layers
	}
	if len(input.VisualReplacements) > 0 {
		visualReplacements := make([]interface{}, 0, len(input.VisualReplacements))
		for _, r := range input.VisualReplacements {
			entry := map[string]interface{}{
				"replacement_id":    strings.TrimSpace(r.ReplacementID),
				"asset_id":          strings.TrimSpace(r.AssetID),
				"timeline_start_us": r.TimelineStartUS,
				"timeline_end_us":   r.TimelineEndUS,
				"profile_id":        strings.TrimSpace(r.ProfileID),
			}
			if url := strings.TrimSpace(r.URL); url != "" {
				entry["url"] = url
			}
			if r.SHA256 != "" {
				entry["sha256"] = r.SHA256
			}
			visualReplacements = append(visualReplacements, entry)
		}
		payload["visual_replacements"] = visualReplacements
	}
	return payload
}

func clipToMap(input *ClipInput) map[string]interface{} {
	if input == nil {
		return nil
	}
	out := map[string]interface{}{}
	if input.AssetID != "" {
		out["asset_id"] = input.AssetID
	}
	if input.DriveFileID != "" {
		out["drive_file_id"] = input.DriveFileID
	}
	if input.URL != "" {
		out["url"] = strings.TrimSpace(input.URL)
	}
	if input.SHA256 != "" {
		out["sha256"] = input.SHA256
	}
	if input.StartMS > 0 {
		out["start_ms"] = input.StartMS
	}
	if input.EndMS > 0 {
		out["end_ms"] = input.EndMS
	}
	if input.DurationMS > 0 {
		out["duration_ms"] = input.DurationMS
	}
	return out
}

func voiceoverToMap(input *VoiceoverInput) map[string]interface{} {
	if input == nil {
		return nil
	}
	out := map[string]interface{}{}
	if input.AssetID != "" {
		out["asset_id"] = input.AssetID
	}
	if input.DriveFileID != "" {
		out["drive_file_id"] = input.DriveFileID
	}
	if input.URL != "" {
		out["url"] = strings.TrimSpace(input.URL)
	}
	if input.SHA256 != "" {
		out["sha256"] = input.SHA256
	}
	if input.SizeBytes > 0 {
		out["size_bytes"] = input.SizeBytes
	}
	if input.DurationMS > 0 {
		out["duration_ms"] = input.DurationMS
	}
	if input.Language != "" {
		out["language"] = input.Language
	}
	return out
}

func subtitlesToMap(input *SubtitlesInput) map[string]interface{} {
	if input == nil {
		return nil
	}
	out := map[string]interface{}{}
	if input.AssetID != "" {
		out["asset_id"] = input.AssetID
	}
	if input.Format != "" {
		out["format"] = input.Format
	}
	if input.URL != "" {
		out["url"] = strings.TrimSpace(input.URL)
	}
	if input.SHA256 != "" {
		out["sha256"] = input.SHA256
	}
	if input.Language != "" {
		out["language"] = input.Language
	}
	return out
}
