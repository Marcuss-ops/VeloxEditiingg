package projection

import "strings"

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
	Spec               map[string]interface{}
	Output             *OutputInput
	RenderManifest     map[string]interface{}
	ManifestRef        map[string]interface{}
	ManifestSHA256     string
	PlacementPin       string
	LegacyVoiceovers   []string
	Scenes             []SceneInput
	Layers             []LayerInput
	AudioTracks        []AudioTrackInput
	DeliveryPlan       []RawDeliveryPlanEntry
	RetryBudgetDefault int
}

type OutputInput struct {
	Width  int
	Height int
	FPS    int
	Format string
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

type AudioTrackInput struct {
	AssetID         string
	SourceURL       string
	Role            string
	Volume          float64
	StartTimeOffset float64
	DurationSeconds float64
	Loop            bool
	FadeInSeconds   float64
	FadeOutSeconds  float64
	DuckingEnabled  bool
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
		Spec:               input.Spec,
		Output:             outputToMap(input.Output),
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
	if len(input.AudioTracks) > 0 {
		audioTracks := make([]interface{}, 0, len(input.AudioTracks))
		for _, track := range input.AudioTracks {
			audioTracks = append(audioTracks, audioTrackToMap(track))
		}
		payload["audio_tracks"] = audioTracks
	}
	return payload
}

func outputToMap(output *OutputInput) map[string]interface{} {
	if output == nil {
		return nil
	}
	return map[string]interface{}{"width": output.Width, "height": output.Height, "fps": output.FPS, "format": output.Format}
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

func audioTrackToMap(input AudioTrackInput) map[string]interface{} {
	out := map[string]interface{}{}
	if trimmed := strings.TrimSpace(input.SourceURL); trimmed != "" {
		out["source_url"] = trimmed
	}
	if input.AssetID != "" {
		out["asset_id"] = input.AssetID
	}
	if input.Role != "" {
		out["role"] = input.Role
	}
	if input.Volume > 0 {
		out["volume"] = input.Volume
	}
	if input.StartTimeOffset > 0 {
		out["start_time_offset"] = input.StartTimeOffset
	}
	if input.DurationSeconds > 0 {
		out["duration_seconds"] = input.DurationSeconds
	}
	if input.Loop {
		out["loop"] = true
	}
	if input.FadeInSeconds > 0 {
		out["fade_in_seconds"] = input.FadeInSeconds
	}
	if input.FadeOutSeconds > 0 {
		out["fade_out_seconds"] = input.FadeOutSeconds
	}
	if input.DuckingEnabled {
		out["ducking_enabled"] = true
	}
	return out
}
