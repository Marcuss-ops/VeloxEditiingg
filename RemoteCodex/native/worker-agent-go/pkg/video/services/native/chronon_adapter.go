package native

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"velox-worker-agent/pkg/video/plan"
)

type chrononPlan struct {
	Schema      string            `json:"schema"`
	Version     int               `json:"version"`
	JobID       string            `json:"job_id"`
	Canvas      map[string]int    `json:"canvas"`
	Layers      []map[string]any  `json:"layers"`
	AudioTracks []map[string]any  `json:"audio_tracks,omitempty"`
	Output      map[string]string `json:"output"`
}

func chrononBackendEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("VELOX_RENDER_BACKEND")), "chronon")
}

func chrononPlanJSON(p *plan.RenderPlan) ([]byte, error) {
	if p == nil || p.Canvas.Fps <= 0 {
		return nil, fmt.Errorf("invalid Velox render plan for Chronon")
	}
	result := chrononPlan{
		Schema:  "chronon.render-plan",
		Version: 1,
		JobID:   p.JobID,
		Canvas: map[string]int{
			"width": p.Canvas.Width, "height": p.Canvas.Height, "fps": p.Canvas.Fps,
		},
		Layers:      make([]map[string]any, 0, len(p.Timeline)),
		AudioTracks: make([]map[string]any, 0, len(p.AudioTracks)),
		Output:      map[string]string{"path": p.OutputPath, "format": "mp4"},
	}
	for _, track := range p.AudioTracks {
		if strings.TrimSpace(track.SourceURL) == "" {
			continue
		}
		entry := map[string]any{
			"source": track.SourceURL, "volume": track.Volume,
			"start_time_offset": track.StartTimeOffset, "duration_seconds": track.DurationSeconds,
			"role": track.Role,
		}
		// Role-aware rendering hints for the C++ engine.
		if track.Loop {
			entry["loop"] = true
		}
		if track.FadeInSeconds > 0 {
			entry["fade_in_seconds"] = track.FadeInSeconds
		}
		if track.FadeOutSeconds > 0 {
			entry["fade_out_seconds"] = track.FadeOutSeconds
		}
		if track.DuckingEnabled {
			entry["ducking_enabled"] = true
		}
		result.AudioTracks = append(result.AudioTracks, entry)
	}
	frame := 0
	for index, item := range p.Timeline {
		frames := int(math.Round(item.DurationSeconds * float64(p.Canvas.Fps)))
		if frames < 1 {
			frames = 1
		}
		layer := map[string]any{
			"id":              fmt.Sprintf("timeline_%d", index),
			"start_frame":     frame,
			"duration_frames": frames,
		}
		switch item.Source.Type {
		case "image":
			layer["type"] = "image"
			layer["asset"] = item.Source.URL
			if item.Transform != nil && item.Transform.ScaleMode != "" {
				layer["fit"] = item.Transform.ScaleMode
			}
		case "video":
			layer["type"] = "video"
			layer["source"] = item.Source.URL
		case "color":
			layer["type"] = "color"
			layer["color"] = hexColor(item.Source.ColorHex)
		default:
			return nil, fmt.Errorf("unsupported Velox source type %q for Chronon", item.Source.Type)
		}
		result.Layers = append(result.Layers, layer)
		frame += frames
	}
	for _, overlay := range p.Layers {
		layer := map[string]any{
			"id": overlay.ID, "type": overlay.Type,
			"start_frame": int(math.Round(overlay.StartSeconds * float64(p.Canvas.Fps))),
		}
		if overlay.DurationSeconds > 0 {
			layer["duration_frames"] = int(math.Round(overlay.DurationSeconds * float64(p.Canvas.Fps)))
		}
		if overlay.Role != "" {
			layer["role"] = overlay.Role
		}
		if overlay.Text != "" {
			layer["text"] = overlay.Text
		}
		if overlay.Asset != "" {
			layer["asset"] = overlay.Asset
		}
		if overlay.Source != "" {
			layer["source"] = overlay.Source
		}
		if overlay.Font != "" {
			layer["font"] = overlay.Font
		}
		if overlay.FontSize > 0 {
			layer["font_size"] = overlay.FontSize
		}
		if len(overlay.Position) > 0 {
			layer["position"] = overlay.Position
		}
		if overlay.Preset != "" {
			layer["preset"] = overlay.Preset
		}
		if overlay.Animation != "" {
			layer["animation"] = map[string]any{"preset": overlay.Animation}
		}
		result.Layers = append(result.Layers, layer)
	}
	for _, subtitle := range p.Subtitles {
		if strings.TrimSpace(subtitle.Source) == "" {
			continue
		}
		layer := map[string]any{"id": fmt.Sprintf("subtitles_%d", len(result.Layers)), "type": "subtitle_track", "source": subtitle.Source}
		if subtitle.Preset != "" {
			layer["preset"] = subtitle.Preset
		}
		if subtitle.Font != "" {
			layer["font"] = subtitle.Font
		}
		result.Layers = append(result.Layers, layer)
	}
	result.Canvas["duration_frames"] = frame
	return json.MarshalIndent(result, "", "  ")
}

func hexColor(raw string) []float64 {
	value := strings.TrimPrefix(strings.TrimSpace(raw), "#")
	if len(value) != 6 {
		return []float64{0, 0, 0, 1}
	}
	channel := func(offset int) float64 {
		parsed, err := strconv.ParseUint(value[offset:offset+2], 16, 8)
		if err != nil {
			return 0
		}
		return float64(parsed) / 255
	}
	return []float64{channel(0), channel(2), channel(4), 1}
}
