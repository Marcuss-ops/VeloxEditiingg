// Package plan defines the RenderPlan V1 contract shared between Go and C++.
// This is the canonical output format that all pipeline compilers produce
// and that the C++ engine consumes via --render --plan.
package plan

// RenderPlan is the V1 contract for video rendering.
// All pipeline compilers produce this; the C++ engine consumes it.
type RenderPlan struct {
	Version     int            `json:"version"`
	JobID       string         `json:"job_id"`
	Canvas      CanvasSpec     `json:"canvas"`
	Timeline    []TimelineItem `json:"timeline"`
	AudioTracks []AudioTrack   `json:"audio_tracks"`
	OutputPath  string         `json:"output_path"`
}

// CanvasSpec defines the output video dimensions and frame rate.
type CanvasSpec struct {
	Width  int `json:"width"`
	Height int `json:"height"`
	Fps    int `json:"fps"`
}

// MediaSource is the union type for timeline source media.
type MediaSource struct {
	Type     string `json:"type"`
	URL      string `json:"url,omitempty"`
	CacheKey string `json:"cache_key,omitempty"`
	ColorHex string `json:"color_hex,omitempty"`
}

// TransformSpec defines how a timeline item is rendered.
type TransformSpec struct {
	ScaleMode string `json:"scale_mode,omitempty"`
	SlowZoom  *bool  `json:"slow_zoom,omitempty"`
}

// TimelineItem is a single segment in the rendering timeline.
type TimelineItem struct {
	Source          MediaSource    `json:"source"`
	DurationSeconds float64        `json:"duration_seconds"`
	Transform       *TransformSpec `json:"transform,omitempty"`
}

// AudioTrack defines an audio source to mix into the final video.
type AudioTrack struct {
	SourceURL       string  `json:"source_url"`
	Volume          float64 `json:"volume,omitempty"`
	StartTimeOffset float64 `json:"start_time_offset,omitempty"`
}

// DefaultCanvas returns a standard 1080p canvas.
func DefaultCanvas() CanvasSpec {
	return CanvasSpec{Width: 1920, Height: 1080, Fps: 30}
}

// VerticalCanvas returns a 1080x1920 vertical canvas.
func VerticalCanvas() CanvasSpec {
	return CanvasSpec{Width: 1080, Height: 1920, Fps: 30}
}
