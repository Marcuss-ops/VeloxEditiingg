// Package plan defines the RenderPlan V1 contract shared between Go and C++.
// This is the canonical output format that all pipeline compilers produce
// and that the C++ engine consumes via --render --plan.
package plan

// RenderPlan is the V1 contract for video rendering.
// All pipeline compilers produce this; the C++ engine consumes it.
type RenderPlan struct {
	Version     int             `json:"version"`
	JobID       string          `json:"job_id"`
	Canvas      CanvasSpec      `json:"canvas"`
	Timeline    []TimelineItem  `json:"timeline"`
	AudioTracks []AudioTrack    `json:"audio_tracks"`
	Layers      []Layer         `json:"layers,omitempty"`
	Subtitles   []SubtitleTrack `json:"subtitle_tracks,omitempty"`
	OutputPath  string          `json:"output_path"`
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
	SceneID         string         `json:"scene_id,omitempty"`
	DurationSeconds float64        `json:"duration_seconds"`
	IncludeAudio    bool           `json:"include_audio,omitempty"`
	Transform       *TransformSpec `json:"transform,omitempty"`
}

// AudioTrack defines an audio source to mix into the final video.
type AudioTrack struct {
	SourceURL       string  `json:"source_url"`
	Volume          float64 `json:"volume,omitempty"`
	StartTimeOffset float64 `json:"start_time_offset,omitempty"`
	// DurationSeconds bounds this track to its scene. This is important for
	// narrated clip timelines: a source clip can contain more audio than the
	// visible scene and must not bleed into the next scene.
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	Role            string  `json:"role,omitempty"`

	// Loop enables seamless looping when the audio source is shorter
	// than the total video duration. The renderer uses -stream_loop -1
	// (or equivalent) and trims to the output length.
	Loop bool `json:"loop,omitempty"`

	// FadeInSeconds applies a linear fade-in from silence over the
	// given duration at the start of the track. 0 means no fade.
	FadeInSeconds float64 `json:"fade_in_seconds,omitempty"`

	// FadeOutSeconds applies a linear fade-out to silence over the
	// given duration at the end of the track. 0 means no fade.
	FadeOutSeconds float64 `json:"fade_out_seconds,omitempty"`

	// DuckingEnabled instructs the renderer to apply sidechain
	// compression (or equivalent volume automation) that lowers this
	// track's volume when voiceover is active. Typical for
	// background_music role.
	DuckingEnabled bool `json:"ducking_enabled,omitempty"`
}

// Layer is an independent compositing layer. Media remains in Timeline;
// overlays (text, names, important phrases and extra images) are separate
// so a video job is not forced into an image-only or text-only mode.
type Layer struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`           // text, image, video, color
	Role            string    `json:"role,omitempty"` // title, name, important_phrase, overlay
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty"`
	Position        []float64 `json:"position,omitempty"`
	StartSeconds    float64   `json:"start_seconds,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// SubtitleTrack is kept separate from visual layers so subtitle APIs can
// evolve independently from media and editorial overlays.
type SubtitleTrack struct {
	Source string          `json:"source"`
	Preset string          `json:"preset,omitempty"`
	Font   string          `json:"font,omitempty"`
	Events []SubtitleEvent `json:"events,omitempty"`
}

// SubtitleEvent is a deterministic, already-aligned caption interval.
// Subtitle alignment consumes the final voiceover timing and never infers
// timing from text length on the worker.
type SubtitleEvent struct {
	Text         string  `json:"text"`
	StartSeconds float64 `json:"start_seconds"`
	EndSeconds   float64 `json:"end_seconds"`
}

// DefaultCanvas returns a standard 1080p canvas.
func DefaultCanvas() CanvasSpec {
	return CanvasSpec{Width: 1920, Height: 1080, Fps: 30}
}

// VerticalCanvas returns a 1080x1920 vertical canvas.
func VerticalCanvas() CanvasSpec {
	return CanvasSpec{Width: 1080, Height: 1920, Fps: 30}
}
