// Package clips implements the clips.v1 pipeline compiler.
// It produces a RenderPlan from a list of video clip URLs + audio.
package clips

import (
	"context"
	"fmt"
	"strings"

	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
)

// Request is the validated input for the clips.v1 pipeline.
type Request struct {
	Clips    []ClipInput
	AudioURL string
	Fit      string // "contain", "cover", "stretch"
}

// ClipInput is a single clip with URL and duration.
type ClipInput struct {
	URL      string
	Duration float64
}

// Validate checks raw input parameters for the clips.v1 pipeline.
func Validate(input map[string]interface{}) error {
	clips := input["clips"]
	if clips == nil {
		return fmt.Errorf("clips.v1: clips array is required")
	}
	clipList, ok := clips.([]interface{})
	if !ok || len(clipList) == 0 {
		return fmt.Errorf("clips.v1: at least one clip is required")
	}
	for i, c := range clipList {
		cm, ok := c.(map[string]interface{})
		if !ok {
			return fmt.Errorf("clips.v1: clips[%d] must be an object", i)
		}
		url, _ := cm["url"].(string)
		if strings.TrimSpace(url) == "" {
			return fmt.Errorf("clips.v1: clips[%d].url is required", i)
		}
	}
	audioURL, _ := input["audio_url"].(string)
	if strings.TrimSpace(audioURL) == "" {
		return fmt.Errorf("clips.v1: audio_url is required")
	}
	return nil
}

// Compile produces a RenderPlan from the clips.v1 request.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}

	req := parseRequest(input)

	// Build timeline
	timeline_items := make([]plan.TimelineItem, len(req.Clips))
	for i, clip := range req.Clips {
		transform := &plan.TransformSpec{ScaleMode: req.Fit}
		timeline_items[i] = plan.TimelineItem{
			Source:          plan.MediaSource{Type: "video", URL: clip.URL},
			DurationSeconds: clip.Duration,
			Transform:       transform,
		}
	}

	// Audio track
	var audioTracks []plan.AudioTrack
	if req.AudioURL != "" {
		audioTracks = append(audioTracks, plan.AudioTrack{
			SourceURL: req.AudioURL,
			Volume:    1.0,
		})
	}

	return &plan.RenderPlan{
		Version:    1,
		JobID:      jobID,
		Canvas:     plan.DefaultCanvas(),
		Timeline:   timeline_items,
		AudioTracks: audioTracks,
		OutputPath: outputPath,
	}, nil
}

func parseRequest(input map[string]interface{}) *Request {
	req := &Request{
		AudioURL: toString(input["audio_url"]),
		Fit:      toStringDefault(input["fit"], "contain"),
	}

	if clips, ok := input["clips"].([]interface{}); ok {
		for _, c := range clips {
			cm, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			clip := ClipInput{
				URL:      toString(cm["url"]),
				Duration: toFloat64Default(cm["duration"], 4.0),
			}
			if clip.Duration <= 0 {
				clip.Duration = 4.0
			}
			req.Clips = append(req.Clips, clip)
		}
	}

	return req
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func toStringDefault(v interface{}, fallback string) string {
	s := toString(v)
	if s == "" {
		return fallback
	}
	return s
}

func toFloat64Default(v interface{}, fallback float64) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	}
	return fallback
}
