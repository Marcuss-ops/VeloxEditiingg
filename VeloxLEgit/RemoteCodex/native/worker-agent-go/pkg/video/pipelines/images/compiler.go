// Package images implements the images.v1 pipeline compiler.
// It produces a RenderPlan from a list of image URLs + audio.
package images

import (
	"context"
	"fmt"
	"strings"

	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
	"velox-worker-agent/pkg/video/services/timeline"
)

// Request is the validated input for the images.v1 pipeline.
type Request struct {
	Images      []string
	AudioURL    string
	Effect      string // "slow_zoom" or "static"
	Orientation string // "horizontal" or "vertical"
}

// Validator checks raw input parameters for the images.v1 pipeline.
func Validate(input map[string]interface{}) error {
	images := toSliceString(input["images"])
	if len(images) == 0 {
		return fmt.Errorf("images.v1: at least one image URL is required")
	}
	for i, url := range images {
		if strings.TrimSpace(url) == "" {
			return fmt.Errorf("images.v1: images[%d] URL is empty", i)
		}
	}
	audioURL, _ := input["audio_url"].(string)
	if strings.TrimSpace(audioURL) == "" {
		return fmt.Errorf("images.v1: audio_url is required")
	}
	return nil
}

// Compile produces a RenderPlan from the images.v1 request.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}

	req := parseRequest(input)

	// Determine canvas
	canvas := plan.DefaultCanvas()
	if req.Orientation == "vertical" {
		canvas = plan.VerticalCanvas()
	}

	// Probe audio duration
	audioDuration := probe.DurationSeconds(req.AudioURL)
	if audioDuration <= 0 {
		audioDuration = float64(len(req.Images)) * 5.0 // fallback: 5s per image
	}

	// Allocate durations
	explicitDurations := make([]float64, len(req.Images))
	durations := timeline.AllocateDurations(explicitDurations, audioDuration, len(req.Images), 5.0)

	// Build timeline
	timeline_items := make([]plan.TimelineItem, len(req.Images))
	for i, imageURL := range req.Images {
		transform := &plan.TransformSpec{ScaleMode: "cover"}
		if req.Effect != "static" {
			slowZoom := true
			transform.SlowZoom = &slowZoom
		}
		timeline_items[i] = plan.TimelineItem{
			Source:          plan.MediaSource{Type: "image", URL: imageURL},
			DurationSeconds: durations[i],
			Transform:       transform,
		}
	}

	return &plan.RenderPlan{
		Version:  1,
		JobID:    jobID,
		Canvas:   canvas,
		Timeline: timeline_items,
		AudioTracks: []plan.AudioTrack{
			{SourceURL: req.AudioURL, Volume: 1.0},
		},
		OutputPath: outputPath,
	}, nil
}

func parseRequest(input map[string]interface{}) *Request {
	return &Request{
		Images:      toSliceString(input["images"]),
		AudioURL:    toString(input["audio_url"]),
		Effect:      toStringDefault(input["effect"], "slow_zoom"),
		Orientation: toStringDefault(input["orientation"], "horizontal"),
	}
}

func toSliceString(v interface{}) []string {
	switch val := v.(type) {
	case []interface{}:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, strings.TrimSpace(s))
			}
		}
		return result
	case []string:
		return val
	}
	return nil
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
