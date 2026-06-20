// Package hybrid implements the hybrid.v1 pipeline compiler.
// It produces a RenderPlan from mixed sources: images + clips + color.
package hybrid

import (
	"context"
	"fmt"
	"strings"

	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
)

// Request is the validated input for the hybrid.v1 pipeline.
type Request struct {
	Items    []ItemInput
	AudioURL string
	Fit      string
}

// ItemInput is a single timeline item.
type ItemInput struct {
	Type     string // "image", "video", "color"
	URL      string
	ColorHex string
	Duration float64
	Fit      string
}

// Validate checks raw input parameters for the hybrid.v1 pipeline.
func Validate(input map[string]interface{}) error {
	items := input["items"]
	if items == nil {
		// Fallback: check for images + clips arrays
		images := toSliceString(input["images"])
		clips := toSliceString(input["clips"])
		if len(images) == 0 && len(clips) == 0 {
			return fmt.Errorf("hybrid.v1: items array or images/clips arrays are required")
		}
		return nil
	}
	itemList, ok := items.([]interface{})
	if !ok || len(itemList) == 0 {
		return fmt.Errorf("hybrid.v1: at least one item is required")
	}
	audioURL, _ := input["audio_url"].(string)
	if strings.TrimSpace(audioURL) == "" {
		return fmt.Errorf("hybrid.v1: audio_url is required")
	}
	return nil
}

// Compile produces a RenderPlan from the hybrid.v1 request.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}

	req := parseRequest(input)

	// Build timeline
	timeline_items := make([]plan.TimelineItem, len(req.Items))
	for i, item := range req.Items {
		source := plan.MediaSource{Type: item.Type}
		switch item.Type {
		case "image":
			source.URL = item.URL
		case "video":
			source.URL = item.URL
		case "color":
			source.ColorHex = item.ColorHex
		}

		transform := &plan.TransformSpec{ScaleMode: item.Fit}
		timeline_items[i] = plan.TimelineItem{
			Source:          source,
			DurationSeconds: item.Duration,
			Transform:       transform,
		}
	}

	// Audio tracks
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

	// Try explicit items array first
	if items, ok := input["items"].([]interface{}); ok {
		for _, item := range items {
			im, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			req.Items = append(req.Items, ItemInput{
				Type:     toStringDefault(im["type"], "image"),
				URL:      toString(im["url"]),
				ColorHex: toStringDefault(im["color_hex"], "#000000"),
				Duration: toFloat64Default(im["duration"], 4.0),
				Fit:      toStringDefault(im["fit"], req.Fit),
			})
		}
		return req
	}

	// Fallback: build from images + clips arrays
	images := toSliceString(input["images"])
	clips := toSliceString(input["clips"])

	for _, url := range images {
		req.Items = append(req.Items, ItemInput{
			Type:     "image",
			URL:      url,
			Duration: 5.0,
			Fit:      "cover",
		})
	}
	for _, url := range clips {
		req.Items = append(req.Items, ItemInput{
			Type:     "video",
			URL:      url,
			Duration: 4.0,
			Fit:      "contain",
		})
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
