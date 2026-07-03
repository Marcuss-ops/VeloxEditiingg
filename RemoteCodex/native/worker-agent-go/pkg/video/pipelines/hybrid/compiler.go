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
	Items       []ItemInput
	AudioURL    string
	AudioTracks []AudioTrackInput
	Fit         string
}

// ItemInput is a single timeline item.
type ItemInput struct {
	Type     string // "image", "video", "color"
	URL      string
	ColorHex string
	Duration float64
	Fit      string
}

// AudioTrackInput is a single audio source mixed into the render plan.
type AudioTrackInput struct {
	SourceURL       string
	Volume          float64
	StartTimeOffset float64
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
	audioTracks := make([]plan.AudioTrack, 0, len(req.AudioTracks))
	for _, track := range req.AudioTracks {
		if track.SourceURL == "" {
			continue
		}
		volume := track.Volume
		if volume <= 0 {
			volume = 1.0
		}
		audioTracks = append(audioTracks, plan.AudioTrack{
			SourceURL:       track.SourceURL,
			Volume:          volume,
			StartTimeOffset: track.StartTimeOffset,
		})
	}
	if len(audioTracks) == 0 && req.AudioURL != "" {
		audioTracks = append(audioTracks, plan.AudioTrack{
			SourceURL: req.AudioURL,
			Volume:    1.0,
		})
	}

	return &plan.RenderPlan{
		Version:     1,
		JobID:       jobID,
		Canvas:      plan.DefaultCanvas(),
		Timeline:    timeline_items,
		AudioTracks: audioTracks,
		OutputPath:  outputPath,
	}, nil
}

func parseRequest(input map[string]interface{}) *Request {
	req := &Request{
		AudioURL: toString(input["audio_url"]),
		Fit:      toStringDefault(input["fit"], "contain"),
	}

	if rawTracks, ok := input["audio_tracks"].([]interface{}); ok {
		for _, item := range rawTracks {
			trackMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			req.AudioTracks = append(req.AudioTracks, AudioTrackInput{
				SourceURL:       toStringDefault(trackMap["source_url"], toString(trackMap["url"])),
				Volume:          toFloat64Default(trackMap["volume"], 1.0),
				StartTimeOffset: toFloat64Default(trackMap["start_time_offset"], 0.0),
			})
		}
	}

	// Canonical-purity contract (Step 2/8): when items[] carries a
	// (role, scene) reference, resolve the URL and (when missing) the
	// duration from scene-level metadata rather than reconstructing
	// from clips[]/stock_clip_paths. scenes[] MAY be absent (legacy
	// callers pre-resolve URLs in items[]); in that case items[i].url
	// and items[i].duration are honored verbatim.
	var scenes []map[string]interface{}
	if rawScenes, ok := input["scenes"].([]interface{}); ok {
		for _, s := range rawScenes {
			if sm, ok := s.(map[string]interface{}); ok {
				scenes = append(scenes, sm)
			}
		}
	}

	// Try explicit items array first
	if items, ok := input["items"].([]interface{}); ok {
		for _, item := range items {
			im, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			itemType := toStringDefault(im["type"], "image")
			itemURL := toString(im["url"])
			itemDuration := toFloat64Default(im["duration"], 4.0)
			itemFit := toStringDefault(im["fit"], req.Fit)
			itemHasDuration := toFloat64Default(im["duration"], 0.0) > 0

			// Role-based URL + (optional) duration routing.
			if role := toString(im["role"]); role != "" {
				sceneIdx := -1
				switch v := im["scene"].(type) {
				case int:
					sceneIdx = v
				case int64:
					sceneIdx = int(v)
				case float64:
					sceneIdx = int(v)
				}
				if sceneIdx >= 0 && sceneIdx < len(scenes) {
					scene := scenes[sceneIdx]
					switch role {
					case "voiceover_bed":
						if s := toString(scene["stock_link"]); s != "" {
							itemURL = s
						}
						if !itemHasDuration {
							if d := toFloat64Default(scene["voiceover_duration_seconds"], 0.0); d > 0 {
								itemDuration = d
							}
						}
					case "scene_clip":
						if s := toString(scene["clip_link"]); s != "" {
							itemURL = s
						}
						if !itemHasDuration {
							if d := toFloat64Default(scene["final_clip_duration_seconds"], 0.0); d > 0 {
								itemDuration = d
							}
						}
					}
				}
			}

			req.Items = append(req.Items, ItemInput{
				Type:     itemType,
				URL:      itemURL,
				ColorHex: toStringDefault(im["color_hex"], "#000000"),
				Duration: itemDuration,
				Fit:      itemFit,
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
