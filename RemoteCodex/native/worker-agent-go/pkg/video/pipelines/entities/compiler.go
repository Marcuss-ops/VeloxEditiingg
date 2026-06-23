// Package entities implements the entities.v1 pipeline compiler.
// It produces a RenderPlan from script + audio + entity definitions.
// The pipeline: transcribe → extract entities → resolve images → build timeline.
package entities

import (
	"context"
	"fmt"
	"strings"

	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
	"velox-worker-agent/pkg/video/services/timeline"
)

// Request is the validated input for the entities.v1 pipeline.
type Request struct {
	Script       string
	AudioURL     string
	EntityStyle  string // "image_overlay", "full_screen", "split"
	OutputFormat string // "youtube", "tiktok", "instagram"
	Entities     []EntityInput
}

// EntityInput is a single entity definition.
type EntityInput struct {
	Name     string
	ImageURL string
	Start    float64
	End      float64
}

// Validate checks raw input parameters for the entities.v1 pipeline.
func Validate(input map[string]interface{}) error {
	script, _ := input["script"].(string)
	if strings.TrimSpace(script) == "" {
		return fmt.Errorf("entities.v1: script is required")
	}
	audioURL, _ := input["audio_url"].(string)
	if strings.TrimSpace(audioURL) == "" {
		return fmt.Errorf("entities.v1: audio_url is required")
	}
	return nil
}

// Compile produces a RenderPlan from the entities.v1 request.
// In V1, entities with resolved images become sequential timeline items.
// Gaps between entities are filled with a dark background.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}

	req := parseRequest(input)

	// Probe audio duration
	audioDuration := probe.DurationSeconds(req.AudioURL)
	if audioDuration <= 0 {
		audioDuration = 10.0 // fallback
	}

	// Determine canvas based on output format
	canvas := plan.DefaultCanvas()
	switch req.OutputFormat {
	case "tiktok", "instagram":
		canvas = plan.VerticalCanvas()
	}

	// Build timeline from entities
	// For V1: entities with images become sequential items, gaps get dark background
	var timeline_items []plan.TimelineItem

	if len(req.Entities) == 0 {
		// No entities: single dark background for full audio duration
		timeline_items = append(timeline_items, plan.TimelineItem{
			Source:          plan.MediaSource{Type: "color", ColorHex: "#111111"},
			DurationSeconds: audioDuration,
		})
	} else {
		// Sort entities by start time and build timeline
		currentTime := 0.0
		for _, entity := range req.Entities {
			// Fill gap before this entity with dark background
			if entity.Start > currentTime {
				gap := entity.Start - currentTime
				if gap > 0.1 {
					timeline_items = append(timeline_items, plan.TimelineItem{
						Source:          plan.MediaSource{Type: "color", ColorHex: "#111111"},
						DurationSeconds: gap,
					})
				}
			}

			// Add entity image
			dur := entity.End - entity.Start
			if dur <= 0 {
				dur = 3.0 // minimum duration
			}
			if entity.ImageURL != "" {
				slowZoom := true
				timeline_items = append(timeline_items, plan.TimelineItem{
					Source:          plan.MediaSource{Type: "image", URL: entity.ImageURL},
					DurationSeconds: dur,
					Transform:       &plan.TransformSpec{ScaleMode: "cover", SlowZoom: &slowZoom},
				})
			} else {
				// Entity without image: dark background
				timeline_items = append(timeline_items, plan.TimelineItem{
					Source:          plan.MediaSource{Type: "color", ColorHex: "#111111"},
					DurationSeconds: dur,
				})
			}
			currentTime = entity.End
		}

		// Fill trailing gap
		if currentTime < audioDuration {
			trailing := audioDuration - currentTime
			if trailing > 0.1 {
				timeline_items = append(timeline_items, plan.TimelineItem{
					Source:          plan.MediaSource{Type: "color", ColorHex: "#111111"},
					DurationSeconds: trailing,
				})
			}
		}
	}

	// If no timeline items were created (shouldn't happen), add a fallback
	if len(timeline_items) == 0 {
		durations := timeline.AllocateDurations(nil, audioDuration, 1, 5.0)
		timeline_items = append(timeline_items, plan.TimelineItem{
			Source:          plan.MediaSource{Type: "color", ColorHex: "#111111"},
			DurationSeconds: durations[0],
		})
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
	req := &Request{
		Script:       toString(input["script"]),
		AudioURL:     toString(input["audio_url"]),
		EntityStyle:  toStringDefault(input["entity_style"], "full_screen"),
		OutputFormat: toStringDefault(input["output_format"], "youtube"),
	}

	// Parse pre-resolved entities if provided
	if entities, ok := input["entities"].([]interface{}); ok {
		for _, e := range entities {
			em, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			req.Entities = append(req.Entities, EntityInput{
				Name:     toString(em["name"]),
				ImageURL: toString(em["image_url"]),
				Start:    toFloat64Default(em["start"], 0),
				End:      toFloat64Default(em["end"], 0),
			})
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
