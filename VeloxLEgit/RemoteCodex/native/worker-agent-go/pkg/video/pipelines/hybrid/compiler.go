// Package hybrid implements the hybrid.v1 pipeline compiler.
// It produces a RenderPlan from mixed sources: images + clips + color.
package hybrid

import (
	"context"
	"fmt"
	"strings"

	"velox-worker-agent/internal/oteltrace"
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
//
// For role-aware compilation (see compileItemsToTimeline), an item
// may declare its semantic role via the Role field:
//
//   - "voiceover_bed": the item is the stock clip that visually
//     carries the voiceover; DurationSeconds is taken from
//     VoiceoverDurationSeconds (the detected voiceover length).
//   - "scene_clip": the item is the final user-visible clip for that
//     scene; DurationSeconds is taken from FinalClipDurationSeconds.
//   - "" (empty) or any other value: the legacy path is used and
//     DurationSeconds comes from the generic Duration field.
//
// NOTE on naming: in the worker-side contract, "Item" in
// compileItemsToTimeline's signature corresponds to this struct, and
// "TimelineSegment" corresponds to plan.TimelineItem. We keep the
// canonical names here (ItemInput / plan.TimelineItem) for Go
// idiomaticity and because plan.TimelineItem is the V1 wire contract
// shared with the C++ engine.
type ItemInput struct {
	Type                     string // "image", "video", "color"
	URL                      string
	ColorHex                 string
	Duration                 float64
	Fit                      string
	Role                     string // "voiceover_bed", "scene_clip", or "" (legacy)
	VoiceoverDurationSeconds float64
	FinalClipDurationSeconds float64
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
//
// Scorecard v2 / Step 15: starts a "compile" span for distributed tracing.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	ctx, span := oteltrace.StartSpan(ctx, "compile", oteltrace.AttrJobID(jobID))
	defer span.End()

	if err := Validate(input); err != nil {
		return nil, err
	}

	req := parseRequest(input)

	// Build timeline using the role-aware compileItemsToTimeline
	// helper. When req.Items contains items with role=voiceover_bed or
	// role=scene_clip, the helper selects DurationSeconds from the
	// role-specific field (VoiceoverDurationSeconds /
	// FinalClipDurationSeconds). Items without a role fall through
	// to the legacy Duration field. The request-level Fit is passed
	// as the default for items that do not declare their own.
	timeline_items := compileItemsToTimeline(req.Items, req.Fit)

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

// compileItemsToTimeline maps a list of role-aware items to the
// canonical RenderPlan.Timeline. When an item carries a non-empty
// Role, the function selects DurationSeconds based on the role:
//
//   - "voiceover_bed" → VoiceoverDurationSeconds (the stock clip
//     plays for as long as the voiceover lasts).
//   - "scene_clip"    → FinalClipDurationSeconds (the final clip
//     plays for its own intrinsic duration).
//
// For items without a role, the legacy Duration field is used
// (falling back to 4.0 if both Duration and the role-specific field
// are non-positive). The function preserves the order of input
// items and forces MediaSource.Type to "video" for both role-aware
// kinds since they are by definition video segments.
//
// The defaultFit argument is the request-level fallback for items
// that do not declare their own Fit; it preserves the legacy
// behavior where an item without an explicit fit inherited the
// request-level fit (req.Fit, which itself defaults to "contain").
//
// NOTE on naming: the user-facing signature is
// `compileItemsToTimeline(items []Item) []TimelineSegment`; in this
// package, "Item" is ItemInput and "TimelineSegment" is
// plan.TimelineItem (the V1 wire contract shared with the C++ engine).
func compileItemsToTimeline(items []ItemInput, defaultFit string) []plan.TimelineItem {
	timeline := make([]plan.TimelineItem, len(items))
	for i, item := range items {
		timeline[i] = plan.TimelineItem{
			Source:          sourceForItem(item),
			DurationSeconds: effectiveDuration(item),
			Transform:       &plan.TransformSpec{ScaleMode: effectiveFit(item, defaultFit)},
		}
	}
	return timeline
}

// sourceForItem builds the MediaSource for a single item. Role-aware
// items (voiceover_bed / scene_clip) are always video; legacy items
// follow the original Type-driven switch.
func sourceForItem(item ItemInput) plan.MediaSource {
	if item.Role == "voiceover_bed" || item.Role == "scene_clip" {
		return plan.MediaSource{Type: "video", URL: item.URL}
	}
	src := plan.MediaSource{Type: item.Type}
	switch item.Type {
	case "image", "video":
		src.URL = item.URL
	case "color":
		src.ColorHex = item.ColorHex
	}
	if src.Type == "" {
		src.Type = "image"
	}
	return src
}

// effectiveDuration picks the duration field per role contract.
// Role-specific fields take precedence; legacy Duration is the
// fallback; the package-wide 4.0 default is the last resort.
func effectiveDuration(item ItemInput) float64 {
	switch item.Role {
	case "voiceover_bed":
		if item.VoiceoverDurationSeconds > 0 {
			return item.VoiceoverDurationSeconds
		}
	case "scene_clip":
		if item.FinalClipDurationSeconds > 0 {
			return item.FinalClipDurationSeconds
		}
	}
	if item.Duration > 0 {
		return item.Duration
	}
	return 4.0
}

// effectiveFit returns the item's Fit if set, else the
// request-level defaultFit (which itself defaults to "contain" via
// parseRequest). This preserves the legacy behavior where items
// without an explicit fit inherited the request-level fit.
func effectiveFit(item ItemInput, defaultFit string) string {
	if item.Fit != "" {
		return item.Fit
	}
	if defaultFit != "" {
		return defaultFit
	}
	return "contain"
}

func parseRequest(input map[string]interface{}) *Request {
	// audio_url is the canonical field; voiceover_url is accepted as
	// an alias for parity with payloads emitted by enqueue_clips.go
	// (which uses "voiceover_url" for the shared voiceover track).
	// audio_url wins when both are set.
	req := &Request{
		AudioURL: toStringDefault(input["audio_url"], toString(input["voiceover_url"])),
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

	// Try explicit items array first. When present, this is the
	// CANONICAL timeline; the `clips` / `images` fallback below is
	// only used when items is absent (legacy compatibility index).
	//
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
				Type:                     itemType,
				URL:                      itemURL,
				ColorHex:                 toStringDefault(im["color_hex"], "#000000"),
				Duration:                 itemDuration,
				Fit:                      itemFit,
				Role:                     toString(im["role"]),
				VoiceoverDurationSeconds: toFloat64Default(im["voiceover_duration_seconds"], 0.0),
				FinalClipDurationSeconds: toFloat64Default(im["final_clip_duration_seconds"], 0.0),
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
