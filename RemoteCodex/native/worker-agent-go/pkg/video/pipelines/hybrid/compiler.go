// Package hybrid implements the hybrid.v1 pipeline compiler.
// It produces a RenderPlan from mixed sources: images + clips + color.
//
// File split:
//   - compiler.go          : Request/ItemInput/AudioTrackInput types,
//     Validate, Compile (the orchestrator).
//   - compiler_parse.go    : parseRequest + parseLayers +
//     parseSceneSubtitleTracks + the toString*/toFloat64Default/
//     toBoolDefault/toSliceString coercion helpers.
//   - compiler_timeline.go : compileItemsToTimeline + sourceForItem +
//     effectiveDuration + effectiveFit (role-aware timeline building).
package hybrid

import (
	"context"
	"fmt"

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
	Layers      []plan.Layer
	Subtitles   []plan.SubtitleTrack
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
	SceneID                  string
	ColorHex                 string
	Duration                 float64
	Fit                      string
	Role                     string // "voiceover_bed", "scene_clip", or "" (legacy)
	VoiceoverDurationSeconds float64
	FinalClipDurationSeconds float64
	IncludeAudio             bool
}

// AudioTrackInput is a single audio source mixed into the render plan.
type AudioTrackInput struct {
	SourceURL       string
	Volume          float64
	StartTimeOffset float64
	DurationSeconds float64
	Role            string
	Loop            bool
	FadeInSeconds   float64
	FadeOutSeconds  float64
	DuckingEnabled  bool

	// hasExplicitBGMConfig is true when the user explicitly provided
	// at least one of the loop/fade/ducking fields in the payload.
	// When false for a background_music track, the compiler applies
	// sensible defaults (loop=true, fade=0.5s, ducking=true). When
	// true, the compiler trusts the user's explicit values.
	hasExplicitBGMConfig bool
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

	// Audio tracks — role-aware processing for background_music.
	// Loop, fade, and ducking defaults are applied automatically when
	// the role is "background_music" AND the user did not explicitly
	// configure them. When the user provides ANY of loop/fade/ducking
	// fields, the compiler trusts those values verbatim (so a user can
	// opt out with loop:false, ducking_enabled:false, etc.).
	audioTracks := make([]plan.AudioTrack, 0, len(req.AudioTracks))
	for _, track := range req.AudioTracks {
		if track.SourceURL == "" {
			continue
		}
		volume := track.Volume
		if volume <= 0 {
			volume = 1.0
		}
		at := plan.AudioTrack{
			SourceURL:       track.SourceURL,
			Volume:          volume,
			StartTimeOffset: track.StartTimeOffset,
			DurationSeconds: track.DurationSeconds,
			Role:            track.Role,
		}

		if track.Role == "background_music" {
			if track.hasExplicitBGMConfig {
				// User explicitly configured BGM — trust
				// their values verbatim, even when zero.
				at.Loop = track.Loop
				at.FadeInSeconds = track.FadeInSeconds
				at.FadeOutSeconds = track.FadeOutSeconds
				at.DuckingEnabled = track.DuckingEnabled
			} else {
				// No explicit config — apply sensible
				// defaults for background music.
				at.Loop = true
				at.FadeInSeconds = 0.5
				at.FadeOutSeconds = 0.5
				at.DuckingEnabled = true
			}
		}

		audioTracks = append(audioTracks, at)
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
		Layers:      req.Layers,
		Subtitles:   req.Subtitles,
		OutputPath:  outputPath,
	}, nil
}
