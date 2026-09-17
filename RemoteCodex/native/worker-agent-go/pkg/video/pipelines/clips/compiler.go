// Package clips implements the clips.v1 pipeline compiler.
// It produces a RenderPlan from a list of video clip URLs + audio.
package clips

import (
	"context"
	"fmt"
	"math"
	"strings"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/pkg/video/plan"
	"velox-worker-agent/pkg/video/services/audio"
)

// Request is the validated input for the clips.v1 pipeline.
type Request struct {
	Clips    []ClipInput
	AudioURL string
	Fit      string // "contain", "cover", "stretch"
	CopyOnly bool
}

// ClipInput is a single clip with URL and duration.
type ClipInput struct {
	URL      string
	Duration float64
}

// Validate checks raw input parameters for the clips.v1 pipeline.
func Validate(input map[string]interface{}) error {
	if encoded := toString(input["scenes_json"]); encoded != "" {
		scenes, err := decodeSceneTimeline(encoded)
		if err != nil {
			return fmt.Errorf("clips.v1: invalid scenes_json: %w", err)
		}
		if sceneTimelineRequired(scenes) {
			return validateSceneTimeline(scenes)
		}
	}
	if !toBoolDefault(input["copy_only"], false) {
		return fmt.Errorf("clips.v1: copy-only policy is required; set copy_only=true")
	}
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
	// A clips-only submission may intentionally omit an external final mix.
	// In that mode the renderer preserves each source clip's original audio;
	// an explicit audio_url still takes precedence when supplied.
	return nil
}

// Compile produces a RenderPlan from the clips.v1 request.
func Compile(ctx context.Context, jobID string, input map[string]interface{}, outputPath string, probe audio.Probe) (*plan.RenderPlan, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}
	if encoded := toString(input["scenes_json"]); encoded != "" {
		if scenes, err := decodeSceneTimeline(encoded); err == nil && sceneTimelineRequired(scenes) {
			compiled, err := compileSceneTimeline(ctx, jobID, scenes, outputPath, probe)
			if err != nil {
				return nil, err
			}
			return applyOverlayIntent(compiled, input)
		}
	}

	req := parseRequest(input)

	// Build timeline
	timeline_items := make([]plan.TimelineItem, len(req.Clips))
	for i, clip := range req.Clips {
		timeline_items[i] = plan.TimelineItem{
			Source:          plan.MediaSource{Type: "video", URL: clip.URL},
			DurationSeconds: clip.Duration,
			IncludeAudio:    req.AudioURL == "",
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

	compiled := &plan.RenderPlan{
		Version:     1,
		JobID:       jobID,
		Canvas:      plan.DefaultCanvas(),
		CopyOnly:    true,
		Timeline:    timeline_items,
		AudioTracks: audioTracks,
		OutputPath:  outputPath,
	}
	return applyOverlayIntent(compiled, input)
}

// applyOverlayIntent is the worker-side bridge for legacy clips.v1 jobs. A
// replace overlay is converted to one contiguous editorial timeline using
// the shared frame resolver. The supplied overlay contract represents still
// images; those segments are rendered by the image-capable editorial path,
// while the surrounding stock/clip timeline remains intact. Composite intent
// is never forwarded as native layers: it must be prepared by Chronon and
// arrive as certified V2 prepared_video_fragment assets.
func applyOverlayIntent(renderPlan *plan.RenderPlan, input map[string]interface{}) (*plan.RenderPlan, error) {
	if renderPlan == nil {
		return nil, fmt.Errorf("clips.v1: nil render plan")
	}
	overlays, err := contract.ParseOverlays(input["overlays"])
	if err != nil {
		return nil, err
	}
	if len(overlays) == 0 {
		return renderPlan, nil
	}
	// Overlay timing is bound to the canonical Velox stream profile: 24 fps.
	// Do not interpret start_frame using the legacy clips.v1 30 fps default.
	const overlayFPS = 24
	renderPlan.Canvas.Fps = overlayFPS
	base := make([]contract.VideoSegmentV2, 0, len(renderPlan.Timeline))
	baseURLs := make(map[string]string, len(renderPlan.Timeline))
	var cursor int64
	for index, item := range renderPlan.Timeline {
		if item.Source.Type != "video" || strings.TrimSpace(item.Source.URL) == "" {
			return nil, fmt.Errorf("clips.v1: overlays require a video-only base timeline")
		}
		frames := int64(math.Round(item.DurationSeconds * float64(overlayFPS)))
		if frames <= 0 {
			return nil, fmt.Errorf("clips.v1: timeline segment %d has zero frames", index)
		}
		assetID := fmt.Sprintf("base-segment-%06d", index)
		baseURLs[assetID] = item.Source.URL
		base = append(base, contract.VideoSegmentV2{AssetID: assetID, TimelineStartFrame: cursor, FrameCount: frames, SourceInUS: item.SourceInUS, SourceDurationUS: item.SourceDurationUS})
		cursor += frames
	}
	if _, windows, err := contract.ResolveOverlayTimeline(base, overlays, overlayFPS, 1); err != nil {
		return nil, fmt.Errorf("clips.v1: overlay timeline: %w", err)
	} else if len(windows) > 0 {
		return nil, fmt.Errorf("clips.v1: composite overlays require Chronon prepared fragments (%d windows); native Velox layers are unsupported", len(windows))
	}
	hasReplaceOverlay := false
	for _, overlay := range overlays {
		if overlay.Mode != string(contract.OverlayModeReplace) {
			continue
		}
		hasReplaceOverlay = true
		url := strings.TrimSpace(overlay.URL)
		if url == "" {
			if ref, refErr := assetref.NewDeferredDrive(overlay.AssetID); refErr == nil {
				url = ref.Wire()
			}
		}
		if url == "" {
			return nil, fmt.Errorf("clips.v1: overlay %q has no resolvable URL", overlay.ID)
		}
		baseURLs[overlay.AssetID] = url
	}
	resolved, _, err := contract.ResolveOverlayTimeline(base, overlays, overlayFPS, 1)
	if err != nil {
		return nil, fmt.Errorf("clips.v1: overlay timeline: %w", err)
	}
	timeline := make([]plan.TimelineItem, 0, len(resolved))
	for _, segment := range resolved {
		url := baseURLs[segment.AssetID]
		if url == "" {
			return nil, fmt.Errorf("clips.v1: overlay asset %q was not resolved", segment.AssetID)
		}
		timeline = append(timeline, plan.TimelineItem{
			Source:          plan.MediaSource{Type: "image", URL: url},
			DurationSeconds: float64(segment.FrameCount) / float64(overlayFPS),
			IncludeAudio:    false, SourceInUS: segment.SourceInUS, SourceDurationUS: segment.SourceDurationUS,
		})
	}
	renderPlan.Timeline = timeline
	if hasReplaceOverlay {
		// Image replacements require the image-capable editorial renderer. Do
		// not send them to copy_only or mixed packet muxing: PNG/JPEG source
		// streams are not packet-compatible with the H.264 base timeline.
		renderPlan.CopyOnly = false
		renderPlan.Mixed = false
	}
	return renderPlan, nil
}

func parseRequest(input map[string]interface{}) *Request {
	req := &Request{
		AudioURL: firstNonEmptyString(input, "audio_url", "audio_path", "voiceover_path", "voiceover"),
		Fit:      toStringDefault(input["fit"], "contain"),
		CopyOnly: toBoolDefault(input["copy_only"], false),
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

func firstNonEmptyString(input map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(toString(input[key])); value != "" {
			return value
		}
	}
	return ""
}

func toBoolDefault(v interface{}, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
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
