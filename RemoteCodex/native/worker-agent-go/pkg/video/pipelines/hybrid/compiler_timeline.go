// Package hybrid / compiler_timeline.go
//
// Role-aware timeline building for the hybrid.v1 compiler:
// compileItemsToTimeline + the per-item source/duration/fit resolvers.
package hybrid

import "velox-worker-agent/pkg/video/plan"

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
			IncludeAudio:    item.IncludeAudio,
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
