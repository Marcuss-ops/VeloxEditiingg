package renderplan

// compiler_segments.go: visual segment interpretation for the
// RenderPlanCompiler — scene (clip/stock) fallback, clip_segments trim
// windows, and compiled items timeline.

import (
	"fmt"
	"strings"
)

// compileSceneSegments walks one scene and appends its visual segments in
// declared order (clip first, then stock[]), accumulating the timeline
// cursor. Returns the new cursor.
func compileSceneSegments(scene map[string]interface{}, sceneIndex int, cursor int64, segments *[]Segment) int64 {
	sceneDuration := sceneDurationMS(scene)
	if clip, ok := asMap(scene["clip"]); ok {
		if seg, duration, ok := segmentFromSceneAsset(clip, len(*segments), cursor, sceneDuration); ok {
			*segments = append(*segments, *seg)
			cursor += duration
		}
	}
	for _, stock := range sliceMaps(scene["stock"]) {
		if seg, duration, ok := segmentFromSceneAsset(stock, len(*segments), cursor, sceneDuration); ok {
			*segments = append(*segments, *seg)
			cursor += duration
		}
	}
	return cursor
}

// segmentFromSceneAsset converts a canonical scene asset (clip/stock) into a
// segment. The trim window comes from start_ms/end_ms when present.
func segmentFromSceneAsset(asset map[string]interface{}, index int, cursor, sceneDuration int64) (*Segment, int64, bool) {
	assetID, ok := assetIDOf(asset, "url")
	if !ok {
		return nil, 0, false
	}
	start := int64Param(asset, "start_ms")
	if start <= 0 {
		start = secondsToMS(floatParam(asset, "start_seconds"))
	}
	end := int64Param(asset, "end_ms")
	if end <= 0 {
		end = secondsToMS(floatParam(asset, "end_seconds"))
	}
	duration := int64Param(asset, "duration_ms")
	if duration <= 0 && end > start {
		duration = end - start
	}
	if duration <= 0 {
		duration = sceneDuration
	}
	return &Segment{
		SegmentID:       fmt.Sprintf("seg_%03d", index),
		AssetID:         assetID,
		AssetSHA256:     strParam(asset, "sha256"),
		SourceInMS:      start,
		SourceOutMS:     end,
		TimelineStartMS: cursor,
	}, duration, true
}

// segmentFromClipSegment converts one clip_segments entry into a segment.
// Supported shapes: {source: "velox-asset://<id>" | {asset_id,url}, start_ms,
// end_ms} or {asset_id, url, start_ms, end_ms}.
func segmentFromClipSegment(item map[string]interface{}, index int, cursor int64) (*Segment, int64, error) {
	assetID, ok := assetIDOf(item, "source")
	if !ok {
		assetID, ok = assetIDOf(item, "url")
	}
	if !ok {
		if bare := strings.TrimSpace(strParam(item, "asset_id")); bare != "" {
			assetID, ok = bare, true
		}
	}
	if !ok {
		// Non-local source (drive URL, remote) stays deferred to the worker
		// resolver — the compiled plan only carries local asset references.
		return nil, 0, nil
	}
	start := int64Param(item, "start_ms")
	if start <= 0 {
		start = secondsToMS(floatParam(item, "start_seconds"))
	}
	end := int64Param(item, "end_ms")
	if end <= 0 {
		end = secondsToMS(floatParam(item, "end_seconds"))
	}
	duration := int64Param(item, "duration_ms")
	if duration <= 0 && end > start {
		duration = end - start
	}
	return &Segment{
		SegmentID:       fmt.Sprintf("seg_%03d", index),
		AssetID:         assetID,
		AssetSHA256:     strParam(item, "sha256"),
		SourceInMS:      start,
		SourceOutMS:     end,
		TimelineStartMS: cursor,
	}, duration, nil
}

// segmentFromTimelineItem converts one compiled timeline item into a segment.
// Items carry {type, url, duration (seconds), role, scene_id, asset_id,
// sha256}. Non-local urls are skipped (deferred to the worker).
func segmentFromTimelineItem(item map[string]interface{}, index int, cursor int64) (*Segment, int64, error) {
	assetID, ok := assetIDOf(item, "url")
	if !ok {
		if bare := strings.TrimSpace(strParam(item, "asset_id")); bare != "" {
			assetID, ok = bare, true
		}
	}
	if !ok {
		return nil, 0, nil
	}
	duration := secondsToMS(floatParam(item, "duration"))
	if duration <= 0 {
		duration = int64Param(item, "duration_ms")
	}
	if duration <= 0 {
		duration = secondsToMS(floatParam(item, "duration_seconds"))
	}
	return &Segment{
		SegmentID:       fmt.Sprintf("seg_%03d", index),
		AssetID:         assetID,
		AssetSHA256:     strParam(item, "sha256"),
		TimelineStartMS: cursor,
		SourceOutMS:     duration,
	}, duration, nil
}

// sceneDurationMS returns the scene span: duration_seconds, else the
// voiceover duration, else the clip duration.
func sceneDurationMS(scene map[string]interface{}) int64 {
	if d := secondsToMS(floatParam(scene, "duration_seconds")); d > 0 {
		return d
	}
	if voiceover, ok := asMap(scene["voiceover"]); ok {
		if d := int64Param(voiceover, "duration_ms"); d > 0 {
			return d
		}
	}
	if clip, ok := asMap(scene["clip"]); ok {
		if d := int64Param(clip, "duration_ms"); d > 0 {
			return d
		}
	}
	return 0
}
