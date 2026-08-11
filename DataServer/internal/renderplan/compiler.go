// Package renderplan — compiler.go: RenderPlanCompiler.
//
// The compiler interprets the canonical worker payload into a
// CompiledRenderPlan. It is deliberately dependency-light: parsing helpers
// here are local so the compiler stays pure (no downloads, no filesystem,
// no ffprobe). Optional registry-metadata enrichment goes through the
// MetadataResolver seam and NEVER fails the compile.
package renderplan

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"velox-shared/assetref"
)

// AssetMetadata is the registry-derived description the compiler attaches to
// plan assets. Always best-effort: a nil resolver or a failed lookup leaves
// the fields empty and NEVER fails the compile.
type AssetMetadata struct {
	AssetID    string
	SHA256     string
	Kind       string
	MimeType   string
	SizeBytes  int64
	DurationMs int64
	Width      int
	Height     int
}

// MetadataResolver resolves registry metadata for a local asset reference.
// Implementations MUST be read-only (registry reads only — no downloads,
// no writes).
type MetadataResolver interface {
	ResolveAssetMetadata(ctx context.Context, assetID string) (AssetMetadata, error)
}

// Options configures a RenderPlanCompiler.
type Options struct {
	// MetadataResolver optionally enriches plan assets with registry
	// metadata (sha256, media description). Nil disables enrichment.
	MetadataResolver MetadataResolver
	// DefaultContract is the media contract used when the payload does
	// not declare one. Zero fields fall back to 1080p30 h264.
	DefaultContract MediaContract
}

// RenderPlanCompiler is the canonical master-side component that turns a
// normalized worker payload into a CompiledRenderPlan. It performs NO
// prefetch and NO download: compile is interpretation plus optional
// read-only registry metadata enrichment.
type RenderPlanCompiler struct {
	resolver        MetadataResolver
	defaultContract MediaContract
}

// NewCompiler returns the production compiler. Defaults: 1920x1080@30 h264,
// copy_only=false.
func NewCompiler(opts Options) *RenderPlanCompiler {
	contract := opts.DefaultContract
	if contract.Width == 0 {
		contract.Width = 1920
	}
	if contract.Height == 0 {
		contract.Height = 1080
	}
	if contract.FpsNum == 0 {
		contract.FpsNum = 30
	}
	if contract.FpsDen == 0 {
		contract.FpsDen = 1
	}
	if contract.VideoCodec == "" {
		contract.VideoCodec = "h264"
	}
	return &RenderPlanCompiler{
		resolver:        opts.MetadataResolver,
		defaultContract: contract,
	}
}

// Compile interprets the normalized worker payload for the given attempt and
// returns the compiled plan. attemptID is required (it is part of the
// canonical document and therefore of plan_sha256).
//
// Timeline precedence (documented, deterministic):
//  1. clip_segments — explicit trim windows (scene-image flow);
//  2. items         — the compiled clip/stock timeline (authoritative);
//  3. scenes        — interpreted scene clips/stocks with trims.
//
// Audio precedence: audio_tracks (compiled) when present, else per-scene
// voiceover assets.
func (c *RenderPlanCompiler) Compile(ctx context.Context, payload map[string]interface{}, attemptID string) (*CompiledRenderPlan, error) {
	if c == nil {
		return nil, fmt.Errorf("render plan: compiler unavailable")
	}
	if payload == nil {
		return nil, fmt.Errorf("render plan: payload is required")
	}
	jobID := strings.TrimSpace(strParam(payload, "job_id"))
	if jobID == "" {
		return nil, fmt.Errorf("render plan: job_id is required")
	}
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return nil, fmt.Errorf("render plan: attempt_id is required")
	}

	contract := c.mediaContract(payload)
	var segments []Segment
	var scenes []map[string]interface{}
	cursor := int64(0)

	switch {
	case nonEmptySlice(payload["clip_segments"]):
		for i, item := range sliceMaps(payload["clip_segments"]) {
			seg, duration, err := segmentFromClipSegment(item, i, cursor)
			if err != nil {
				return nil, fmt.Errorf("render plan: clip_segments[%d]: %w", i, err)
			}
			if seg == nil {
				continue
			}
			segments = append(segments, *seg)
			cursor += duration
		}
		scenes = extractScenes(payload)
	case nonEmptySlice(payload["items"]):
		for i, item := range sliceMaps(payload["items"]) {
			seg, duration, err := segmentFromTimelineItem(item, i, cursor)
			if err != nil {
				return nil, fmt.Errorf("render plan: items[%d]: %w", i, err)
			}
			if seg == nil {
				continue
			}
			segments = append(segments, *seg)
			cursor += duration
		}
		scenes = extractScenes(payload)
	default:
		scenes = extractScenes(payload)
		if len(scenes) == 0 {
			return nil, fmt.Errorf("render plan: no renderable timeline (clip_segments/items/scenes all empty)")
		}
		for i, scene := range scenes {
			cursor = c.compileSceneSegments(scene, i, cursor, &segments)
		}
	}

	// Audio: compiled audio_tracks win; otherwise derive per-scene voiceovers.
	audio := compileAudioTracks(payload)
	if len(audio) == 0 && len(scenes) > 0 {
		audio = compileSceneVoiceovers(scenes)
	}

	assets := c.collectAssets(ctx, segments, audio)

	duration := cursor
	for _, track := range audio {
		if end := track.StartMS + track.DurationMS; end > duration {
			duration = end
		}
	}
	if declared := durationDeclared(payload); declared > duration {
		duration = declared
	}

	plan := &CompiledRenderPlan{
		PlanVersion:   PlanVersion,
		JobID:         jobID,
		AttemptID:     attemptID,
		DurationMS:    duration,
		MediaContract: contract,
		Segments:      segments,
		Audio:         audio,
		Assets:        assets,
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return plan, nil
}

// ── Scene interpretation (fallback + trims) ────────────────────────────────

// compileSceneSegments walks one scene and appends its visual segments in
// declared order (clip first, then stock[]), accumulating the timeline
// cursor. Returns the new cursor.
func (c *RenderPlanCompiler) compileSceneSegments(scene map[string]interface{}, sceneIndex int, cursor int64, segments *[]Segment) int64 {
	sceneDuration := sceneDurationMS(scene)
	if clip, ok := asMap(scene["clip"]); ok {
		if seg, duration, ok := segmentFromSceneAsset(clip, "clip", len(*segments), cursor, sceneDuration); ok {
			*segments = append(*segments, *seg)
			cursor += duration
		}
	}
	for _, stock := range sliceMaps(scene["stock"]) {
		if seg, duration, ok := segmentFromSceneAsset(stock, "stock", len(*segments), cursor, sceneDuration); ok {
			*segments = append(*segments, *seg)
			cursor += duration
		}
	}
	return cursor
}

// segmentFromSceneAsset converts a canonical scene asset (clip/stock) into a
// segment. The trim window comes from start_ms/end_ms when present.
func segmentFromSceneAsset(asset map[string]interface{}, role string, index int, cursor, sceneDuration int64) (*Segment, int64, bool) {
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
	_ = role
	return &Segment{
		SegmentID:       fmt.Sprintf("seg_%03d", index),
		AssetID:         assetID,
		AssetSHA256:     strParam(asset, "sha256"),
		SourceInMS:      start,
		SourceOutMS:     end,
		TimelineStartMS: cursor,
	}, duration, true
}

// ── clip_segments interpretation ───────────────────────────────────────────

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

// ── items timeline interpretation ──────────────────────────────────────────

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

// ── Audio ──────────────────────────────────────────────────────────────────

// compileAudioTracks converts payload audio_tracks into plan audio entries.
// Only tracks whose source_url is a canonical velox wire reference (or that
// carry an explicit asset_id) become plan audio; other sources stay deferred.
func compileAudioTracks(payload map[string]interface{}) []AudioTrack {
	var out []AudioTrack
	for _, track := range sliceMaps(payload["audio_tracks"]) {
		assetID, ok := assetIDOf(track, "source_url")
		if !ok {
			if bare := strings.TrimSpace(strParam(track, "asset_id")); bare != "" {
				assetID, ok = bare, true
			}
		}
		if !ok {
			continue
		}
		duration := secondsToMS(floatParam(track, "duration_seconds"))
		if duration <= 0 {
			duration = int64Param(track, "duration_ms")
		}
		out = append(out, AudioTrack{
			AssetID:     assetID,
			AssetSHA256: strParam(track, "sha256"),
			Role:        strParam(track, "role"),
			StartMS:     secondsToMS(floatParam(track, "start_time_offset")),
			DurationMS:  duration,
			Volume:      floatParam(track, "volume"),
			Loop:        boolParam(track, "loop"),
			FadeInMS:    secondsToMS(floatParam(track, "fade_in_seconds")),
			FadeOutMS:   secondsToMS(floatParam(track, "fade_out_seconds")),
		})
	}
	return out
}

// compileSceneVoiceovers derives per-scene voiceover audio tracks with the
// scene's timeline start offset.
func compileSceneVoiceovers(scenes []map[string]interface{}) []AudioTrack {
	var out []AudioTrack
	cursor := int64(0)
	for _, scene := range scenes {
		sceneDuration := sceneDurationMS(scene)
		if voiceover, ok := asMap(scene["voiceover"]); ok {
			assetID, hasID := assetIDOf(voiceover, "url")
			if !hasID {
				if bare := strings.TrimSpace(strParam(voiceover, "asset_id")); bare != "" {
					assetID, hasID = bare, true
				}
			}
			if hasID {
				duration := int64Param(voiceover, "duration_ms")
				if duration <= 0 {
					duration = sceneDuration
				}
				out = append(out, AudioTrack{
					AssetID:     assetID,
					AssetSHA256: strParam(voiceover, "sha256"),
					Role:        "voiceover",
					StartMS:     cursor,
					DurationMS:  duration,
					Volume:      1.0,
				})
			}
		}
		cursor += sceneDuration
	}
	return out
}

// ── Media contract ─────────────────────────────────────────────────────────

// mediaContract derives the output contract from the payload (nested output
// map + top-level copy_only/video_mode/orientation), falling back to the
// compiler default.
func (c *RenderPlanCompiler) mediaContract(payload map[string]interface{}) MediaContract {
	mc := c.defaultContract
	if output, ok := asMap(payload["output"]); ok {
		if boolParam(output, "copy_only") {
			mc.CopyOnly = true
		}
		if v := int64Param(output, "width"); v > 0 {
			mc.Width = int(v)
		}
		if v := int64Param(output, "height"); v > 0 {
			mc.Height = int(v)
		}
		if v := int64Param(output, "fps"); v > 0 {
			mc.FpsNum = int(v)
			mc.FpsDen = 1
		}
		if v := strings.TrimSpace(strParam(output, "codec", "video_codec")); v != "" {
			mc.VideoCodec = v
		}
	}
	if boolParam(payload, "copy_only") {
		mc.CopyOnly = true
	}
	if v := strings.TrimSpace(strParam(payload, "video_mode")); strings.EqualFold(v, "clip_stock") {
		mc.CopyOnly = true
	}
	if v := strings.TrimSpace(strParam(payload, "orientation")); strings.EqualFold(v, "vertical") || strings.EqualFold(v, "portrait") {
		if mc.Width <= 0 || mc.Height <= 0 {
			mc.Width, mc.Height = 1080, 1920
		}
	}
	return mc
}

// ── Asset collection + enrichment ──────────────────────────────────────────

// collectAssets dedupes every asset referenced by the plan and enriches it
// with registry metadata (best-effort). The result is sorted by asset_id so
// the canonical JSON (and therefore plan_sha256) is order-stable.
func (c *RenderPlanCompiler) collectAssets(ctx context.Context, segments []Segment, audio []AudioTrack) []AssetRef {
	seen := make(map[string]AssetRef)
	merge := func(id, sha string) {
		if strings.TrimSpace(id) == "" {
			return
		}
		ref := seen[id]
		if ref.AssetID == "" {
			ref.AssetID = id
		}
		if ref.SHA256 == "" {
			ref.SHA256 = sha
		}
		seen[id] = ref
	}
	for _, seg := range segments {
		merge(seg.AssetID, seg.AssetSHA256)
	}
	for _, track := range audio {
		merge(track.AssetID, track.AssetSHA256)
	}
	if c != nil && c.resolver != nil {
		for id := range seen {
			meta, err := c.resolver.ResolveAssetMetadata(ctx, id)
			if err != nil {
				// Best-effort enrichment: a missing registry row must never
				// fail the compile or invent metadata.
				continue
			}
			ref := seen[id]
			if ref.SHA256 == "" {
				ref.SHA256 = meta.SHA256
			}
			ref.Kind = meta.Kind
			ref.MimeType = meta.MimeType
			if ref.SizeBytes == 0 {
				ref.SizeBytes = meta.SizeBytes
			}
			ref.DurationMS = meta.DurationMs
			ref.Width = meta.Width
			ref.Height = meta.Height
			seen[id] = ref
		}
	}
	out := make([]AssetRef, 0, len(seen))
	for _, ref := range seen {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AssetID < out[j].AssetID })
	return out
}

// ── Payload helpers (local, dependency-light) ──────────────────────────────

// extractScenes reads the canonical scenes array or scenes_json string.
func extractScenes(payload map[string]interface{}) []map[string]interface{} {
	if raw, present := payload["scenes"]; present {
		if scenes := sliceMaps(raw); len(scenes) > 0 {
			return scenes
		}
	}
	if raw := strings.TrimSpace(strParam(payload, "scenes_json")); raw != "" {
		var scenes []map[string]interface{}
		if json.Unmarshal([]byte(raw), &scenes) == nil && len(scenes) > 0 {
			return scenes
		}
		var generic []interface{}
		if json.Unmarshal([]byte(raw), &generic) == nil {
			out := make([]map[string]interface{}, 0, len(generic))
			for _, item := range generic {
				if m, ok := item.(map[string]interface{}); ok {
					out = append(out, m)
				}
			}
			return out
		}
	}
	return nil
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

// durationDeclared reads the payload-declared total duration (seconds).
func durationDeclared(payload map[string]interface{}) int64 {
	return secondsToMS(floatParam(payload, "total_duration_secs"))
}

// assetIDOf extracts the canonical asset id from a map carrying a wire
// reference under one of the given keys (velox-asset:// or velox-drive://),
// or from an explicit asset_id field.
func assetIDOf(m map[string]interface{}, urlKeys ...string) (string, bool) {
	for _, key := range urlKeys {
		if id, ok := assetref.WireAssetID(strParam(m, key)); ok {
			return id, true
		}
	}
	if bare := strings.TrimSpace(strParam(m, "asset_id")); bare != "" {
		return bare, true
	}
	return "", false
}

func nonEmptySlice(value interface{}) bool {
	return len(sliceMaps(value)) > 0
}

func sliceMaps(value interface{}) []map[string]interface{} {
	switch typed := value.(type) {
	case []map[string]interface{}:
		return typed
	case []interface{}:
		out := make([]map[string]interface{}, 0, len(typed))
		for _, item := range typed {
			if m, ok := item.(map[string]interface{}); ok {
				out = append(out, m)
			}
		}
		return out
	default:
		return nil
	}
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	m, ok := value.(map[string]interface{})
	return m, ok
}

func strParam(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if s, ok := m[key].(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func boolParam(m map[string]interface{}, key string) bool {
	b, _ := m[key].(bool)
	return b
}

// floatParam reads a numeric value (float64/int/string) as float64.
func floatParam(m map[string]interface{}, key string) float64 {
	if m == nil {
		return 0
	}
	value := m[key]
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case int32:
		return float64(typed)
	case json.Number:
		f, _ := typed.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return f
	default:
		return 0
	}
}

func int64Param(m map[string]interface{}, key string) int64 {
	return int64(floatParam(m, key))
}

func secondsToMS(seconds float64) int64 {
	if seconds <= 0 {
		return 0
	}
	return int64(seconds * 1000)
}
