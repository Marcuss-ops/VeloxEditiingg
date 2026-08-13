// Package renderplan — compiler.go: RenderPlanCompiler.
//
// The compiler interprets the canonical worker payload into a
// CompiledRenderPlan. It is deliberately dependency-light: parsing helpers
// here are local so the compiler stays pure (no downloads, no filesystem,
// no ffprobe). Optional registry-metadata enrichment goes through the
// MetadataResolver seam and NEVER fails the compile.
//
// File layout:
//   - compiler.go            types, constructor, Compile entrypoint
//   - compiler_segments.go   visual segment interpretation
//   - compiler_audio.go      audio track + scene voiceover compilation
//   - compiler_contract.go   media contract + asset collection/enrichment
//   - compiler_payload.go    local payload parsing helpers
package renderplan

import (
	"context"
	"fmt"
	"strings"
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
			cursor = compileSceneSegments(scene, i, cursor, &segments)
		}
	}

	// Audio: compiled audio_tracks win; otherwise derive per-scene voiceovers.
	// Caveat (metadata-only): when clip_segments is the visual source, the
	// scene-derived voiceover StartMS accumulates by sceneDurationMS and may
	// not match the clip_segments timeline exactly — the worker still muxes
	// audio from the payload; the plan offsets are a determinism aid, not a
	// replacement for the payload's own audio timeline.
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
