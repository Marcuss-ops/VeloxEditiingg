package contract

import (
	"fmt"
	"sort"
	"strings"
)

// Visual replacement error codes. These are the MASTER-side timeline
// validation failures (range / overlap / bounds / asset identity). The
// media-level failures (duration mismatch, audio-present, profile mismatch,
// keyframe safety) are detected later by the worker's real probe and use the
// remaining VISUAL_REPLACEMENT_* / COPY_ONLY_* codes — the master does not
// trust any client-supplied media metadata.
const (
	VisualReplacementCodeInvalidRange = "VISUAL_REPLACEMENT_INVALID_RANGE"
	VisualReplacementCodeOverlap      = "VISUAL_REPLACEMENT_OVERLAP"
	VisualReplacementCodeOutOfBounds  = "VISUAL_REPLACEMENT_OUT_OF_BOUNDS"
	VisualReplacementCodeAssetInvalid = "VISUAL_REPLACEMENT_ASSET_INVALID"

	// Media-level (worker-probe) replacement failures. These are distinct
	// from the master-side range/overlap codes above: they are only raised
	// when the worker's real media probe proves a prepared replacement is
	// not the declared video-only, canonical-profile segment.
	VisualReplacementCodeDurationMismatch = "VISUAL_REPLACEMENT_DURATION_MISMATCH"
	VisualReplacementCodeAudioNotAllowed  = "VISUAL_REPLACEMENT_AUDIO_NOT_ALLOWED"
	CopyOnlyMediaSignatureMismatchCode    = "COPY_ONLY_MEDIA_SIGNATURE_MISMATCH"
)

// ReplacementDurationToleranceUS is the centralized, single-source tolerance
// for comparing a prepared replacement's real (or declared) duration against
// its timeline window. It exists for timebase/frame-rounding headroom ONLY:
// a replacement that is shorter or longer by more than this amount is a
// contract violation and is rejected with
// VISUAL_REPLACEMENT_DURATION_MISMATCH, never silently padded, trimmed or
// re-encoded. Callers MUST NOT re-derive a per-call `if diff < 0.1s`
// threshold — this constant is the validator.
const ReplacementDurationToleranceUS int64 = 50_000 // 50 ms, same headroom as the packet-copy probe gate

// ReplacementMediaProbe is the worker's real media probe for one prepared
// replacement, reduced to the fields the replacement contract cares about.
// It is the ONLY source of truth for the media-level replacement checks — the
// master never trusts any client-supplied media metadata, so a value is only
// admitted after the worker has actually probed the resolved asset file.
type ReplacementMediaProbe struct {
	Codec       string
	Profile     string
	PixelFormat string
	Width       int
	Height      int
	FPSNum      int
	FPSDen      int
	DurationUS  int64
	HasAudio    bool
}

// ValidateVisualReplacementMedia applies the fail-closed media-level checks
// for one prepared replacement against the canonical output profile and its
// declared timeline window. It returns a *VisualReplacementError carrying the
// exact machine-readable code; every rejection is final — the copy-only path
// has no transcode or repair fallback to route through.
//
// Checks, in order of the stable error codes:
//  1. Audio present            → VISUAL_REPLACEMENT_AUDIO_NOT_ALLOWED
//  2. Signature mismatch       → COPY_ONLY_MEDIA_SIGNATURE_MISMATCH
//  3. Duration out of window   → VISUAL_REPLACEMENT_DURATION_MISMATCH
func ValidateVisualReplacementMedia(profile CanonicalVideoProfileV1, replacementID string, windowUS int64, probe ReplacementMediaProbe) error {
	if probe.HasAudio {
		return visualReplacementErrorf(VisualReplacementCodeAudioNotAllowed, replacementID, 0, "prepared replacement must be video-only; audio stream is not allowed")
	}
	if !replacementSignatureMatchesProfile(profile, probe) {
		return visualReplacementErrorf(CopyOnlyMediaSignatureMismatchCode, replacementID, 0, "prepared replacement media signature does not match canonical profile %q (codec=%q profile=%q %dx%d %d/%d fps %s)", profile.ProfileID, probe.Codec, probe.Profile, probe.Width, probe.Height, probe.FPSNum, probe.FPSDen, probe.PixelFormat)
	}
	if windowUS <= 0 {
		return visualReplacementErrorf(VisualReplacementCodeInvalidRange, replacementID, 0, "replacement window must be positive")
	}
	if diff := absInt64(probe.DurationUS - windowUS); diff > ReplacementDurationToleranceUS {
		return visualReplacementErrorf(VisualReplacementCodeDurationMismatch, replacementID, 0, "prepared duration %d us differs from replacement window %d us by %d us (tolerance %d us)", probe.DurationUS, windowUS, diff, ReplacementDurationToleranceUS)
	}
	return nil
}

// replacementSignatureMatchesProfile reports whether the probed replacement
// stream carries the canonical profile's exact identity. Codec names are
// normalized so h264_nvenc/h264_vaapi/libx264 all collapse to the profile's
// "h264"; profile name, pixel format, dimensions and frame rate are compared
// strictly.
func replacementSignatureMatchesProfile(profile CanonicalVideoProfileV1, probe ReplacementMediaProbe) bool {
	codec := normalizeReplacementCodec(probe.Codec)
	if codec != profile.Codec || !strings.EqualFold(strings.TrimSpace(probe.Profile), profile.CodecProfile) {
		return false
	}
	if probe.Width != profile.Width || probe.Height != profile.Height {
		return false
	}
	if probe.FPSNum != profile.FPSNum || probe.FPSDen != profile.FPSDen {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(probe.PixelFormat), profile.PixelFormat)
}

// normalizeReplacementCodec collapses the ffmpeg encoder aliases for the
// canonical h264 stream onto the short codec name stored in the profile.
func normalizeReplacementCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "libx264", "h264_nvenc", "h264_vaapi":
		return "h264"
	default:
		return strings.ToLower(strings.TrimSpace(codec))
	}
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// VisualReplacement is one already-composited video segment that replaces
// the base visual timeline over the absolute interval
// [TimelineStartUS, TimelineEndUS). The replacement media is a FINISHED,
// video-only MP4: Velox places it on the timeline as a normal VideoSource
// and never overlays, composites or transcodes it. This is deliberately
// distinct from Layer (Chronon compositing) — a replacement IS the video.
type VisualReplacement struct {
	ReplacementID   string
	AssetID         string
	SHA256          string
	TimelineStartUS int64
	TimelineEndUS   int64
	ProfileID       string
}

// VisualReplacementError is a machine-readable visual-replacement violation.
// It carries the code + replacement_id + offending timeline position so the
// job response can surface the exact boundary that failed.
type VisualReplacementError struct {
	Code          string
	ReplacementID string
	TimelineUS    int64
	Message       string
}

func (e *VisualReplacementError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.ReplacementID != "" {
		return fmt.Sprintf("%s: replacement_id=%s timeline_us=%d: %s", e.Code, e.ReplacementID, e.TimelineUS, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func visualReplacementErrorf(code, replacementID string, timelineUS int64, format string, args ...any) *VisualReplacementError {
	return &VisualReplacementError{
		Code:          code,
		ReplacementID: replacementID,
		TimelineUS:    timelineUS,
		Message:       fmt.Sprintf(format, args...),
	}
}

// ResolveVisualReplacements splits base video segments around the supplied
// replacements and returns a contiguous, replacement-inserted timeline.
//
// base is ONE video track's compiled segments in timeline order. fpsNum /
// fpsDen convert the replacement microsecond boundaries to output frames.
// SegmentID is left unset on the output — the caller renumbers the resolved
// segments so IDs stay unique per track. After this function returns, the
// "replacement" concept disappears: every piece is a normal VideoSegmentV2
// (VideoSource), so no overlay semantics ever reach the renderer.
func ResolveVisualReplacements(base []VideoSegmentV2, replacements []VisualReplacement, fpsNum, fpsDen int) ([]VideoSegmentV2, error) {
	if len(replacements) == 0 {
		return base, nil
	}
	if fpsNum <= 0 || fpsDen <= 0 {
		return nil, fmt.Errorf("visual replacements: invalid frame rate %d/%d", fpsNum, fpsDen)
	}

	sorted := append([]VisualReplacement(nil), replacements...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].TimelineStartUS < sorted[j].TimelineStartUS
	})

	totalUS := int64(0)
	for _, seg := range base {
		if end := frameToUS(seg.TimelineStartFrame+seg.FrameCount, fpsNum, fpsDen); end > totalUS {
			totalUS = end
		}
	}

	for i, r := range sorted {
		if strings.TrimSpace(r.AssetID) == "" {
			return nil, visualReplacementErrorf(VisualReplacementCodeAssetInvalid, r.ReplacementID, r.TimelineStartUS, "asset_id is required")
		}
		if r.TimelineStartUS < 0 {
			return nil, visualReplacementErrorf(VisualReplacementCodeInvalidRange, r.ReplacementID, r.TimelineStartUS, "timeline_start_us must be >= 0")
		}
		if r.TimelineEndUS <= r.TimelineStartUS {
			return nil, visualReplacementErrorf(VisualReplacementCodeInvalidRange, r.ReplacementID, r.TimelineStartUS, "timeline_end_us (%d) must be greater than timeline_start_us (%d)", r.TimelineEndUS, r.TimelineStartUS)
		}
		if i > 0 && r.TimelineStartUS < sorted[i-1].TimelineEndUS {
			prev := sorted[i-1]
			return nil, visualReplacementErrorf(VisualReplacementCodeOverlap, r.ReplacementID, r.TimelineStartUS, "overlaps replacement %s [%d, %d)", prev.ReplacementID, prev.TimelineStartUS, prev.TimelineEndUS)
		}
		if r.TimelineEndUS > totalUS {
			return nil, visualReplacementErrorf(VisualReplacementCodeOutOfBounds, r.ReplacementID, r.TimelineEndUS, "timeline_end_us (%d) exceeds the total timeline duration (%d)", r.TimelineEndUS, totalUS)
		}
	}

	out := make([]VideoSegmentV2, 0, len(base)+2*len(sorted))
	ri := 0
	for _, seg := range base {
		segStart := frameToUS(seg.TimelineStartFrame, fpsNum, fpsDen)
		segEnd := frameToUS(seg.TimelineStartFrame+seg.FrameCount, fpsNum, fpsDen)
		cursor := segStart
		for ri < len(sorted) && sorted[ri].TimelineEndUS <= segStart {
			ri++
		}
		for ri < len(sorted) && sorted[ri].TimelineStartUS < segEnd {
			r := sorted[ri]
			rs := r.TimelineStartUS
			if rs < segStart {
				rs = segStart
			}
			re := r.TimelineEndUS
			if re > segEnd {
				re = segEnd
			}
			if rs > cursor {
				piece, err := basePiece(seg, cursor, rs, segStart, fpsNum, fpsDen)
				if err != nil {
					return nil, err
				}
				out = append(out, piece)
			}
			piece, err := preparedPiece(r, rs, re, fpsNum, fpsDen)
			if err != nil {
				return nil, err
			}
			out = append(out, piece)
			cursor = re
			if r.TimelineEndUS <= segEnd {
				ri++
			} else {
				// The replacement extends past this segment's end; leave ri
				// pointing at it so the next segment resumes the same cut.
				break
			}
		}
		if cursor < segEnd {
			piece, err := basePiece(seg, cursor, segEnd, segStart, fpsNum, fpsDen)
			if err != nil {
				return nil, err
			}
			out = append(out, piece)
		}
	}
	return out, nil
}

// basePiece emits the [startUS, endUS) window of a base segment, advancing
// its source window in lockstep with the timeline (source plays 1:1).
func basePiece(seg VideoSegmentV2, startUS, endUS, segStartUS int64, fpsNum, fpsDen int) (VideoSegmentV2, error) {
	startFrame, frameCount, err := pieceFrames(startUS, endUS, fpsNum, fpsDen)
	if err != nil {
		return VideoSegmentV2{}, err
	}
	return VideoSegmentV2{
		AssetID:            seg.AssetID,
		SHA256:             seg.SHA256,
		TimelineStartFrame: startFrame,
		FrameCount:         frameCount,
		SourceInUS:         seg.SourceInUS + (startUS - segStartUS),
		SourceDurationUS:   endUS - startUS,
	}, nil
}

// preparedPiece emits the replacement asset over [startUS, endUS). The
// source is always trimmed from zero: the prepared MP4 is self-contained.
func preparedPiece(r VisualReplacement, startUS, endUS int64, fpsNum, fpsDen int) (VideoSegmentV2, error) {
	startFrame, frameCount, err := pieceFrames(startUS, endUS, fpsNum, fpsDen)
	if err != nil {
		return VideoSegmentV2{}, err
	}
	return VideoSegmentV2{
		AssetID:            r.AssetID,
		SHA256:             r.SHA256,
		TimelineStartFrame: startFrame,
		FrameCount:         frameCount,
		SourceInUS:         0,
		SourceDurationUS:   endUS - startUS,
	}, nil
}

func pieceFrames(startUS, endUS int64, fpsNum, fpsDen int) (int64, int64, error) {
	startFrame, err := timeUSToNearestFrame(startUS, fpsNum, fpsDen)
	if err != nil {
		return 0, 0, fmt.Errorf("visual replacements: timeline start frame: %w", err)
	}
	endFrame, err := timeUSToNearestFrame(endUS, fpsNum, fpsDen)
	if err != nil {
		return 0, 0, fmt.Errorf("visual replacements: timeline end frame: %w", err)
	}
	frameCount := endFrame - startFrame
	if frameCount <= 0 {
		return 0, 0, fmt.Errorf("visual replacements: range [%d, %d) quantizes to zero frames", startUS, endUS)
	}
	return startFrame, frameCount, nil
}

// frameToUS converts an output frame index to microseconds using the plan's
// frame rate. fpsNum/fpsDen <= 0 yields 0 (the manifest validates the rate
// before the resolver runs).
func frameToUS(frame int64, fpsNum, fpsDen int) int64 {
	if frame <= 0 || fpsNum <= 0 || fpsDen <= 0 {
		return 0
	}
	return frame * int64(fpsDen) * 1_000_000 / int64(fpsNum)
}

// ParseVisualReplacements converts the raw payload slice produced by the
// projection layer into typed replacements. It accepts []interface{} and
// []map[string]interface{} and is tolerant of JSON-decoded numeric types.
func ParseVisualReplacements(raw any) ([]VisualReplacement, error) {
	if raw == nil {
		return nil, nil
	}
	var items []interface{}
	switch v := raw.(type) {
	case []interface{}:
		items = v
	case []map[string]interface{}:
		items = make([]interface{}, len(v))
		for i := range v {
			items[i] = v[i]
		}
	default:
		return nil, fmt.Errorf("visual replacements: must be an array, got %T", raw)
	}
	out := make([]VisualReplacement, 0, len(items))
	for i, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("visual replacements[%d]: must be an object", i)
		}
		out = append(out, VisualReplacement{
			ReplacementID:   vrString(m["replacement_id"]),
			AssetID:         vrString(m["asset_id"]),
			SHA256:          vrString(m["sha256"]),
			TimelineStartUS: vrInt64(m["timeline_start_us"]),
			TimelineEndUS:   vrInt64(m["timeline_end_us"]),
			ProfileID:       vrString(m["profile_id"]),
		})
	}
	return out, nil
}

func vrString(v any) string {
	s, _ := v.(string)
	return s
}

func vrInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case float32:
		return int64(n)
	default:
		return 0
	}
}
