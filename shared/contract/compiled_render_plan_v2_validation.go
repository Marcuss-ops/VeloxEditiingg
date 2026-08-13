package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
)

// CompiledPlanV2DurationToleranceUS is the maximum allowed difference
// between the plan duration and the already-finalized audio duration.
// Duration mismatches are rejected rather than repaired with -shortest.
const CompiledPlanV2DurationToleranceUS int64 = 1_000

// CompiledRenderPlanV2ValidationError identifies one rejected V2 field.
type CompiledRenderPlanV2ValidationError struct {
	Path     string
	Issue    string
	Expected string
	Observed string
}

func (e CompiledRenderPlanV2ValidationError) Error() string {
	message := e.Path + ": " + e.Issue
	if e.Expected != "" {
		message += " (expected " + e.Expected
		if e.Observed != "" {
			message += ", observed " + e.Observed
		}
		message += ")"
	}
	return message
}

// CompiledRenderPlanV2ValidationErrors contains all semantic violations found
// in one plan. Returning all violations makes admission failures diagnosable
// without weakening the fail-closed decision.
type CompiledRenderPlanV2ValidationErrors []CompiledRenderPlanV2ValidationError

func (e CompiledRenderPlanV2ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	parts := make([]string, len(e))
	for i, violation := range e {
		parts[i] = violation.Error()
	}
	return strings.Join(parts, "; ")
}

// ValidateCompiledRenderPlanV2 validates the typed V2 document. It does not
// perform JSON decoding; callers receiving wire bytes should use
// DecodeCompiledRenderPlanV2 or ValidateCompiledRenderPlanV2JSON instead.
func ValidateCompiledRenderPlanV2(plan *CompiledRenderPlanV2) error {
	if plan == nil {
		return CompiledRenderPlanV2ValidationErrors{{
			Path:     "$",
			Issue:    "required",
			Expected: "CompiledRenderPlanV2",
			Observed: "nil",
		}}
	}

	var violations CompiledRenderPlanV2ValidationErrors
	if plan.PlanVersion != CompiledPlanVersionV2 {
		violations.add("plan_version", "unsupported_version", fmt.Sprint(CompiledPlanVersionV2), fmt.Sprint(plan.PlanVersion))
	}
	if plan.TimelineRevision <= 0 {
		violations.add("timeline_revision", "out_of_range", "positive integer", fmt.Sprint(plan.TimelineRevision))
	}
	if !isLowerSHA256(plan.TimelineSHA256) {
		violations.add("timeline_sha256", "invalid_sha256", "64 lowercase hexadecimal characters", plan.TimelineSHA256)
	}
	if plan.DurationUS <= 0 {
		violations.add("duration_us", "out_of_range", "positive microseconds", fmt.Sprint(plan.DurationUS))
	}

	validateOutputContract(&violations, plan.Output)
	validateFinalAudio(&violations, plan)

	assetsByID := make(map[string]AssetRefV2, len(plan.Assets))
	if len(plan.Assets) == 0 {
		violations.add("assets", "empty", "at least one asset", "")
	}
	for index, asset := range plan.Assets {
		path := fmt.Sprintf("assets[%d]", index)
		validateAsset(&violations, path, asset)
		if asset.AssetID == "" {
			continue
		}
		if _, exists := assetsByID[asset.AssetID]; exists {
			violations.add(path+".asset_id", "duplicate", "unique asset_id", asset.AssetID)
			continue
		}
		assetsByID[asset.AssetID] = asset
	}

	if plan.FinalAudio.AssetID != "" {
		finalAudioAsset, ok := assetsByID[plan.FinalAudio.AssetID]
		if !ok {
			violations.add("final_audio.asset_id", "unknown_reference", "an asset_id in assets[]", plan.FinalAudio.AssetID)
		} else {
			if finalAudioAsset.Kind != "final_audio" {
				violations.add("final_audio.asset_id", "wrong_asset_kind", "final_audio", finalAudioAsset.Kind)
			}
			if finalAudioAsset.SHA256 != plan.FinalAudio.SHA256 {
				violations.add("final_audio.sha256", "asset_hash_mismatch", finalAudioAsset.SHA256, plan.FinalAudio.SHA256)
			}
			if finalAudioAsset.SizeBytes != plan.FinalAudio.SizeBytes {
				violations.add("final_audio.size_bytes", "asset_size_mismatch", fmt.Sprint(finalAudioAsset.SizeBytes), fmt.Sprint(plan.FinalAudio.SizeBytes))
			}
			if finalAudioAsset.DurationUS > 0 && !withinDuration(finalAudioAsset.DurationUS, plan.FinalAudio.DurationUS, CompiledPlanV2DurationToleranceUS) {
				violations.add("final_audio.duration_us", "asset_duration_mismatch", fmt.Sprint(finalAudioAsset.DurationUS), fmt.Sprint(plan.FinalAudio.DurationUS))
			}
		}
	}

	if len(plan.VideoTracks) == 0 {
		violations.add("video_tracks", "empty", "at least one video track", "")
	}
	totalFrames, totalFramesOK := outputFrameCount(plan.DurationUS, plan.Output.FPSNum, plan.Output.FPSDen)
	trackIDs := make(map[string]struct{}, len(plan.VideoTracks))
	segmentIDs := make(map[string]struct{})
	for trackIndex, track := range plan.VideoTracks {
		trackPath := fmt.Sprintf("video_tracks[%d]", trackIndex)
		if strings.TrimSpace(track.TrackID) == "" {
			violations.add(trackPath+".track_id", "required", "non-empty string", track.TrackID)
		} else if _, exists := trackIDs[track.TrackID]; exists {
			violations.add(trackPath+".track_id", "duplicate", "unique track_id", track.TrackID)
		} else {
			trackIDs[track.TrackID] = struct{}{}
		}
		if len(track.Segments) == 0 {
			violations.add(trackPath+".segments", "empty", "at least one segment", "")
		}
		for segmentIndex, segment := range track.Segments {
			segmentPath := fmt.Sprintf("%s.segments[%d]", trackPath, segmentIndex)
			validateSegment(&violations, segmentPath, segment, assetsByID, totalFrames, totalFramesOK, segmentIDs)
		}
	}

	if len(violations) > 0 {
		return violations
	}
	return nil
}

// DecodeCompiledRenderPlanV2 strictly decodes, validates, and canonical-form
// checks a V2 JSON document. Unknown fields, trailing JSON, semantic errors,
// and non-canonical whitespace/order are all rejected.
func DecodeCompiledRenderPlanV2(data []byte) (*CompiledRenderPlanV2, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("compiled render plan v2: empty document")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var plan CompiledRenderPlanV2
	if err := decoder.Decode(&plan); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: strict decode: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("compiled render plan v2: trailing JSON value")
		}
		return nil, fmt.Errorf("compiled render plan v2: trailing data: %w", err)
	}
	if err := ValidateCompiledRenderPlanV2(&plan); err != nil {
		return nil, err
	}

	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(data, canonical) {
		return nil, fmt.Errorf("compiled render plan v2: document is not canonical JSON")
	}
	return &plan, nil
}

// ValidateCompiledRenderPlanV2JSON is the validation-oriented alias for the
// strict decoder. It returns the decoded plan for callers that need it after
// admission.
func ValidateCompiledRenderPlanV2JSON(data []byte) (*CompiledRenderPlanV2, error) {
	return DecodeCompiledRenderPlanV2(data)
}

// ValidateCompiledRenderPlanV2Payload validates the TaskOffer envelope keys
// when a V2 compiled plan is present. A payload with neither V2 key remains
// valid for legacy workers; a partial or malformed V2 envelope fails closed.
// The SHA256 is computed over the exact JSON string carried on the wire.
func ValidateCompiledRenderPlanV2Payload(raw map[string]interface{}) error {
	if raw == nil {
		return nil
	}
	planJSON, hasJSON := raw[PayloadKeyCompiledRenderPlanJSON]
	planSHA, hasSHA := raw[PayloadKeyCompiledRenderPlanSHA]
	if !hasJSON && !hasSHA {
		return nil
	}
	if !hasJSON {
		return fmt.Errorf("compiled render plan v2: %q is required when %q is present", PayloadKeyCompiledRenderPlanJSON, PayloadKeyCompiledRenderPlanSHA)
	}
	if !hasSHA {
		return fmt.Errorf("compiled render plan v2: %q is required when %q is present", PayloadKeyCompiledRenderPlanSHA, PayloadKeyCompiledRenderPlanJSON)
	}

	rawJSON, ok := planJSON.(string)
	if !ok || strings.TrimSpace(rawJSON) == "" {
		return fmt.Errorf("compiled render plan v2: %q must be a non-empty string", PayloadKeyCompiledRenderPlanJSON)
	}
	rawSHA, ok := planSHA.(string)
	if !ok || !isLowerSHA256(strings.TrimSpace(rawSHA)) {
		return fmt.Errorf("compiled render plan v2: %q must be 64 lowercase hexadecimal characters", PayloadKeyCompiledRenderPlanSHA)
	}
	actual := sha256.Sum256([]byte(rawJSON))
	if rawSHA != hex.EncodeToString(actual[:]) {
		return fmt.Errorf("compiled render plan v2: %q does not match %q", PayloadKeyCompiledRenderPlanSHA, PayloadKeyCompiledRenderPlanJSON)
	}
	_, err := DecodeCompiledRenderPlanV2([]byte(rawJSON))
	return err
}

func validateOutputContract(violations *CompiledRenderPlanV2ValidationErrors, output OutputContractV2) {
	if output.Container != "mp4" {
		violations.add("output.container", "unsupported_value", "mp4", output.Container)
	}
	if strings.TrimSpace(output.VideoCodec) == "" {
		violations.add("output.video_codec", "required", "non-empty codec", output.VideoCodec)
	}
	if output.Width <= 0 {
		violations.add("output.width", "out_of_range", "positive integer", fmt.Sprint(output.Width))
	}
	if output.Height <= 0 {
		violations.add("output.height", "out_of_range", "positive integer", fmt.Sprint(output.Height))
	}
	if output.FPSNum <= 0 {
		violations.add("output.fps_num", "out_of_range", "positive integer", fmt.Sprint(output.FPSNum))
	}
	if output.FPSDen <= 0 {
		violations.add("output.fps_den", "out_of_range", "positive integer", fmt.Sprint(output.FPSDen))
	}
}

func validateFinalAudio(violations *CompiledRenderPlanV2ValidationErrors, plan *CompiledRenderPlanV2) {
	audio := plan.FinalAudio
	if audio.Mode != AudioModeFinalAudioCopy {
		violations.add("final_audio.mode", "unsupported_value", AudioModeFinalAudioCopy, audio.Mode)
	}
	if strings.TrimSpace(audio.AssetID) == "" {
		violations.add("final_audio.asset_id", "required", "asset_id", audio.AssetID)
	}
	if !isLowerSHA256(audio.SHA256) {
		violations.add("final_audio.sha256", "invalid_sha256", "64 lowercase hexadecimal characters", audio.SHA256)
	}
	if audio.SizeBytes <= 0 {
		violations.add("final_audio.size_bytes", "out_of_range", "positive bytes", fmt.Sprint(audio.SizeBytes))
	}
	if audio.Codec != "aac" {
		violations.add("final_audio.codec", "unsupported_value", "aac", audio.Codec)
	}
	if audio.SampleRateHz != 48_000 {
		violations.add("final_audio.sample_rate_hz", "unsupported_value", "48000", fmt.Sprint(audio.SampleRateHz))
	}
	if audio.Channels != 2 {
		violations.add("final_audio.channels", "unsupported_value", "2", fmt.Sprint(audio.Channels))
	}
	if audio.DurationUS <= 0 {
		violations.add("final_audio.duration_us", "out_of_range", "positive microseconds", fmt.Sprint(audio.DurationUS))
	} else if plan.DurationUS > 0 && !withinDuration(audio.DurationUS, plan.DurationUS, CompiledPlanV2DurationToleranceUS) {
		violations.add("final_audio.duration_us", "duration_mismatch", fmt.Sprintf("within %d microseconds of %d", CompiledPlanV2DurationToleranceUS, plan.DurationUS), fmt.Sprint(audio.DurationUS))
	}
	if audio.TimelineRevision != plan.TimelineRevision {
		violations.add("final_audio.timeline_revision", "timeline_revision_mismatch", fmt.Sprint(plan.TimelineRevision), fmt.Sprint(audio.TimelineRevision))
	}
	if audio.TimelineSHA256 != plan.TimelineSHA256 {
		violations.add("final_audio.timeline_sha256", "timeline_hash_mismatch", plan.TimelineSHA256, audio.TimelineSHA256)
	}
}

func validateAsset(violations *CompiledRenderPlanV2ValidationErrors, path string, asset AssetRefV2) {
	if strings.TrimSpace(asset.AssetID) == "" {
		violations.add(path+".asset_id", "required", "non-empty asset_id", asset.AssetID)
	}
	if !isLowerSHA256(asset.SHA256) {
		violations.add(path+".sha256", "invalid_sha256", "64 lowercase hexadecimal characters", asset.SHA256)
	}
	if asset.SizeBytes <= 0 {
		violations.add(path+".size_bytes", "out_of_range", "positive bytes", fmt.Sprint(asset.SizeBytes))
	}
	if strings.TrimSpace(asset.Kind) == "" {
		violations.add(path+".kind", "required", "non-empty asset kind", asset.Kind)
	}
	if asset.DurationUS < 0 {
		violations.add(path+".duration_us", "out_of_range", "non-negative microseconds", fmt.Sprint(asset.DurationUS))
	}
	if asset.Width < 0 || asset.Height < 0 {
		violations.add(path, "invalid_dimensions", "non-negative width and height", fmt.Sprintf("%dx%d", asset.Width, asset.Height))
	}
	if asset.Kind == "video" || asset.Kind == "prepared_video_fragment" || asset.Kind == "final_audio" {
		if asset.DurationUS <= 0 {
			violations.add(path+".duration_us", "required", "positive microseconds for media assets", fmt.Sprint(asset.DurationUS))
		}
	}
}

func validateSegment(violations *CompiledRenderPlanV2ValidationErrors, path string, segment VideoSegmentV2, assets map[string]AssetRefV2, totalFrames int64, totalFramesOK bool, segmentIDs map[string]struct{}) {
	if strings.TrimSpace(segment.SegmentID) == "" {
		violations.add(path+".segment_id", "required", "non-empty segment_id", segment.SegmentID)
	} else if _, exists := segmentIDs[segment.SegmentID]; exists {
		violations.add(path+".segment_id", "duplicate", "unique segment_id", segment.SegmentID)
	} else {
		segmentIDs[segment.SegmentID] = struct{}{}
	}
	if strings.TrimSpace(segment.AssetID) == "" {
		violations.add(path+".asset_id", "required", "asset_id in assets[]", segment.AssetID)
	}
	if !isLowerSHA256(segment.SHA256) {
		violations.add(path+".sha256", "invalid_sha256", "64 lowercase hexadecimal characters", segment.SHA256)
	}
	asset, assetFound := assets[segment.AssetID]
	if !assetFound {
		violations.add(path+".asset_id", "unknown_reference", "an asset_id in assets[]", segment.AssetID)
	} else {
		if asset.Kind != "video" && asset.Kind != "prepared_video_fragment" {
			violations.add(path+".asset_id", "wrong_asset_kind", "video or prepared_video_fragment", asset.Kind)
		}
		if segment.SHA256 != asset.SHA256 {
			violations.add(path+".sha256", "asset_hash_mismatch", asset.SHA256, segment.SHA256)
		}
		if sourceEnd, ok := nonNegativeSum(segment.SourceInUS, segment.SourceDurationUS); !ok || sourceEnd > asset.DurationUS {
			violations.add(path, "source_range_out_of_bounds", fmt.Sprintf("source_in_us + source_duration_us <= %d", asset.DurationUS), fmt.Sprintf("%d + %d", segment.SourceInUS, segment.SourceDurationUS))
		}
	}
	if segment.TimelineStartFrame < 0 {
		violations.add(path+".timeline_start_frame", "out_of_range", "non-negative frame", fmt.Sprint(segment.TimelineStartFrame))
	}
	if segment.FrameCount <= 0 {
		violations.add(path+".frame_count", "out_of_range", "positive frame count", fmt.Sprint(segment.FrameCount))
	}
	if segment.SourceInUS < 0 {
		violations.add(path+".source_in_us", "out_of_range", "non-negative microseconds", fmt.Sprint(segment.SourceInUS))
	}
	if segment.SourceDurationUS <= 0 {
		violations.add(path+".source_duration_us", "out_of_range", "positive microseconds", fmt.Sprint(segment.SourceDurationUS))
	}
	if endFrame, ok := nonNegativeSum(segment.TimelineStartFrame, segment.FrameCount); !ok {
		violations.add(path, "timeline_range_overflow", "timeline_start_frame + frame_count within int64", "overflow")
	} else if totalFramesOK && endFrame > totalFrames {
		violations.add(path, "timeline_out_of_bounds", fmt.Sprintf("timeline end <= %d frames", totalFrames), fmt.Sprint(endFrame))
	}
}

func (e *CompiledRenderPlanV2ValidationErrors) add(path, issue, expected, observed string) {
	*e = append(*e, CompiledRenderPlanV2ValidationError{
		Path: path, Issue: issue, Expected: expected, Observed: observed,
	})
}

func isLowerSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func withinDuration(left, right, tolerance int64) bool {
	if left >= right {
		return left-right <= tolerance
	}
	return right-left <= tolerance
}

func nonNegativeSum(left, right int64) (int64, bool) {
	if left < 0 || right < 0 || left > int64(^uint64(0)>>1)-right {
		return 0, false
	}
	return left + right, true
}

func outputFrameCount(durationUS int64, fpsNum, fpsDen int) (int64, bool) {
	if durationUS <= 0 || fpsNum <= 0 || fpsDen <= 0 {
		return 0, false
	}
	numerator := new(big.Int).Mul(big.NewInt(durationUS), big.NewInt(int64(fpsNum)))
	denominator := new(big.Int).Mul(big.NewInt(int64(fpsDen)), big.NewInt(1_000_000))
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, false
	}
	return quotient.Int64(), true
}
