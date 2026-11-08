package contract

import (
	"fmt"
	"strconv"
)

// CanonicalVideoEncodeArgs is the single FFmpeg argument projection for the
// canonical H.264 stream identity. Callers append container-specific options
// and the output path after these video-only codec settings.
func CanonicalVideoEncodeArgs(profile CanonicalVideoProfileV1, quality PreparationQualityPolicy) []string {
	openGOP := 0
	if !profile.ClosedGOP {
		openGOP = 1
	}
	return []string{
		"-c:v", "libx264",
		"-b:v", quality.VideoBitrate,
		"-maxrate", quality.VideoMaxRate,
		"-bufsize", quality.VideoBufferSize,
		"-pix_fmt", profile.PixelFormat,
		"-profile:v", profile.CodecProfile,
		"-level:v", profile.CodecLevel,
		"-g", strconv.Itoa(profile.GOPSize),
		"-bf", strconv.Itoa(profile.BFrames),
		"-sc_threshold", "0",
		"-x264-params", fmt.Sprintf("scenecut=0:open-gop=%d:keyint=%d:min-keyint=%d", openGOP, profile.GOPSize, profile.GOPSize),
		"-video_track_timescale", strconv.Itoa(profile.TimeBaseDen),
	}
}

// CanonicalVideoScaleGeometryFilter returns the canonical letterbox/pad
// geometry. Keeping this in the shared contract prevents the Master and
// workers from preparing visually or bitstream-incompatible fragments.
func CanonicalVideoScaleGeometryFilter(profile CanonicalVideoProfileV1) string {
	return fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
		profile.Width, profile.Height, profile.Width, profile.Height)
}

// CanonicalVideoScaleFilter adds the profile frame rate to the shared
// canonical geometry filter.
func CanonicalVideoScaleFilter(profile CanonicalVideoProfileV1) string {
	return fmt.Sprintf("%s,fps=%d/%d", CanonicalVideoScaleGeometryFilter(profile), profile.FPSNum, profile.FPSDen)
}

// CanonicalVideoWindowArgs builds a frame-accurate video-only preparation
// command. It selects the requested source-frame interval, normalizes source
// timestamps, converts to the output profile rate/geometry, forces an IDR at
// the fragment boundary and limits output to the declared exact frame count.
func CanonicalVideoWindowArgs(
	profile CanonicalVideoProfileV1,
	quality PreparationQualityPolicy,
	inputPath, outputPath string,
	sourceStartFrame, sourceFrameCount int64,
	sourceFPSNum, sourceFPSDen int,
	outputFrameCount int64,
) []string {
	lastSourceFrame := sourceStartFrame + sourceFrameCount - 1
	filter := fmt.Sprintf("select='between(n\\,%d\\,%d)',setpts=N*%d/(%d*TB),fps=%d/%d,%s",
		sourceStartFrame, lastSourceFrame, sourceFPSDen, sourceFPSNum,
		profile.FPSNum, profile.FPSDen, CanonicalVideoScaleGeometryFilter(profile))
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-i", inputPath,
		"-map", "0:v:0", "-an", "-sn", "-dn", "-vf", filter,
	}
	args = append(args, CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args,
		"-fps_mode", "passthrough",
		"-force_key_frames", "expr:eq(n,0)",
		"-frames:v", strconv.FormatInt(outputFrameCount, 10),
		"-avoid_negative_ts", "make_zero", "-reset_timestamps", "1",
		"-movflags", "+faststart", outputPath,
	)
	return args
}

// CanonicalVideoWindowTimestampArgs prepares a source-time interval when the
// producer does not claim a CFR frame index for the original media. Input
// seeking stays before -i so FFmpeg seeks to a preceding keyframe and decodes
// forward accurately; output is still normalized to an exact canonical frame
// count and the same H.264 bitstream profile as frame-indexed preparation.
func CanonicalVideoWindowTimestampArgs(
	profile CanonicalVideoProfileV1,
	quality PreparationQualityPolicy,
	inputPath, outputPath string,
	sourceStartUS, sourceDurationUS, outputFrameCount int64,
) []string {
	startSeconds := strconv.FormatFloat(float64(sourceStartUS)/1_000_000, 'f', 6, 64)
	durationSeconds := strconv.FormatFloat(float64(sourceDurationUS)/1_000_000, 'f', 6, 64)
	filter := fmt.Sprintf("setpts=PTS-STARTPTS,fps=%d/%d,%s", profile.FPSNum, profile.FPSDen, CanonicalVideoScaleGeometryFilter(profile))
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-ss", startSeconds,
		"-i", inputPath, "-t", durationSeconds, "-map", "0:v:0", "-an", "-sn", "-dn", "-vf", filter,
	}
	args = append(args, CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args,
		"-fps_mode", "cfr", "-force_key_frames", "expr:eq(n,0)",
		"-frames:v", strconv.FormatInt(outputFrameCount, 10),
		"-avoid_negative_ts", "make_zero", "-reset_timestamps", "1",
		"-movflags", "+faststart", outputPath,
	)
	return args
}
