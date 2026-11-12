package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
)

// Canonical H.264 encoder settings. These values are part of the stream
// identity because they change the SPS/PPS (avcC) a packet-copy muxer must
// share across every fragment. They are constants, not quality knobs: a
// change requires invalidating prepared fragments and re-certifying the
// producer/worker image. The preset and reference count are pinned explicitly
// instead of relying on libx264 defaults, and the colour metadata is pinned to
// BT.709 so a source carrying different VUI metadata cannot silently produce
// a different SPS.
const (
	CanonicalVideoEncoderPreset      = "veryfast"
	CanonicalVideoEncoderRefFrames   = "1"
	CanonicalVideoColorPrimaries     = "bt709"
	CanonicalVideoColorTransfer      = "bt709"
	CanonicalVideoColorspace         = "bt709"
	CanonicalVideoColorRange         = "tv"
	CanonicalVideoEncoderContractRev = "video-encoder-args.v1"
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
		"-preset", CanonicalVideoEncoderPreset,
		"-refs", CanonicalVideoEncoderRefFrames,
		"-crf", strconv.Itoa(quality.VideoCRF),
		"-maxrate", quality.VideoMaxRate,
		"-bufsize", quality.VideoBufferSize,
		"-pix_fmt", profile.PixelFormat,
		"-profile:v", profile.CodecProfile,
		"-level:v", profile.CodecLevel,
		"-colorspace", CanonicalVideoColorspace,
		"-color_primaries", CanonicalVideoColorPrimaries,
		"-color_trc", CanonicalVideoColorTransfer,
		"-color_range", CanonicalVideoColorRange,
		"-g", strconv.Itoa(profile.GOPSize),
		"-bf", strconv.Itoa(profile.BFrames),
		"-sc_threshold", "0",
		"-x264-params", fmt.Sprintf("scenecut=0:open-gop=%d:keyint=%d:min-keyint=%d", openGOP, profile.GOPSize, profile.GOPSize),
		"-video_track_timescale", strconv.Itoa(profile.TimeBaseDen),
	}
}

// CanonicalVideoEncoderArgsDigest is the stable identity of the exact encoder
// argument projection. It is folded into VideoWindowPreparationRevision so a
// preset/refs/colour change invalidates every cached prepared fragment even
// when the profile struct itself is unchanged.
func CanonicalVideoEncoderArgsDigest(profile CanonicalVideoProfileV1, quality PreparationQualityPolicy) (string, error) {
	if err := profile.Validate(); err != nil {
		return "", err
	}
	if err := quality.Validate(); err != nil {
		return "", err
	}
	contract := struct {
		Version string   `json:"version"`
		Args    []string `json:"args"`
	}{Version: CanonicalVideoEncoderContractRev, Args: CanonicalVideoEncodeArgs(profile, quality)}
	data, err := json.Marshal(contract)
	if err != nil {
		return "", fmt.Errorf("canonical video encoder contract: encode digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// CanonicalVideoScaleGeometryFilter returns the canonical letterbox/pad
// geometry. Keeping this in the shared contract prevents the Master and
// workers from preparing visually or bitstream-incompatible fragments.
//
// `setsar=1` is part of the identity, not cosmetics: scale/pad preserves a
// non-square source SAR and libx264 writes it into the SPS VUI, so an
// anamorphic source (PAL 64:45, DV 4:3, …) emits an avcC that differs from the
// canonical one even though codec, profile, level, geometry and fps all agree.
// The one-frame reference is encoded from a square-pixel synthetic source, so
// without this normalization the worker rejects its own valid output with
// WINDOW_EXTRADATA_IDENTITY_MISMATCH. The Rust cut lane already pins
// `setsar=1` for the same reason; this keeps every producer and every window
// on the same square-pixel identity.
func CanonicalVideoScaleGeometryFilter(profile CanonicalVideoProfileV1) string {
	return fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,setsar=1",
		profile.Width, profile.Height, profile.Width, profile.Height)
}

// CanonicalVideoScaleFilter adds the profile frame rate to the shared
// canonical geometry filter.
func CanonicalVideoScaleFilter(profile CanonicalVideoProfileV1) string {
	return fmt.Sprintf("%s,fps=%d/%d", CanonicalVideoScaleGeometryFilter(profile), profile.FPSNum, profile.FPSDen)
}

// CanonicalVideoNormalizeSourceArgs converts a whole source into the canonical
// stream once. Workers cache this derivation by source SHA-256 and profile, so
// every window of the same immutable source starts from already-normalized
// frames instead of repeating scale/fps conversion per window. The output is
// video-only; window preparation still selects and encodes its exact frame
// interval, because a source may have arbitrary GOP boundaries.
func CanonicalVideoNormalizeSourceArgs(profile CanonicalVideoProfileV1, quality PreparationQualityPolicy, inputPath, outputPath string) []string {
	filter := fmt.Sprintf("setpts=PTS-STARTPTS,fps=%d/%d,%s", profile.FPSNum, profile.FPSDen, CanonicalVideoScaleGeometryFilter(profile))
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-i", inputPath,
		"-map", "0:v:0", "-an", "-sn", "-dn", "-vf", filter,
	}
	args = append(args, CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args,
		"-fps_mode", "cfr",
		"-force_key_frames", "expr:eq(n,0)",
		"-avoid_negative_ts", "make_zero", "-reset_timestamps", "1",
		"-movflags", "+faststart", outputPath,
	)
	return args
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
// producer does not claim a CFR frame index for the original media. Accurate
// seeking stays after -i: fast input seeking can drop the final frame of a
// short window near EOF. Output is still normalized to an exact canonical
// frame count and the same H.264 bitstream profile as frame-indexed preparation.
func CanonicalVideoWindowTimestampArgs(
	profile CanonicalVideoProfileV1,
	quality PreparationQualityPolicy,
	inputPath, outputPath string,
	sourceStartUS, sourceDurationUS, outputFrameCount int64,
) []string {
	startSeconds := strconv.FormatFloat(float64(sourceStartUS)/1_000_000, 'f', 6, 64)
	// The timestamp interval's end is exclusive, and sources can begin a
	// fraction of a frame after the seek point. Extend the decode window by
	// one output frame so fps normalization has enough input to emit the
	// declared final frame. -frames:v below still caps output at the exact
	// canonical count.
	decodeDurationSeconds := float64(sourceDurationUS)/1_000_000 + float64(profile.FPSDen)/float64(profile.FPSNum)
	durationSeconds := strconv.FormatFloat(decodeDurationSeconds, 'f', 6, 64)
	filter := fmt.Sprintf("setpts=PTS-STARTPTS,fps=%d/%d,tpad=stop_mode=clone:stop=1,%s", profile.FPSNum, profile.FPSDen, CanonicalVideoScaleGeometryFilter(profile))
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y", "-i", inputPath,
		"-ss", startSeconds, "-t", durationSeconds, "-map", "0:v:0", "-an", "-sn", "-dn", "-vf", filter,
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
