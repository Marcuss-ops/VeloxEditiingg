package executors

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/internal/runtimeassets"
)

// render_batch_args.go builds the ffmpeg argv for the two render_batch
// phases: the visual (video-only) render and the final stream-copy mux. The
// filter graph is preallocated from the compiled plan so it never grows one
// segment at a time.

// totalVideoSegments counts the ordered video segments across all tracks so
// buildVideoOnlyArgs can preallocate its filter graph instead of growing it
// one append at a time.
func totalVideoSegments(plan *contract.CompiledRenderPlanV2) int {
	total := 0
	for _, track := range plan.VideoTracks {
		total += len(track.Segments)
	}
	return total
}

func buildVideoOnlyArgs(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings, outputPath string) ([]string, error) {
	if plan == nil || len(plan.VideoTracks) == 0 {
		return nil, errors.New("render_batch@1: video_tracks must not be empty")
	}
	duration := float64(plan.DurationUS) / 1_000_000
	args := []string{
		"-f", "lavfi", "-i",
		fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=%.6f", plan.Output.Width, plan.Output.Height, plan.Output.FPSNum, plan.Output.FPSDen, duration),
	}
	// Preallocate for the unique asset count (upper bound) and for the filter
	// graph: two filter strings per segment plus the final format filter.
	inputIndex := make(map[string]int, len(plan.Assets))
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			if _, exists := inputIndex[segment.AssetID]; exists {
				continue
			}
			inputIndex[segment.AssetID] = len(inputIndex) + 1
			args = append(args, "-i", bindings[segment.AssetID].Path)
		}
	}

	// Two filter strings per segment plus the final format filter, written in
	// one pass into a single builder. The overlay's input label is derived from
	// the previous overlay index ([0:v] for the first segment) instead of a base
	// string variable, and segment/overlay labels are written directly from the
	// integer index.
	var filter strings.Builder
	filter.Grow(totalVideoSegments(plan)*300 + 64)

	width := plan.Output.Width
	height := plan.Output.Height
	segmentIndex := 0
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			input := inputIndex[segment.AssetID]
			start := float64(segment.TimelineStartFrame*int64(plan.Output.FPSDen)) / float64(plan.Output.FPSNum)
			sourceIn := float64(segment.SourceInUS) / 1_000_000
			sourceDuration := float64(segment.SourceDurationUS) / 1_000_000
			frameDuration := float64(segment.FrameCount*int64(plan.Output.FPSDen)) / float64(plan.Output.FPSNum)
			if math.Abs(frameDuration-sourceDuration) > 1.0/float64(plan.Output.FPSNum) {
				return nil, fmt.Errorf("segment %q source_duration_us=%d does not match frame_count=%d at %d/%d fps", segment.SegmentID, segment.SourceDurationUS, segment.FrameCount, plan.Output.FPSNum, plan.Output.FPSDen)
			}

			if filter.Len() > 0 {
				filter.WriteByte(';')
			}
			filter.WriteByte('[')
			writeInt(&filter, input)
			filter.WriteString(":v]trim=start=")
			writeFloat6(&filter, sourceIn)
			filter.WriteString(":duration=")
			writeFloat6(&filter, sourceDuration)
			filter.WriteString(",setpts=PTS-STARTPTS+")
			writeFloat6(&filter, start)
			filter.WriteString("/TB,scale=")
			writeInt(&filter, width)
			filter.WriteByte(':')
			writeInt(&filter, height)
			filter.WriteString(":force_original_aspect_ratio=decrease,pad=")
			writeInt(&filter, width)
			filter.WriteByte(':')
			writeInt(&filter, height)
			filter.WriteString(":(ow-iw)/2:(oh-ih)/2:color=black[batch_segment_")
			writeInt(&filter, segmentIndex)
			filter.WriteByte(']')

			filter.WriteByte(';')
			if segmentIndex == 0 {
				filter.WriteString("[0:v]")
			} else {
				filter.WriteString("[batch_overlay_")
				writeInt(&filter, segmentIndex-1)
				filter.WriteByte(']')
			}
			filter.WriteString("[batch_segment_")
			writeInt(&filter, segmentIndex)
			filter.WriteString("]overlay=eof_action=pass:shortest=0[batch_overlay_")
			writeInt(&filter, segmentIndex)
			filter.WriteByte(']')
			segmentIndex++
		}
	}

	if filter.Len() > 0 {
		filter.WriteByte(';')
	}
	if segmentIndex == 0 {
		filter.WriteString("[0:v]")
	} else {
		filter.WriteString("[batch_overlay_")
		writeInt(&filter, segmentIndex-1)
		filter.WriteByte(']')
	}
	filter.WriteString("format=yuv420p[vout]")

	pixelFormat := plan.Output.PixelFormat
	if pixelFormat == "" {
		pixelFormat = "yuv420p"
	}
	args = append(args,
		"-filter_complex", filter.String(),
		"-map", "[vout]", "-an",
		"-c:v", plan.Output.VideoCodec,
		"-pix_fmt", pixelFormat,
		"-r", strconv.Itoa(plan.Output.FPSNum)+"/"+strconv.Itoa(plan.Output.FPSDen),
		"-t", fmt.Sprintf("%.6f", duration),
		"-y", outputPath,
	)
	return args, nil
}

func faststartEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_FASTSTART")))
	return v == "1" || v == "true" || v == "yes"
}

func faststartArgs() []string {
	if faststartEnabled() {
		return []string{"-movflags", "+faststart"}
	}
	return nil
}

func buildFinalAudioCopyArgs(videoOnlyPath, audioPath, outputPath string) []string {
	args := []string{
		"-i", videoOnlyPath,
		"-i", audioPath,
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-c:v", "copy",
		"-c:a", "copy",
	}
	args = append(args, faststartArgs()...)
	args = append(args, "-y", outputPath)
	return args
}

// writeConcatList creates the only input consumed by the strict video
// packet-copy pass. Every entry is a complete, already-trimmed clip. Partial
// trims, filters, scaling and gaps are deliberately handled by the validator
// as incompatible rather than silently falling back to a video encode.
func writeConcatList(path string, videoPaths []string) error {
	if strings.TrimSpace(path) == "" || len(videoPaths) == 0 {
		return errors.New("render_batch@1: concat list requires a path and at least one video")
	}
	var content strings.Builder
	for _, videoPath := range videoPaths {
		if strings.TrimSpace(videoPath) == "" {
			return errors.New("render_batch@1: concat list contains an empty path")
		}
		// The concat demuxer uses single-quoted paths. Escape the only
		// metacharacter that can terminate that quoted value.
		content.WriteString("file '")
		content.WriteString(strings.ReplaceAll(videoPath, "'", "'\\''"))
		content.WriteString("'\n")
	}
	return os.WriteFile(path, []byte(content.String()), 0o640)
}

func buildVideoOnlyPacketCopyArgs(concatListPath, outputPath string) []string {
	args := []string{"-f", "concat", "-safe", "0", "-i", concatListPath, "-map", "0:v:0", "-an", "-c:v", "copy"}
	args = append(args, faststartArgs()...)
	args = append(args, "-y", outputPath)
	return args
}
