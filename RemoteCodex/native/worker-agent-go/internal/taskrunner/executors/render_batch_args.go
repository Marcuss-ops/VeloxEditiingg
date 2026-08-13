package executors

import (
	"errors"
	"fmt"
	"math"
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

	filters := make([]string, 0, totalVideoSegments(plan)*2+1)
	base := "[0:v]"
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
			segmentLabel := fmt.Sprintf("[batch_segment_%d]", segmentIndex)
			filters = append(filters, fmt.Sprintf("[%d:v]trim=start=%.6f:duration=%.6f,setpts=PTS-STARTPTS+%.6f/TB,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2:color=black%s", input, sourceIn, sourceDuration, start, plan.Output.Width, plan.Output.Height, plan.Output.Width, plan.Output.Height, segmentLabel))
			overlayOutput := fmt.Sprintf("[batch_overlay_%d]", segmentIndex)
			filters = append(filters, fmt.Sprintf("%s%soverlay=eof_action=pass:shortest=0%s", base, segmentLabel, overlayOutput))
			base = overlayOutput
			segmentIndex++
		}
	}
	filters = append(filters, base+"format=yuv420p[vout]")

	pixelFormat := plan.Output.PixelFormat
	if pixelFormat == "" {
		pixelFormat = "yuv420p"
	}
	args = append(args,
		"-filter_complex", strings.Join(filters, ";"),
		"-map", "[vout]", "-an",
		"-c:v", plan.Output.VideoCodec,
		"-pix_fmt", pixelFormat,
		"-r", strconv.Itoa(plan.Output.FPSNum)+"/"+strconv.Itoa(plan.Output.FPSDen),
		"-t", fmt.Sprintf("%.6f", duration),
		"-y", outputPath,
	)
	return args, nil
}

func buildFinalAudioCopyArgs(videoOnlyPath, audioPath, outputPath string) []string {
	return []string{
		"-i", videoOnlyPath,
		"-i", audioPath,
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-c:v", "copy",
		"-c:a", "copy",
		"-movflags", "+faststart",
		"-y", outputPath,
	}
}