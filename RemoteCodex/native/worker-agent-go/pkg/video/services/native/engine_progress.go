package native

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"

	"velox-worker-agent/pkg/video/pipeline"
)

// streamEngineOutput starts two goroutines (stderr + stdout readers) and
// forwards structured native progress through the task-local pipeline callback.
func streamEngineOutput(stdout, stderr io.ReadCloser, ctx context.Context, onProgress DetailedProgressFunc, legacyProgress ProgressFunc, stderrBuf, stdoutBuf *strings.Builder) chan struct{} {
	progressDone := make(chan struct{})

	stderrReader := bufio.NewReader(stderr)
	go func() {
		defer close(progressDone)
		for {
			line, err := stderrReader.ReadString('\n')
			if len(line) > 0 {
				line = strings.TrimRight(line, "\n\r")
				stderrBuf.WriteString(line)
				stderrBuf.WriteString("\n")

				var prog struct {
					Percent          int     `json:"percent"`
					Scene            int     `json:"scene"`
					Total            int     `json:"total_scenes"`
					Segment          int     `json:"segment"`
					TotalSegments    int     `json:"total_segments"`
					SegmentCompleted bool    `json:"segment_completed"`
					Stage            string  `json:"stage"`
					Phase            string  `json:"phase"`
					FramesEncoded    int64   `json:"frames_encoded"`
					FramesDecoded    int64   `json:"frames_decoded"`
					FramesComposited int64   `json:"frames_composited"`
					FfmpegSpeedX     float64 `json:"speed_x"`
					ElapsedMS        int64   `json:"elapsed_ms"`
				}
				if json.Unmarshal([]byte(line), &prog) == nil && prog.Percent >= 0 {
					// Legacy lifecycle lines contain only percent/stage. Do
					// not route them through the detailed callback: replacing
					// a live snapshot with zero scene/segment/frame values
					// would corrupt the canonical Attempt projection.
					detailed := prog.Phase != "" || prog.Scene != 0 || prog.Total != 0 ||
						prog.Segment != 0 || prog.TotalSegments != 0 || prog.SegmentCompleted ||
						prog.FramesEncoded != 0 || prog.FramesDecoded != 0 ||
						prog.FramesComposited != 0 || prog.ElapsedMS != 0
					if !detailed {
						if legacy := pipeline.ProgressCallback(ctx); legacy != nil {
							legacy(prog.Percent, prog.Scene, prog.Total, prog.Stage)
						} else if legacyProgress != nil {
							legacyProgress(prog.Percent, prog.Scene, prog.Total, prog.Stage)
						}
						continue
					}
					phase := prog.Phase
					if phase == "" {
						phase = prog.Stage
					}
					metrics := map[string]float64{
						"frames_encoded":    float64(prog.FramesEncoded),
						"frames_decoded":    float64(prog.FramesDecoded),
						"frames_composited": float64(prog.FramesComposited),
						"ffmpeg_speed_x":    prog.FfmpegSpeedX,
						"elapsed_ms":        float64(prog.ElapsedMS),
					}
					snapshot := pipeline.ProgressSnapshot{
						Percent:           int32(prog.Percent),
						Scene:             int32(prog.Scene),
						TotalScenes:       int32(prog.Total),
						Segment:           int32(prog.Segment),
						TotalSegments:     int32(prog.TotalSegments),
						SegmentCompleted:  prog.SegmentCompleted,
						Phase:             phase,
						FramesEncoded:     prog.FramesEncoded,
						FramesDecoded:     prog.FramesDecoded,
						FramesComposited:  prog.FramesComposited,
						FfmpegSpeedX:      prog.FfmpegSpeedX,
						ElapsedMS:         prog.ElapsedMS,
						CumulativeMetrics: metrics,
					}
					if legacy := pipeline.ProgressCallback(ctx); legacy != nil {
						legacy(int(snapshot.Percent), int(snapshot.Scene), int(snapshot.TotalScenes), snapshot.Phase)
					} else if legacyProgress != nil {
						legacyProgress(int(snapshot.Percent), int(snapshot.Scene), int(snapshot.TotalScenes), snapshot.Phase)
					}
					if fn := pipeline.DetailedProgressCallback(ctx); fn != nil {
						fn(snapshot)
					} else if onProgress != nil {
						onProgress(snapshot)
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	stdoutReader := bufio.NewReader(stdout)
	go func() {
		for {
			line, err := stdoutReader.ReadString('\n')
			if len(line) > 0 {
				stdoutBuf.WriteString(line)
			}
			if err != nil {
				break
			}
		}
	}()

	return progressDone
}
