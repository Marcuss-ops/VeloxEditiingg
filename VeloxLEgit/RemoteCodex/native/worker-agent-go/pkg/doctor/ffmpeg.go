package doctor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"velox-worker-agent/pkg/config"
)

// FFmpegValidator checks that ffmpeg and ffprobe are available and
// ffprobe reports a major version >= 4.
// RW-PROD-002 §2 item 9.
type FFmpegValidator struct{}

func (v *FFmpegValidator) ID() string { return "tools.ffmpeg" }

func (v *FFmpegValidator) Run(ctx context.Context, _ *config.WorkerConfig) Result {
	var failures []string

	// Check ffmpeg.
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		failures = append(failures, fmt.Sprintf("ffmpeg: %v", err))
	} else {
		// Quick smoke test: encode a 1-frame null output.
		ffCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ffCtx, ffmpegPath,
			"-f", "lavfi", "-i", "color=c=black:s=64x64", "-frames:v", "1", "-f", "null", "-")
		if out, err := cmd.CombinedOutput(); err != nil {
			failures = append(failures, fmt.Sprintf("ffmpeg smoke test failed: %v (output: %s)", err, trim(string(out))))
		}
	}

	// Check ffprobe.
	ffprobePath, err := exec.LookPath("ffprobe")
	if err != nil {
		failures = append(failures, fmt.Sprintf("ffprobe: %v", err))
	} else {
		prCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(prCtx, ffprobePath, "-version")
		out, err := cmd.CombinedOutput()
		if err != nil {
			failures = append(failures, fmt.Sprintf("ffprobe -version failed: %v", err))
		} else {
			// Parse major version. Output is like: "ffprobe version 4.4.2 ..."
			versionLine := strings.SplitN(string(out), "\n", 2)[0]
			var major int
			if _, err := fmt.Sscanf(versionLine, "ffprobe version %d", &major); err != nil || major < 4 {
				failures = append(failures, fmt.Sprintf("ffprobe version %q: major=%d (need >=4)", versionLine, major))
			}
		}
	}

	if len(failures) > 0 {
		detail := ""
		for i, f := range failures {
			if i > 0 {
				detail += "; "
			}
			detail += f
		}
		return fail("tools.ffmpeg", detail, "install ffmpeg and ffprobe (version >= 4) on the worker host")
	}

	return pass("tools.ffmpeg", "ffmpeg + ffprobe available")
}
