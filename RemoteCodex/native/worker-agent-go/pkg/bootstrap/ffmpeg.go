// pkg/bootstrap/ffmpeg.go — ffmpeg/ffprobe self-test (RW-PROD-003 §3 A3).
//
// A worker package can drift silently: docker layers sometimes
// shadow the real ffmpeg with a stub script that succeeds the
// shell-exec test but produces nonsense output. The bootstrap
// verifies:
//
//  1. Both `ffprobe` and `ffmpeg` are reachable via PATH (exec.LookPath).
//  2. `ffprobe -version` reports a major version ≥ opts.FFmpegMinMajor
//     (RW-PROD-003 default 4). Anything older is fragile on libx264
//     arg surfaces.
//  3. `ffmpeg -h encoder=libx264` lists `libx264` among its encoders —
//     if not, video export will silently fail at runtime. This is the
//     cheapest reproducible check available without an actual encode.
//
// All errors carry one of four stable codes:
//
//   - tools.ffprobe_missing (binary not in PATH)
//   - tools.ffmpeg_missing (binary not in PATH)
//   - tools.ffprobe_version_low (binary present but major < min)
//   - tools.ffmpeg_x264_unsupported (ffmpeg present but no libx264 encoder)
//
// The ffmpeg/ffprobe checks are the only place Run() duplicates real
// production deps outside of pkg/video (which has the C++ engine).
package bootstrap

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// runFFmpegSelfTest (RW-PROD-003 A3). ctx doubles as the proc-timeout
// source; tests can apply their own shorter deadlines.
func runFFmpegSelfTest(ctx context.Context, opts Options) StepResult {
	start := time.Now().UTC()
	res := StepResult{
		Name:      "ffmpeg",
		StartedAt: start,
	}

	for _, bin := range FFmpegBinaries {
		if _, err := exec.LookPath(bin); err != nil {
			res.Status = "FAIL"
			switch bin {
			case "ffprobe":
				res.Code = "tools.ffprobe_missing"
			case "ffmpeg":
				res.Code = "tools.ffmpeg_missing"
			default:
				res.Code = "tools." + bin + "_missing"
			}
			res.Detail = fmt.Sprintf("exec.LookPath(%q): %v", bin, err)
			res.CompletedAt = time.Now().UTC()
			res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
			return res
		}
	}

	// ffprobe -version: parse "ffprobe version X.Y.Z ..." → major X.
	if major, err := ffprobeMajorVersion(ctx); err != nil {
		res.Status = "FAIL"
		res.Code = "tools.ffprobe_version_unparseable"
		res.Detail = err.Error()
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	} else if major < opts.FFmpegMinMajor {
		res.Status = "FAIL"
		res.Code = "tools.ffprobe_version_low"
		res.Detail = fmt.Sprintf("ffprobe major=%d < required major=%d (RW-PROD-003 A3)", major, opts.FFmpegMinMajor)
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	// ffmpeg -h encoder=libx264 — cheap reproduce-check of the encoder surface.
	if err := ffmpegHasLibx264(ctx); err != nil {
		res.Status = "FAIL"
		res.Code = "tools.ffmpeg_x264_unsupported"
		res.Detail = err.Error()
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	res.Status = "OK"
	res.Code = "tools.ok"
	res.Detail = fmt.Sprintf("ffprobe major≥%d ffmpeg -h encoder=libx264 OK (min=%d)", opts.FFmpegMinMajor, opts.FFmpegMinMajor)
	res.CompletedAt = time.Now().UTC()
	res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
	return res
}

// ffprobeVersionRE matches the first line of `ffprobe -version`:
//
//	ffprobe version 4.4.2-0ubuntu0.22.04.1 Copyright ...
var ffprobeVersionRE = regexp.MustCompile(`(?i)ffprobe version\s+(\d+)(?:\.(\d+))?`)

// ffmpegEncoderHelpRE matches the section header of `ffmpeg -h
// encoder=libx264` output: it lists "Encoder libx264 [libx264 H.264 ...".
// We keep the lookup tolerant of any whitespace / punctuation.
var ffmpegEncoderHelpRE = regexp.MustCompile(`(?im)\blibx264\b`)

// ffprobeMajorVersion runs `ffprobe -version` and returns the parsed
// major version. The first numeric token after "ffprobe version" wins.
func ffprobeMajorVersion(ctx context.Context) (int, error) {
	cmd := exec.CommandContext(ctx, "ffprobe", "-version")
	out, err := cmd.Output()
	if err != nil {
		// ExitErr may carry stderr; surface whatever the inner error
		// exposes so dashboarding can grep it.
		return 0, fmt.Errorf("ffprobe -version failed: %w", err)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		m := ffprobeVersionRE.FindStringSubmatch(scanner.Text())
		if len(m) >= 2 {
			maj, parseErr := strconv.Atoi(m[1])
			if parseErr != nil {
				return 0, fmt.Errorf("ffprobe major version unparseable: %q", m[1])
			}
			return maj, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("ffprobe -version: scan failed: %w", err)
	}
	return 0, fmt.Errorf("ffprobe -version: no version line matched %q", ffprobeVersionRE.String())
}

// ffmpegHasLibx264 runs `ffmpeg -h encoder=libx264` and returns nil if
// the encoder is listed. The output format is permissive across ffmpeg
// 4.x / 5.x / 6.x; we look for the substring `libx264` on any line and
// (belt + suspenders) for the presence of an encoders section header.
func ffmpegHasLibx264(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "ffmpeg", "-h", "encoder=libx264")
	out, err := cmd.CombinedOutput()
	combined := string(out)
	if err != nil {
		// ffmpeg returns non-zero when an encoder is missing entirely;
		// both cases funnel into the same code so dashboards only have
		// to alert on a single signal.
		return fmt.Errorf("ffmpeg -h encoder=libx264 (exit=%v): %s", err, trimForLog(combined))
	}
	if !ffmpegEncoderHelpRE.MatchString(combined) {
		return fmt.Errorf("ffmpeg -h encoder=libx264 produced no libx264 reference: %s", trimForLog(combined))
	}
	return nil
}

// trimForLog trims ffmpeg's sometimes-rambling help output to one line
// for stable log emission. Operators can re-run the command from the
// error code if they need the full text.
func trimForLog(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 240 {
		return s[:240] + "…"
	}
	return s
}
