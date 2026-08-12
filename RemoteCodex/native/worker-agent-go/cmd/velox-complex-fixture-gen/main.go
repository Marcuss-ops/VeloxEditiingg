package main

// velox-complex-fixture-gen builds COMPLEX_CANONICAL_5M_V1: 24 mixed-size
// H264 source clips plus 14 deterministic AAC tracks. The generated manifest
// is the only runtime input needed by the complex benchmark renderer.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-worker-agent/pkg/performance"
)

const fixtureID = performance.FixtureComplexCanonical5MV1

func main() {
	outDir := flag.String("out-dir", "", "directory to generate the complex track into")
	verify := flag.String("verify-manifest", "", "verify a generated complex manifest")
	printSHA := flag.Bool("print-spec-sha", false, "print the pinned complex spec digest and exit")
	flag.Parse()
	if flag.NArg() > 0 {
		failUsage("unexpected arguments: %v", flag.Args())
	}
	spec := performance.ComplexCanonicalFixtureSpecV1()
	if *printSHA {
		fmt.Println(spec.SpecSHA256())
		return
	}
	if *verify != "" {
		manifest, err := performance.LoadComplexFixtureManifest(*verify)
		if err != nil {
			fail("%v", err)
		}
		fixture, ok := performance.NewBenchmarkFixtureRegistry().Fixture(fixtureID)
		if !ok {
			fail("fixture %s is not registered", fixtureID)
		}
		if problems := performance.ValidateComplexManifest(*manifest, fixture, spec); len(problems) > 0 {
			for _, problem := range problems {
				fmt.Fprintf(os.Stderr, "  - %s\n", problem)
			}
			fail("manifest does not match %s", fixtureID)
		}
		fmt.Printf("[velox-complex-fixture-gen][PASS] manifest matches %s (%s)\n", fixtureID, spec.SpecSHA256())
		return
	}
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			failUsage("missing dependency: %s", tool)
		}
	}
	if *outDir == "" {
		dir, err := os.MkdirTemp("", "velox-complex-track.*")
		if err != nil {
			fail("create temp dir: %v", err)
		}
		*outDir = dir
		fmt.Fprintf(os.Stderr, "[velox-complex-fixture-gen] generated into %s\n", *outDir)
	} else if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fail("create out dir: %v", err)
	}

	start := time.Now()
	manifest := performance.ComplexFixtureManifest{
		FixtureID: fixtureID, SpecSHA256: spec.SpecSHA256(), FFmpegVersion: ffmpegVersion(),
	}
	for _, clip := range spec.ClipSources {
		name := fmt.Sprintf("clip_%03d.mp4", clip.Index)
		path := filepath.Join(*outDir, name)
		if err := generateClip(path, clip, spec); err != nil {
			fail("generate %s: %v", name, err)
		}
		info, err := probeClip(path)
		if err != nil {
			fail("verify %s: %v", name, err)
		}
		if info.Frames != clip.Frames || info.Width != clip.Width || info.Height != clip.Height {
			fail("verify %s: got %dx%d/%d frames, want %dx%d/%d", name, info.Width, info.Height, info.Frames, clip.Width, clip.Height, clip.Frames)
		}
		manifest.Clips = append(manifest.Clips, performance.ManifestAsset{
			Name: name, SHA256: fileSHA256(path), SizeBytes: fileSize(path),
			DurationSec: float64(spec.PerClipFrames) / float64(spec.Output.FPS), Frames: info.Frames,
		})
		fmt.Fprintf(os.Stderr, "[velox-complex-fixture-gen] %s ok (%dx%d, frames=%d)\n", name, info.Width, info.Height, info.Frames)
	}
	for _, audio := range spec.AudioTracks {
		name := fmt.Sprintf("audio_%03d.m4a", audio.Index)
		path := filepath.Join(*outDir, name)
		if err := generateAudio(path, audio.Frequency, audio.Duration, spec); err != nil {
			fail("generate %s: %v", name, err)
		}
		manifest.AudioTracks = append(manifest.AudioTracks, performance.ManifestAsset{
			Name: name, SHA256: fileSHA256(path), SizeBytes: fileSize(path), DurationSec: float64(audio.Duration),
		})
		fmt.Fprintf(os.Stderr, "[velox-complex-fixture-gen] %s ok\n", name)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fail("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(*outDir, performance.ComplexFixtureManifestName), append(data, '\n'), 0o644); err != nil {
		fail("write manifest: %v", err)
	}
	fmt.Printf("[velox-complex-fixture-gen][PASS] track built in %s: %d clips + %d audio tracks, spec %s\n", time.Since(start).Round(time.Millisecond), len(manifest.Clips), len(manifest.AudioTracks), spec.SpecSHA256())
}

type clipProbe struct {
	Width  int
	Height int
	Frames int
}

func generateClip(path string, clip performance.ComplexClipSpec, spec performance.ComplexCanonicalFixtureSpec) error {
	args := []string{"-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
		"-i", fmt.Sprintf("color=c=0x%06X:s=%dx%d:r=%d", clip.ColorRGB, clip.Width, clip.Height, spec.Output.FPS),
		"-frames:v", strconv.Itoa(clip.Frames), "-r", strconv.Itoa(spec.Output.FPS),
		"-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", spec.Output.PixelFormat,
		"-profile:v", "high", "-level", "4.0", "-video_track_timescale", "15360", "-an", "-threads", "1", path}
	return runFFmpeg(args)
}

func generateAudio(path string, frequency, duration int, spec performance.ComplexCanonicalFixtureSpec) error {
	args := []string{"-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
		"-i", fmt.Sprintf("sine=frequency=%d:sample_rate=%d", frequency, spec.Audio.SampleRate),
		"-t", strconv.Itoa(duration), "-c:a", spec.Audio.Codec, "-ar", strconv.Itoa(spec.Audio.SampleRate),
		"-ac", strconv.Itoa(spec.Audio.Channels), "-b:a", "128k", "-threads", "1", path}
	return runFFmpeg(args)
}

func probeClip(path string) (clipProbe, error) {
	out, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,nb_frames", "-of", "csv=p=0", path).CombinedOutput()
	if err != nil {
		return clipProbe{}, fmt.Errorf("ffprobe: %v: %s", err, strings.TrimSpace(string(out)))
	}
	var result clipProbe
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d,%d,%d", &result.Width, &result.Height, &result.Frames); err != nil {
		return clipProbe{}, fmt.Errorf("parse ffprobe output %q: %v", strings.TrimSpace(string(out)), err)
	}
	return result, nil
}

func runFFmpeg(args []string) error {
	if out, err := exec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func ffmpegVersion() string {
	out, err := exec.Command("ffmpeg", "-version").CombinedOutput()
	if err != nil {
		return "unknown"
	}
	return strings.SplitN(string(out), "\n", 2)[0]
}

func fileSHA256(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func failUsage(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-complex-fixture-gen][ERROR] "+format+"\n", args...)
	os.Exit(2)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-complex-fixture-gen][FAIL] "+format+"\n", args...)
	os.Exit(1)
}
