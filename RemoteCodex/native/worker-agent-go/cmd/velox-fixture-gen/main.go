package main

// velox-fixture-gen builds the COPY_ONLY_CANONICAL_5M_V1 "Formula 1
// test track" (plan §14): 24 H264 1920x1080 CFR clips + one AAC 48 kHz
// stereo final audio track, zero filters/subtitles/transformations,
// warm cache. It generates the assets with ffmpeg from the canonical
// spec (deterministic per-clip variation, byte-reproducible for a given
// encoder build), self-verifies every clip with ffprobe, and writes a
// machine-readable manifest (per-asset SHA-256) that must match the
// spec digest pinned on the fixture.
//
// Usage:
//
//	velox-fixture-gen -out-dir DIR [-verify-manifest PATH] [-print-spec-sha]
//
//	-out-dir DIR            generate the full track into DIR (default:
//	                        temp dir; files are never written into the
//	                        repo — see docs/gitignore-policy.md)
//	-verify-manifest PATH   load a previously generated manifest and
//	                        verify it against the canonical spec
//	-print-spec-sha         print the pinned spec digest and exit
//
// Exit codes (mirror velox-fixture-gate):
//	0 pass / 1 verification failed / 2 usage error

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"velox-worker-agent/pkg/performance"
)

const fixtureID = performance.FixtureCopyOnlyCanonical5MV1

func failUsage(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-fixture-gen][ERROR] "+format+"\n", args...)
	os.Exit(2)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[velox-fixture-gen][FAIL] "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	outDir := flag.String("out-dir", "", "directory to generate the track into")
	verifyManifest := flag.String("verify-manifest", "", "verify a generated manifest against the canonical spec")
	printSpecSHA := flag.Bool("print-spec-sha", false, "print the pinned spec digest and exit")
	flag.Parse()

	spec := performance.CanonicalFixtureSpecV1()
	if flag.NArg() > 0 {
		failUsage("unexpected arguments: %v", flag.Args())
	}

	if *printSpecSHA {
		fmt.Println(spec.SpecSHA256())
		return
	}
	if *verifyManifest != "" {
		m, err := performance.LoadFixtureManifest(*verifyManifest)
		if err != nil {
			fail("%v", err)
		}
		reg := performance.NewBenchmarkFixtureRegistry()
		fixture, ok := reg.Fixture(fixtureID)
		if !ok {
			fail("fixture %s not registered", fixtureID)
		}
		problems := performance.ValidateManifest(*m, fixture, spec)
		if len(problems) > 0 {
			for _, p := range problems {
				fmt.Fprintf(os.Stderr, "  - %s\n", p)
			}
			fail("manifest %s does not match the canonical track", *verifyManifest)
		}
		fmt.Printf("[velox-fixture-gen][PASS] manifest %s matches the canonical spec (%s)\n", *verifyManifest, spec.SpecSHA256())
		return
	}
	for _, tool := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(tool); err != nil {
			failUsage("missing dependency: %s", tool)
		}
	}
	if *outDir == "" {
		dir, err := os.MkdirTemp("", "velox-canonical-track.*")
		if err != nil {
			fail("create temp dir: %v", err)
		}
		outDir = &dir
		fmt.Fprintf(os.Stderr, "[velox-fixture-gen] generated into %s\n", *outDir)
	} else if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fail("create out dir: %v", err)
	}

	start := time.Now()
	manifest := performance.FixtureManifest{
		FixtureID:     fixtureID,
		SpecSHA256:    spec.SpecSHA256(),
		FFmpegVersion: ffmpegVersion(),
		GeneratedAt:   time.Now().UTC(),
	}

	for i, clip := range spec.Clips {
		name := fmt.Sprintf("clip_%03d.mp4", i+1)
		path := filepath.Join(*outDir, name)
		if err := generateClip(path, clip, spec); err != nil {
			fail("generate %s: %v", name, err)
		}
		info, err := probeClip(path, spec)
		if err != nil {
			fail("verify %s: %v", name, err)
		}
		manifest.Clips = append(manifest.Clips, performance.ManifestAsset{
			Name: name, SHA256: fileSHA256(path), SizeBytes: info.size,
			DurationSec: spec.ClipDurationSec(), Frames: info.frames,
		})
		fmt.Fprintf(os.Stderr, "[velox-fixture-gen] %s ok (frames=%d, %d bytes)\n", name, info.frames, info.size)
	}

	finalPath := filepath.Join(*outDir, "final_audio.m4a")
	if err := generateFinalAudio(finalPath, spec); err != nil {
		fail("generate final audio: %v", err)
	}
	manifest.FinalAudio = performance.ManifestAsset{
		Name: "final_audio.m4a", SHA256: fileSHA256(finalPath),
		SizeBytes: fileSize(finalPath), DurationSec: float64(spec.DurationSec), Frames: spec.TotalFrames(),
	}

	manifestPath := filepath.Join(*outDir, "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fail("marshal manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o644); err != nil {
		fail("write manifest: %v", err)
	}

	fmt.Printf("[velox-fixture-gen][PASS] track built in %s: %d clips + final audio, spec %s\n",
		time.Since(start).Round(time.Millisecond), len(manifest.Clips), spec.SpecSHA256())
}

type clipProbe struct {
	frames int
	size   int64
}

// generateClip renders one canonical clip: solid-color CFR video + sine
// audio, H264 1920x1080 yuv420p at the canonical timescale, AAC 48 kHz
// stereo. -threads 1 keeps the encode byte-reproducible for a given
// encoder build.
func generateClip(path string, clip performance.CanonicalClipSpec, spec performance.CanonicalFixtureSpec) error {
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", fmt.Sprintf("color=c=0x%06X:s=%dx%d:r=%d", clip.ColorRGB, spec.Video.Width, spec.Video.Height, spec.Video.FPS),
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=%d", clip.AudioFrequencyHz, spec.Audio.SampleRate),
		"-frames:v", fmt.Sprintf("%d", clip.Frames),
		"-r", fmt.Sprintf("%d", spec.Video.FPS),
		"-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
		"-pix_fmt", spec.Video.PixelFormat, "-profile:v", "high", "-level", "4.0",
		"-video_track_timescale", "15360",
		"-c:a", spec.Audio.Codec, "-ar", fmt.Sprintf("%d", spec.Audio.SampleRate), "-ac", fmt.Sprintf("%d", spec.Audio.Channels),
		"-threads", "1",
		"-shortest",
		path,
	}
	cmd := exec.Command("ffmpeg", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// generateFinalAudio renders the 300 s AAC 48 kHz stereo final track
// (sine 440 Hz, deterministic).
func generateFinalAudio(path string, spec performance.CanonicalFixtureSpec) error {
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
		"-t", fmt.Sprintf("%d", spec.DurationSec),
		"-c:a", spec.Audio.Codec, "-ar", fmt.Sprintf("%d", spec.Audio.SampleRate), "-ac", fmt.Sprintf("%d", spec.Audio.Channels),
		path,
	}
	cmd := exec.Command("ffmpeg", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg final audio: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// probeClip validates the generated clip with ffprobe: H264 1920x1080,
// CFR frame count, AAC 48 kHz stereo. It fails the build on any spec
// violation — generation self-verifies instead of trusting ffmpeg.
func probeClip(path string, spec performance.CanonicalFixtureSpec) (*clipProbe, error) {
	out, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0",
		"-show_entries", "stream=codec_name,width,height,r_frame_rate,nb_frames",
		"-of", "json", path).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("ffprobe: %v", err)
	}
	var probe struct {
		Streams []struct {
			CodecName  string `json:"codec_name"`
			Width      int    `json:"width"`
			Height     int    `json:"height"`
			RFrameRate string `json:"r_frame_rate"`
			NbFrames   string `json:"nb_frames"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return nil, fmt.Errorf("parse ffprobe: %v", err)
	}
	if len(probe.Streams) == 0 {
		return nil, fmt.Errorf("no video stream")
	}
	s := probe.Streams[0]
	if s.CodecName != spec.Video.Codec || s.Width != spec.Video.Width || s.Height != spec.Video.Height {
		return nil, fmt.Errorf("video %s %dx%d != spec %s %dx%d", s.CodecName, s.Width, s.Height, spec.Video.Codec, spec.Video.Width, spec.Video.Height)
	}
	if s.RFrameRate != fmt.Sprintf("%d/1", spec.Video.FPS) {
		return nil, fmt.Errorf("frame rate %s != CFR %d/1", s.RFrameRate, spec.Video.FPS)
	}
	frames, err := parseFrameCount(s.NbFrames)
	if err != nil {
		return nil, fmt.Errorf("frame count unknown (nb_frames=%q): %v", s.NbFrames, err)
	}
	if frames != spec.PerClipFrames {
		return nil, fmt.Errorf("frames %d != spec %d (CFR-exact)", frames, spec.PerClipFrames)
	}
	return &clipProbe{frames: frames, size: fileSize(path)}, nil
}

// parseFrameCount converts the ffprobe nb_frames string to an int,
// failing loudly on "N/A" / empty / malformed values instead of
// reporting a fake frame-count mismatch.
func parseFrameCount(raw string) (int, error) {
	if raw == "" || raw == "N/A" {
		return 0, fmt.Errorf("container reports no frame count")
	}
	var frames int
	if _, err := fmt.Sscanf(raw, "%d", &frames); err != nil || frames <= 0 {
		return 0, fmt.Errorf("unparseable frame count %q", raw)
	}
	return frames, nil
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
