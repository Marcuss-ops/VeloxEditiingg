package assets

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	videoContract "velox-shared/contract"
)

type recordingVideoRunner struct {
	probeOutput []byte
	commands    []videoCommand
}

type videoCommand struct {
	name string
	args []string
}

func (r *recordingVideoRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.commands = append(r.commands, videoCommand{name: name, args: append([]string(nil), args...)})
	if name == "ffprobe" {
		return r.probeOutput, nil
	}
	if len(args) == 0 {
		return nil, nil
	}
	outputPath := args[len(args)-1]
	if err := os.WriteFile(outputPath, []byte("trimmed video"), 0o644); err != nil {
		return nil, err
	}
	return nil, nil
}

func TestVideoTrimmerPlanUsesStreamCopyForNormalizedKeyframeSegment(t *testing.T) {
	runner := &recordingVideoRunner{}
	trimmer := newVideoTrimmerForTest(runner)
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})

	plan, err := trimmer.Plan(probe, VideoSegment{StartSeconds: 2, EndSeconds: 6}, "/source.mp4", "/segment.mp4")
	if err != nil {
		t.Fatalf("Plan() error: %v", err)
	}
	if plan.Mode != TrimModeStreamCopy {
		t.Fatalf("mode = %q, want %q", plan.Mode, TrimModeStreamCopy)
	}
	if plan.RequiresNormalization {
		t.Fatal("normalized source unexpectedly requires normalization")
	}
	assertContainsSequence(t, plan.TrimArgs, "-ss", "2.000000", "-i", "/source.mp4", "-t", "4.000000", "-c", "copy")
}

func TestVideoTrimmerPlanUsesFrameAccurateReencodeForMidGOPSegment(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})

	plan, err := trimmer.Plan(probe, VideoSegment{StartSeconds: 2.25, EndSeconds: 5.75}, "/source.mp4", "/segment.mp4")
	if err != nil {
		t.Fatalf("Plan() error: %v", err)
	}
	if plan.Mode != TrimModeFrameAccurateReencode {
		t.Fatalf("mode = %q, want %q", plan.Mode, TrimModeFrameAccurateReencode)
	}
	assertContainsSequence(t, plan.TrimArgs, "-i", "/source.mp4", "-ss", "2.250000", "-t", "3.500000", "-c:v", "libx264")
	if strings.Contains(strings.Join(plan.TrimArgs, " "), "-c copy") {
		t.Fatal("frame-accurate plan must not use stream copy")
	}
}

func TestVideoTrimmerReencodesCanonicalSourceAboveBitrateCap(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})
	probe.VideoBitrateBPS = 4_000_000

	plan, err := trimmer.Plan(probe, VideoSegment{StartSeconds: 2, EndSeconds: 6}, "/source.mp4", "/segment.mp4")
	if err != nil {
		t.Fatalf("Plan() error: %v", err)
	}
	if plan.Mode != TrimModeFrameAccurateReencode || !plan.RequiresNormalization {
		t.Fatalf("plan = %+v, want bitrate-cap normalization", plan)
	}
}

func TestVideoTrimmerNormalizesBeforeTrimming(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: probeJSON(t, VideoProbe{
		DurationSeconds: 12,
		Width:           1280,
		Height:          720,
		FPSNum:          24000,
		FPSDen:          1001,
		TimebaseNum:     1,
		TimebaseDen:     90000,
		VideoCodec:      "vp9",
		AudioCodec:      "opus",
		PixelFormat:     "yuv420p10le",
	})}
	trimmer := newVideoTrimmerForTest(runner)
	inputPath := filepath.Join(t.TempDir(), "source.webm")
	outputPath := filepath.Join(t.TempDir(), "segments", "clip.mp4")
	if err := os.WriteFile(inputPath, []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := trimmer.Trim(context.Background(), inputPath, outputPath, VideoSegment{StartSeconds: 1.25, EndSeconds: 4.75})
	if err != nil {
		t.Fatalf("Trim() error: %v", err)
	}
	if result.Plan.Mode != TrimModeFrameAccurateReencode {
		t.Fatalf("mode = %q, want frame-accurate re-encode", result.Plan.Mode)
	}
	if !result.Plan.RequiresNormalization {
		t.Fatal("non-normalized source did not require normalization")
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("trimmed output missing: %v", err)
	}
	if len(runner.commands) != 3 {
		t.Fatalf("commands = %d, want ffprobe + normalization ffmpeg + trim ffmpeg", len(runner.commands))
	}
	if runner.commands[0].name != "ffprobe" || runner.commands[1].name != "ffmpeg" || runner.commands[2].name != "ffmpeg" {
		t.Fatalf("command sequence = %#v", runner.commands)
	}
	assertContainsSequence(t, runner.commands[1].args, "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=24/1", "-c:v", "libx264", "-b:v", "2.25M", "-maxrate", "2.25M", "-bufsize", "4.5M", "-pix_fmt", "yuv420p", "-profile:v", "high", "-level:v", "4.0", "-g", "48", "-bf", "0", "-sc_threshold", "0", "-x264-params", "scenecut=0:open-gop=0:keyint=48:min-keyint=48", "-video_track_timescale", "90000", "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2")
	assertContainsSequence(t, runner.commands[2].args, "-ss", "1.250000", "-t", "3.500000", "-vf", "fps=24/1", "-c:v", "libx264", "-b:v", "2.25M", "-maxrate", "2.25M", "-bufsize", "4.5M", "-pix_fmt", "yuv420p", "-profile:v", "high", "-level:v", "4.0", "-g", "48", "-bf", "0", "-sc_threshold", "0", "-x264-params", "scenecut=0:open-gop=0:keyint=48:min-keyint=48", "-video_track_timescale", "90000", "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2")
}

func TestVideoTrimmerRejectsInvalidOrOutOfBoundsSegments(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})
	probe := normalizedVideoProbe(10, []float64{0, 5, 10})
	cases := []VideoSegment{
		{StartSeconds: -1, EndSeconds: 2},
		{StartSeconds: 4, EndSeconds: 4},
		{StartSeconds: 10, EndSeconds: 11},
		{StartSeconds: 8, EndSeconds: 11},
	}
	for _, segment := range cases {
		if _, err := trimmer.Plan(probe, segment, "source.mp4", "segment.mp4"); err == nil {
			t.Errorf("Plan(%+v) accepted invalid segment", segment)
		}
	}
}

func normalizedVideoProbe(duration float64, keyframes []float64) VideoProbe {
	profile := videoContract.CanonicalVideoProfileV1Default
	quality := videoContract.CanonicalPreparationQualityPolicyDefault
	return VideoProbe{
		DurationSeconds: duration,
		Width:           profile.Width,
		Height:          profile.Height,
		FPSNum:          profile.FPSNum,
		FPSDen:          profile.FPSDen,
		TimebaseNum:     profile.TimeBaseNum,
		TimebaseDen:     profile.TimeBaseDen,
		VideoCodec:      profile.Codec,
		AudioCodec:      quality.AudioCodec,
		AudioSampleRate: quality.AudioSampleRate,
		AudioChannels:   quality.AudioChannels,
		PixelFormat:     profile.PixelFormat,
		Keyframes:       keyframes,
	}
}

// TestVideoPreparationUsesCanonicalStreamProfile is the permanent anti-drift
// guard for the video preparation path. Before this test, the master had TWO
// authorities for video compatibility: shared/contract
// CanonicalVideoProfileV1 (1920x1080 @ 24 fps, GOP 48) and the trimmer-local
// VideoNormalization (1920x1080 @ 30 fps). A component that declares its own
// compatibility values must break CI, because W5 content-addressed reuse
// requires the same inputs to always produce exactly the same canonical
// stream.
func TestVideoPreparationUsesCanonicalStreamProfile(t *testing.T) {
	profile := videoContract.CanonicalVideoProfileV1Default
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})

	if got := trimmer.Profile(); got != profile {
		t.Fatalf("trimmer profile = %+v, want the canonical profile %+v", got, profile)
	}
	if trimmer.profile.FPSNum != profile.FPSNum || trimmer.profile.FPSDen != profile.FPSDen {
		t.Fatalf("trimmer frame rate = %d/%d, want canonical %d/%d",
			trimmer.profile.FPSNum, trimmer.profile.FPSDen, profile.FPSNum, profile.FPSDen)
	}
	if trimmer.profile.GOPSize != profile.GOPSize {
		t.Fatalf("trimmer GOP = %d, want canonical %d", trimmer.profile.GOPSize, profile.GOPSize)
	}
	if trimmer.profile.BFrames != profile.BFrames {
		t.Fatalf("trimmer B-frames = %d, want canonical %d", trimmer.profile.BFrames, profile.BFrames)
	}
	if trimmer.profile.Width != profile.Width || trimmer.profile.Height != profile.Height {
		t.Fatalf("trimmer dimensions = %dx%d, want canonical %dx%d",
			trimmer.profile.Width, trimmer.profile.Height, profile.Width, profile.Height)
	}
	if trimmer.profile.TimeBaseNum != profile.TimeBaseNum || trimmer.profile.TimeBaseDen != profile.TimeBaseDen {
		t.Fatalf("trimmer time base = %d/%d, want canonical %d/%d",
			trimmer.profile.TimeBaseNum, trimmer.profile.TimeBaseDen, profile.TimeBaseNum, profile.TimeBaseDen)
	}
	if trimmer.profile.CodecProfile != profile.CodecProfile || trimmer.profile.CodecLevel != profile.CodecLevel {
		t.Fatalf("trimmer codec profile/level = %s/%s, want canonical %s/%s",
			trimmer.profile.CodecProfile, trimmer.profile.CodecLevel, profile.CodecProfile, profile.CodecLevel)
	}

	// The produced ffmpeg command must carry the pinning arguments, so the
	// encoder cannot drift away from the identity the manifest advertises.
	args := strings.Join(normalizationArgs(trimmer.profile, trimmer.quality, "in.mp4", "out.mp4"), " ")
	for _, want := range []string{
		"fps=24/1",
		"-profile:v high",
		"-level:v 4.0",
		"-g 48",
		"-bf 0",
		"-x264-params scenecut=0:open-gop=0:keyint=48:min-keyint=48",
		"-video_track_timescale 90000",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("normalization args %q do not pin %q", args, want)
		}
	}
	// A literal 30 fps (the removed authority) must never come back.
	if strings.Contains(args, "fps=30") {
		t.Fatalf("normalization args still carry the removed 30 fps authority: %s", args)
	}
}

func TestCanonicalGOPAlignedRejectsForeignCadence(t *testing.T) {
	profile := videoContract.CanonicalVideoProfileV1Default // GOP 48 @ 24 fps = 2s
	cases := []struct {
		name      string
		keyframes []float64
		want      bool
	}{
		{"single keyframe cannot disprove alignment", []float64{0}, true},
		{"canonical 2s cadence", []float64{0, 2, 4, 6}, true},
		{"gop 24 (1s cadence)", []float64{0, 1, 2, 3}, false},
		{"gop 60 (2.5s cadence)", []float64{0, 2.5, 5}, false},
		{"scenecut noise", []float64{0, 2, 3.4, 5.9}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalGOPAligned(tc.keyframes, profile); got != tc.want {
				t.Fatalf("canonicalGOPAligned(%v) = %v, want %v", tc.keyframes, got, tc.want)
			}
		})
	}
}

func probeJSON(t *testing.T, probe VideoProbe) []byte {
	t.Helper()
	payload := map[string]interface{}{
		"streams": []interface{}{
			map[string]interface{}{
				"codec_type":   "video",
				"codec_name":   probe.VideoCodec,
				"width":        probe.Width,
				"height":       probe.Height,
				"r_frame_rate": ratioString(probe.FPSNum, probe.FPSDen),
				"time_base":    ratioString(probe.TimebaseNum, probe.TimebaseDen),
				"pix_fmt":      probe.PixelFormat,
				"duration":     probe.DurationSeconds,
			},
			map[string]interface{}{"codec_type": "audio", "codec_name": probe.AudioCodec, "sample_rate": probe.AudioSampleRate, "channels": probe.AudioChannels},
		},
		"frames": []interface{}{
			map[string]interface{}{"media_type": "video", "best_effort_timestamp_time": "0"},
			map[string]interface{}{"media_type": "video", "best_effort_timestamp_time": "2"},
			map[string]interface{}{"media_type": "video", "best_effort_timestamp_time": "4"},
			map[string]interface{}{"media_type": "video", "best_effort_timestamp_time": "6"},
		},
		"format": map[string]interface{}{"duration": probe.DurationSeconds},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func ratioString(numerator, denominator int) string {
	return strings.TrimSpace(strings.Join([]string{itoa(numerator), itoa(denominator)}, "/"))
}

func itoa(value int) string {
	return strconv.Itoa(value)
}

func assertContainsSequence(t *testing.T, args []string, expected ...string) {
	t.Helper()
	position := 0
	for _, want := range expected {
		found := -1
		for i := position; i < len(args); i++ {
			if args[i] == want {
				found = i
				break
			}
		}
		if found < 0 {
			t.Fatalf("args %v do not contain ordered values %v", args, expected)
		}
		position = found + 1
	}
}
