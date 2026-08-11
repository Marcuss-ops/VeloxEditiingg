package assets

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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
	trimmer := newVideoTrimmerForTest(runner, defaultVideoNormalization)
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
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{}, defaultVideoNormalization)
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
	trimmer := newVideoTrimmerForTest(runner, defaultVideoNormalization)
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
	assertContainsSequence(t, runner.commands[1].args, "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,fps=30/1", "-c:v", "libx264", "-video_track_timescale", "90000", "-c:a", "aac", "-ar", "48000", "-ac", "2")
	assertContainsSequence(t, runner.commands[2].args, "-ss", "1.250000", "-t", "3.500000", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-video_track_timescale", "90000")
}

func TestVideoTrimmerRejectsInvalidOrOutOfBoundsSegments(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{}, defaultVideoNormalization)
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
	return VideoProbe{
		DurationSeconds: duration,
		Width:           defaultVideoNormalization.Width,
		Height:          defaultVideoNormalization.Height,
		FPSNum:          defaultVideoNormalization.FPSNum,
		FPSDen:          defaultVideoNormalization.FPSDen,
		TimebaseNum:     1,
		TimebaseDen:     defaultVideoNormalization.VideoTrackTimebase,
		VideoCodec:      defaultVideoNormalization.VideoCodec,
		AudioCodec:      defaultVideoNormalization.AudioCodec,
		AudioSampleRate: defaultVideoNormalization.AudioSampleRate,
		AudioChannels:   defaultVideoNormalization.AudioChannels,
		PixelFormat:     defaultVideoNormalization.PixelFormat,
		Keyframes:       keyframes,
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
