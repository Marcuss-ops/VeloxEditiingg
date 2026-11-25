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

func TestVideoTrimmerPreparationRevisionTracksProfileAndQuality(t *testing.T) {
	profile := videoContract.CanonicalVideoProfileV1Default
	quality := videoContract.CanonicalPreparationQualityPolicyDefault
	base := newVideoTrimmerForTest(&recordingVideoRunner{})
	if got := base.PreparationRevision(); !strings.HasPrefix(got, "video-window-preparer.v1:") {
		t.Fatalf("preparation revision = %q", got)
	}
	quality.VideoCRF = 24
	changedQuality := newVideoTrimmer(&recordingVideoRunner{}, profile, quality)
	if changedQuality.PreparationRevision() == base.PreparationRevision() {
		t.Fatal("quality policy change did not invalidate preparation revision")
	}
	profile.GOPSize++
	changedProfile := newVideoTrimmer(&recordingVideoRunner{}, profile, videoContract.CanonicalPreparationQualityPolicyDefault)
	if changedProfile.PreparationRevision() == base.PreparationRevision() {
		t.Fatal("canonical profile change did not invalidate preparation revision")
	}
}

func TestTrimFramesWithProbeKeepsPacketCopyForKeyframeWindow(t *testing.T) {
	runner := &recordingVideoRunner{}
	trimmer := newVideoTrimmerForTest(runner)
	output := filepath.Join(t.TempDir(), "window.mp4")
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})
	result, err := trimmer.TrimFramesWithProbe(context.Background(), probe, "/source.mp4", output, VideoSegment{StartSeconds: 2, EndSeconds: 6}, 96)
	if err != nil {
		t.Fatalf("TrimFramesWithProbe() error: %v", err)
	}
	if result.Plan.Mode != TrimModeStreamCopy {
		t.Fatalf("mode = %q, want %q", result.Plan.Mode, TrimModeStreamCopy)
	}
	args := strings.Join(result.Plan.TrimArgs, " ")
	for _, required := range []string{"-ss 2.000000 -i /source.mp4", "-c:v copy", "-frames:v 96", "-an -sn -dn"} {
		if !strings.Contains(args, required) {
			t.Fatalf("copy args %q missing %q", args, required)
		}
	}
	if strings.Contains(args, "-t ") {
		t.Fatalf("frame-count-authoritative copy args include a duration cutoff: %q", args)
	}
}

func TestTrimFramesWithProbeUsesFrameCountInsteadOfRoundedDuration(t *testing.T) {
	runner := &recordingVideoRunner{}
	trimmer := newVideoTrimmerForTest(runner)
	output := filepath.Join(t.TempDir(), "window.mp4")
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})
	segment := VideoSegment{StartSeconds: 2, EndSeconds: 6}
	result, err := trimmer.TrimFramesWithProbe(context.Background(), probe, "/source.mp4", output, segment, 96)
	if err != nil {
		t.Fatalf("TrimFramesWithProbe() error: %v", err)
	}
	if result.Plan.Mode != TrimModeStreamCopy {
		t.Fatalf("mode = %q, want stream copy", result.Plan.Mode)
	}
	args := strings.Join(result.Plan.TrimArgs, " ")
	if !strings.Contains(args, "-frames:v 96") || strings.Contains(args, "libx264") {
		t.Fatalf("unexpected packet-copy args: %q", args)
	}
}

func TestVideoTrimmerRejectsMidGOPSegmentWithoutEncoding(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})

	plan, err := trimmer.Plan(probe, VideoSegment{StartSeconds: 2.25, EndSeconds: 5.75}, "/source.mp4", "/segment.mp4")
	if err == nil {
		t.Fatalf("unsafe plan unexpectedly accepted: %+v", plan)
	}
}

func TestVideoTrimmerRejectsNonCanonicalSourceWithoutEncoding(t *testing.T) {
	trimmer := newVideoTrimmerForTest(&recordingVideoRunner{})
	probe := normalizedVideoProbe(10, []float64{0, 2, 4, 6, 8, 10})
	probe.VideoBitrateBPS = 4_000_000

	plan, err := trimmer.Plan(probe, VideoSegment{StartSeconds: 2, EndSeconds: 6}, "/source.mp4", "/segment.mp4")
	if err == nil {
		t.Fatalf("non-canonical source unexpectedly accepted: %+v", plan)
	}
}

func TestVideoTrimmerRejectsIncompatibleSourceWithoutEncoding(t *testing.T) {
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

	_, err := trimmer.Trim(context.Background(), inputPath, outputPath, VideoSegment{StartSeconds: 1.25, EndSeconds: 4.75})
	if err == nil {
		t.Fatal("incompatible source was silently normalized")
	}
	for _, command := range runner.commands {
		if command.name == "ffmpeg" {
			t.Fatalf("incompatible source invoked ffmpeg: %+v", command)
		}
	}
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
		VideoProfile:    profile.CodecProfile,
		VideoLevel:      40,
		AudioCodec:      quality.AudioCodec,
		AudioSampleRate: quality.AudioSampleRate,
		AudioChannels:   quality.AudioChannels,
		PixelFormat:     profile.PixelFormat,
		BFrames:         profile.BFrames,
		ExtradataSHA256: videoContract.CanonicalVideoExtradataSHA256,
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

	args := strings.Join(trimArgs(VideoSegment{StartSeconds: 2, EndSeconds: 4}, 2, "in.mp4", "out.mp4"), " ")
	if strings.Contains(args, "libx264") || !strings.Contains(args, "-c copy") {
		t.Fatalf("Master stock preparation must packet-copy only: %s", args)
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
				"codec_type":     "video",
				"codec_name":     probe.VideoCodec,
				"profile":        probe.VideoProfile,
				"level":          probe.VideoLevel,
				"width":          probe.Width,
				"height":         probe.Height,
				"r_frame_rate":   ratioString(probe.FPSNum, probe.FPSDen),
				"time_base":      ratioString(probe.TimebaseNum, probe.TimebaseDen),
				"pix_fmt":        probe.PixelFormat,
				"duration":       probe.DurationSeconds,
				"nb_frames":      probe.FrameCount,
				"start_time":     probe.StartTimeSeconds,
				"has_b_frames":   probe.BFrames,
				"extradata_hash": "SHA256:" + probe.ExtradataSHA256,
			},
			map[string]interface{}{"codec_type": "audio", "codec_name": probe.AudioCodec, "sample_rate": probe.AudioSampleRate, "channels": probe.AudioChannels},
		},
		"frames": []interface{}{
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "0"},
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "2"},
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "4"},
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "6"},
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "8"},
			map[string]interface{}{"media_type": "video", "key_frame": 1, "best_effort_timestamp_time": "10"},
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
