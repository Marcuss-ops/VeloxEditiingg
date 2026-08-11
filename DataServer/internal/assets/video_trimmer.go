package assets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// VideoNormalization is the canonical format a segment must have before it
// is handed to a remote worker. Keeping this policy here prevents each caller
// from inventing a different resolution, frame rate, time base, or codec.
type VideoNormalization struct {
	Width              int
	Height             int
	FPSNum             int
	FPSDen             int
	VideoCodec         string
	AudioCodec         string
	PixelFormat        string
	AudioSampleRate    int
	AudioChannels      int
	VideoTrackTimebase int
}

// DefaultVideoNormalization is the deterministic Velox video format.
var DefaultVideoNormalization = VideoNormalization{
	Width:              1920,
	Height:             1080,
	FPSNum:             30,
	FPSDen:             1,
	VideoCodec:         "h264",
	AudioCodec:         "aac",
	PixelFormat:        "yuv420p",
	AudioSampleRate:    48000,
	AudioChannels:      2,
	VideoTrackTimebase: 90000,
}

// VideoSegment describes a half-open source interval [StartSeconds, EndSeconds).
type VideoSegment struct {
	StartSeconds float64
	EndSeconds   float64
}

// VideoProbe is the subset of ffprobe metadata needed to make the trimming
// decision. Keyframes are expressed in seconds in the source time base.
type VideoProbe struct {
	DurationSeconds float64
	Width           int
	Height          int
	FPSNum          int
	FPSDen          int
	TimebaseNum     int
	TimebaseDen     int
	VideoCodec      string
	AudioCodec      string
	AudioSampleRate int
	AudioChannels   int
	PixelFormat     string
	Keyframes       []float64
}

// TrimMode identifies which deterministic path was selected.
type TrimMode string

const (
	TrimModeStreamCopy            TrimMode = "stream_copy"
	TrimModeFrameAccurateReencode TrimMode = "frame_accurate_reencode"
)

// TrimPlan is inspectable before execution and is also useful for telemetry.
type TrimPlan struct {
	Mode                  TrimMode
	Segment               VideoSegment
	DurationSeconds       float64
	RequiresNormalization bool
	NormalizationArgs     []string
	TrimArgs              []string
}

// TrimResult reports the selected path and resulting output path.
type TrimResult struct {
	Plan       TrimPlan
	OutputPath string
}

type videoCommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type execVideoCommandRunner struct{}

func (execVideoCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// VideoTrimmer performs master-side segment preparation. The input must be a
// master-local staged file; callers should resolve/download it before calling
// Trim. The output is atomically promoted only after ffmpeg succeeds.
type VideoTrimmer struct {
	runner videoCommandRunner
	spec   VideoNormalization
}

// NewVideoTrimmer creates a trimmer using ffprobe and ffmpeg from PATH.
func NewVideoTrimmer(spec VideoNormalization) *VideoTrimmer {
	if spec.Width <= 0 || spec.Height <= 0 || spec.FPSNum <= 0 || spec.FPSDen <= 0 ||
		spec.VideoCodec == "" || spec.AudioCodec == "" || spec.PixelFormat == "" ||
		spec.AudioSampleRate <= 0 || spec.AudioChannels <= 0 || spec.VideoTrackTimebase <= 0 {
		spec = DefaultVideoNormalization
	}
	return &VideoTrimmer{runner: execVideoCommandRunner{}, spec: spec}
}

func newVideoTrimmerForTest(runner videoCommandRunner, spec VideoNormalization) *VideoTrimmer {
	trimmer := NewVideoTrimmer(spec)
	trimmer.runner = runner
	return trimmer
}

// Probe invokes ffprobe and returns the metadata used by Plan. It is public so
// callers can record the source decision in the asset manifest.
func (t *VideoTrimmer) Probe(ctx context.Context, inputPath string) (VideoProbe, error) {
	if t == nil || t.runner == nil {
		return VideoProbe{}, fmt.Errorf("video trimmer unavailable")
	}
	if strings.TrimSpace(inputPath) == "" {
		return VideoProbe{}, fmt.Errorf("video input path is required")
	}
	args := []string{
		"-v", "error",
		"-skip_frame", "nokey",
		"-show_frames",
		"-show_entries", "stream=codec_type,codec_name,width,height,r_frame_rate,time_base,pix_fmt,duration,sample_rate,channels:format=duration:frame=media_type,best_effort_timestamp_time",
		"-of", "json",
		inputPath,
	}
	output, err := t.runner.Run(ctx, "ffprobe", args...)
	if err != nil {
		return VideoProbe{}, fmt.Errorf("ffprobe %s: %w: %s", inputPath, err, strings.TrimSpace(string(output)))
	}
	var document ffprobeDocument
	if err := json.Unmarshal(output, &document); err != nil {
		return VideoProbe{}, fmt.Errorf("parse ffprobe %s: %w", inputPath, err)
	}
	if len(document.Streams) == 0 {
		return VideoProbe{}, fmt.Errorf("ffprobe %s: no video stream", inputPath)
	}
	var videoStream *ffprobeStream
	probe := VideoProbe{DurationSeconds: jsonFloat(document.Format.Duration)}
	for i := range document.Streams {
		stream := &document.Streams[i]
		switch stream.CodecType {
		case "video":
			if videoStream == nil {
				videoStream = stream
			}
		case "audio":
			if probe.AudioCodec == "" {
				probe.AudioCodec = strings.ToLower(strings.TrimSpace(stream.CodecName))
				probe.AudioSampleRate = jsonInt(stream.SampleRate)
				probe.AudioChannels = jsonInt(stream.Channels)
			}
		}
	}
	if videoStream == nil {
		return VideoProbe{}, fmt.Errorf("ffprobe %s: no video stream", inputPath)
	}
	probe.DurationSeconds = firstPositive(jsonFloat(videoStream.Duration), probe.DurationSeconds)
	probe.Width = videoStream.Width
	probe.Height = videoStream.Height
	probe.VideoCodec = strings.ToLower(strings.TrimSpace(videoStream.CodecName))
	probe.PixelFormat = strings.ToLower(strings.TrimSpace(videoStream.PixelFormat))
	probe.FPSNum, probe.FPSDen = parseRatio(videoStream.FrameRate)
	probe.TimebaseNum, probe.TimebaseDen = parseRatio(videoStream.TimeBase)
	for _, frame := range document.Frames {
		rawTimestamp := bytes.TrimSpace(frame.Timestamp)
		if strings.ToLower(strings.TrimSpace(frame.MediaType)) != "video" || len(rawTimestamp) == 0 || bytes.Equal(rawTimestamp, []byte("null")) {
			continue
		}
		timestamp := jsonFloat(rawTimestamp)
		if timestamp >= 0 {
			probe.Keyframes = append(probe.Keyframes, timestamp)
		}
	}
	if probe.DurationSeconds <= 0 || probe.Width <= 0 || probe.Height <= 0 || probe.FPSNum <= 0 || probe.FPSDen <= 0 || probe.VideoCodec == "" {
		return VideoProbe{}, fmt.Errorf("ffprobe %s: incomplete video metadata", inputPath)
	}
	return probe, nil
}

// Plan selects stream copy only when both boundaries are keyframe-aligned and
// the source already matches the canonical format. Every other case uses a
// frame-accurate re-encode; non-normalized sources are normalized first.
func (t *VideoTrimmer) Plan(probe VideoProbe, segment VideoSegment, inputPath, outputPath string) (TrimPlan, error) {
	if t == nil {
		return TrimPlan{}, fmt.Errorf("video trimmer unavailable")
	}
	if err := validateSegment(probe.DurationSeconds, segment); err != nil {
		return TrimPlan{}, err
	}
	duration := segment.EndSeconds - segment.StartSeconds
	normalizationRequired := !matchesNormalization(probe, t.spec)
	keyframeAligned := isKeyframeBoundary(segment.StartSeconds, probe.Keyframes) &&
		isKeyframeBoundary(segment.EndSeconds, probe.Keyframes)
	mode := TrimModeFrameAccurateReencode
	if !normalizationRequired && keyframeAligned {
		mode = TrimModeStreamCopy
	}

	plan := TrimPlan{
		Mode:                  mode,
		Segment:               segment,
		DurationSeconds:       duration,
		RequiresNormalization: normalizationRequired,
		NormalizationArgs:     normalizationArgs(t.spec, inputPath, ""),
		TrimArgs:              trimArgs(mode, segment, duration, inputPath, outputPath, t.spec),
	}
	return plan, nil
}

// Trim probes, plans, executes, and atomically promotes one segment. A
// non-normalized source is normalized into a sibling temporary file before the
// requested interval is cut, ensuring the worker receives only the segment.
func (t *VideoTrimmer) Trim(ctx context.Context, inputPath, outputPath string, segment VideoSegment) (TrimResult, error) {
	if strings.TrimSpace(inputPath) == "" || strings.TrimSpace(outputPath) == "" {
		return TrimResult{}, fmt.Errorf("video input and output paths are required")
	}
	probe, err := t.Probe(ctx, inputPath)
	if err != nil {
		return TrimResult{}, err
	}
	plan, err := t.Plan(probe, segment, inputPath, outputPath)
	if err != nil {
		return TrimResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return TrimResult{}, fmt.Errorf("create segment output directory: %w", err)
	}

	workingInput := inputPath
	normalizedPath := ""
	if plan.RequiresNormalization {
		tmp, err := os.CreateTemp(filepath.Dir(outputPath), ".velox-normalized-*.mp4")
		if err != nil {
			return TrimResult{}, fmt.Errorf("create normalization temp file: %w", err)
		}
		normalizedPath = tmp.Name()
		if err := tmp.Close(); err != nil {
			_ = os.Remove(normalizedPath)
			return TrimResult{}, fmt.Errorf("close normalization temp file: %w", err)
		}
		normalizeArgs := normalizationArgs(t.spec, inputPath, normalizedPath)
		if output, err := t.runner.Run(ctx, "ffmpeg", normalizeArgs...); err != nil {
			_ = os.Remove(normalizedPath)
			return TrimResult{}, fmt.Errorf("normalize video: %w: %s", err, strings.TrimSpace(string(output)))
		}
		workingInput = normalizedPath
		plan.TrimArgs = trimArgs(TrimModeFrameAccurateReencode, segment, plan.DurationSeconds, workingInput, outputPath, t.spec)
		plan.Mode = TrimModeFrameAccurateReencode
	}
	defer func() {
		if normalizedPath != "" {
			_ = os.Remove(normalizedPath)
		}
	}()

	tmpOutput, err := os.CreateTemp(filepath.Dir(outputPath), ".velox-segment-*.mp4")
	if err != nil {
		return TrimResult{}, fmt.Errorf("create segment temp file: %w", err)
	}
	tmpOutputPath := tmpOutput.Name()
	if err := tmpOutput.Close(); err != nil {
		_ = os.Remove(tmpOutputPath)
		return TrimResult{}, fmt.Errorf("close segment temp file: %w", err)
	}
	defer os.Remove(tmpOutputPath)

	plan.TrimArgs = trimArgs(plan.Mode, segment, plan.DurationSeconds, workingInput, tmpOutputPath, t.spec)
	output, err := t.runner.Run(ctx, "ffmpeg", plan.TrimArgs...)
	if err != nil {
		return TrimResult{}, fmt.Errorf("trim video: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := os.Rename(tmpOutputPath, outputPath); err != nil {
		return TrimResult{}, fmt.Errorf("promote trimmed segment: %w", err)
	}
	return TrimResult{Plan: plan, OutputPath: outputPath}, nil
}

type ffprobeDocument struct {
	Streams []ffprobeStream `json:"streams"`
	Frames  []ffprobeFrame  `json:"frames"`
	Format  ffprobeFormat   `json:"format"`
}

type ffprobeStream struct {
	CodecType   string          `json:"codec_type"`
	CodecName   string          `json:"codec_name"`
	Width       int             `json:"width"`
	Height      int             `json:"height"`
	FrameRate   string          `json:"r_frame_rate"`
	TimeBase    string          `json:"time_base"`
	PixelFormat string          `json:"pix_fmt"`
	Duration    json.RawMessage `json:"duration"`
	SampleRate  json.RawMessage `json:"sample_rate"`
	Channels    json.RawMessage `json:"channels"`
}

type ffprobeFrame struct {
	MediaType string          `json:"media_type"`
	Timestamp json.RawMessage `json:"best_effort_timestamp_time"`
}

type ffprobeFormat struct {
	Duration   json.RawMessage `json:"duration"`
	FormatName json.RawMessage `json:"format_name"`
}

func jsonFloat(raw json.RawMessage) float64 {
	value := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func jsonInt(raw json.RawMessage) int {
	value := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}

func normalizationArgs(spec VideoNormalization, inputPath, outputPath string) []string {
	return []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-i", inputPath,
		"-map", "0:v:0", "-map", "0:a?",
		"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,fps=%d/%d", spec.Width, spec.Height, spec.Width, spec.Height, spec.FPSNum, spec.FPSDen),
		"-c:v", "libx264", "-pix_fmt", spec.PixelFormat,
		"-video_track_timescale", strconv.Itoa(spec.VideoTrackTimebase),
		"-c:a", spec.AudioCodec, "-ar", strconv.Itoa(spec.AudioSampleRate), "-ac", strconv.Itoa(spec.AudioChannels),
		"-movflags", "+faststart", outputPath,
	}
}

func trimArgs(mode TrimMode, segment VideoSegment, duration float64, inputPath, outputPath string, spec VideoNormalization) []string {
	common := []string{"-hide_banner", "-loglevel", "error", "-y"}
	if mode == TrimModeStreamCopy {
		return append(common, "-ss", formatSeconds(segment.StartSeconds), "-i", inputPath, "-t", formatSeconds(duration), "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1", outputPath)
	}
	return append(common, "-i", inputPath, "-ss", formatSeconds(segment.StartSeconds), "-t", formatSeconds(duration), "-map", "0:v:0", "-map", "0:a?", "-vf", fmt.Sprintf("fps=%d/%d", spec.FPSNum, spec.FPSDen), "-c:v", "libx264", "-pix_fmt", spec.PixelFormat, "-video_track_timescale", strconv.Itoa(spec.VideoTrackTimebase), "-c:a", spec.AudioCodec, "-ar", strconv.Itoa(spec.AudioSampleRate), "-ac", strconv.Itoa(spec.AudioChannels), "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1", outputPath)
}

func validateSegment(total float64, segment VideoSegment) error {
	if segment.StartSeconds < 0 || segment.EndSeconds <= segment.StartSeconds {
		return fmt.Errorf("invalid video segment: start must be >= 0 and end must be greater than start")
	}
	if total <= 0 || segment.StartSeconds >= total {
		return fmt.Errorf("video segment starts outside source duration")
	}
	if segment.EndSeconds > total {
		return fmt.Errorf("video segment ends outside source duration")
	}
	return nil
}

func matchesNormalization(probe VideoProbe, spec VideoNormalization) bool {
	return probe.Width == spec.Width && probe.Height == spec.Height &&
		probe.FPSNum == spec.FPSNum && probe.FPSDen == spec.FPSDen &&
		probe.VideoCodec == strings.ToLower(spec.VideoCodec) &&
		probe.AudioCodec == strings.ToLower(spec.AudioCodec) &&
		probe.AudioSampleRate == spec.AudioSampleRate &&
		probe.AudioChannels == spec.AudioChannels &&
		probe.PixelFormat == strings.ToLower(spec.PixelFormat) &&
		probe.TimebaseNum == 1 && probe.TimebaseDen == spec.VideoTrackTimebase
}

func isKeyframeBoundary(value float64, keyframes []float64) bool {
	const tolerance = 0.0005
	for _, keyframe := range keyframes {
		if abs(value-keyframe) <= tolerance {
			return true
		}
	}
	return false
}

func parseRatio(value string) (int, int) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 {
		return 0, 0
	}
	numerator, errN := strconv.Atoi(parts[0])
	denominator, errD := strconv.Atoi(parts[1])
	if errN != nil || errD != nil || numerator <= 0 || denominator <= 0 {
		return 0, 0
	}
	return numerator, denominator
}

func firstPositive(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func formatSeconds(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
