package assets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"velox-shared/contract"
)

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
	VideoBitrateBPS int64
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

	// CanonicalProfile is the encoded-stream identity the prepared segment was
	// produced against. It is the ONLY authority for compatibility fields and
	// is persisted into the asset manifest so downstream consumers never have
	// to re-derive (or re-invent) the identity.
	CanonicalProfile contract.CanonicalVideoProfileV1
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
//
// The trimmer owns NO compatibility policy of its own: every authoritative
// stream field (dimensions, frame rate, pixel format, codec profile/level,
// GOP, B-frames, closed GOP, time base) is read from the canonical profile,
// and only the quality knobs come from the quality policy. This is what makes
// "the same inputs always produce the same canonical stream" enforceable: a
// component that declares its own 30 fps while the profile says 24 fps is a
// regression, not a preference.
type VideoTrimmer struct {
	runner  videoCommandRunner
	profile contract.CanonicalVideoProfileV1
	quality contract.PreparationQualityPolicy
}

// NewVideoTrimmer creates a trimmer using ffprobe and ffmpeg from PATH, wired
// to the canonical video profile and the canonical quality policy.
func NewVideoTrimmer() *VideoTrimmer {
	return newVideoTrimmer(execVideoCommandRunner{}, contract.CanonicalVideoProfileV1Default, contract.CanonicalPreparationQualityPolicyDefault)
}

// newVideoTrimmer is the single construction path. An invalid profile or
// quality policy fails closed onto the canonical defaults instead of encoding
// with a stream identity nobody can certify.
func newVideoTrimmer(runner videoCommandRunner, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) *VideoTrimmer {
	if err := profile.Validate(); err != nil {
		profile = contract.CanonicalVideoProfileV1Default
	}
	if err := quality.Validate(); err != nil {
		quality = contract.CanonicalPreparationQualityPolicyDefault
	}
	return &VideoTrimmer{runner: runner, profile: profile, quality: quality}
}

func newVideoTrimmerForTest(runner videoCommandRunner) *VideoTrimmer {
	return newVideoTrimmer(runner, contract.CanonicalVideoProfileV1Default, contract.CanonicalPreparationQualityPolicyDefault)
}

// Profile returns a copy of the canonical stream identity this trimmer pins.
// Callers (asset manifests, telemetry) read the identity from here instead of
// re-declaring it.
func (t *VideoTrimmer) Profile() contract.CanonicalVideoProfileV1 {
	if t == nil {
		return contract.CanonicalVideoProfileV1Default
	}
	return t.profile
}

// Quality returns a copy of the encoding quality policy this trimmer pins.
func (t *VideoTrimmer) Quality() contract.PreparationQualityPolicy {
	if t == nil {
		return contract.CanonicalPreparationQualityPolicyDefault
	}
	return t.quality
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
	probe.VideoBitrateBPS = jsonInt64(videoStream.BitRate)
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
// the source already carries the canonical stream identity. Every other case
// uses a frame-accurate re-encode; non-canonical sources are prepared first.
func (t *VideoTrimmer) Plan(probe VideoProbe, segment VideoSegment, inputPath, outputPath string) (TrimPlan, error) {
	if t == nil {
		return TrimPlan{}, fmt.Errorf("video trimmer unavailable")
	}
	if err := validateSegment(probe.DurationSeconds, segment); err != nil {
		return TrimPlan{}, err
	}
	duration := segment.EndSeconds - segment.StartSeconds
	normalizationRequired := !matchesCanonicalProfile(probe, t.profile, t.quality)
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
		CanonicalProfile:      t.profile,
		NormalizationArgs:     normalizationArgs(t.profile, t.quality, inputPath, ""),
		TrimArgs:              trimArgs(mode, segment, duration, inputPath, outputPath, t.profile, t.quality),
	}
	return plan, nil
}

// Trim probes, plans, executes, and atomically promotes one segment. A
// non-normalized source is normalized into a sibling temporary file before the
// requested interval is cut, ensuring the worker receives only the segment.
func (t *VideoTrimmer) Trim(ctx context.Context, inputPath, outputPath string, segment VideoSegment) (TrimResult, error) {
	if t == nil || t.runner == nil {
		return TrimResult{}, fmt.Errorf("video trimmer unavailable")
	}
	if strings.TrimSpace(inputPath) == "" || strings.TrimSpace(outputPath) == "" {
		return TrimResult{}, fmt.Errorf("video input and output paths are required")
	}
	probe, err := t.Probe(ctx, inputPath)
	if err != nil {
		return TrimResult{}, err
	}
	return t.trimWithProbe(ctx, probe, inputPath, outputPath, segment)
}

// TrimWithProbe trims one segment using a caller-provided canonical probe
// (registry-first metadata or a per-source memo). Repeated segments from the
// SAME source probe exactly ONCE (Fase C2: probe UNA volta) instead of
// spawning N ffprobe processes for N segments.
func (t *VideoTrimmer) TrimWithProbe(ctx context.Context, probe VideoProbe, inputPath, outputPath string, segment VideoSegment) (TrimResult, error) {
	if t == nil || t.runner == nil {
		return TrimResult{}, fmt.Errorf("video trimmer unavailable")
	}
	if strings.TrimSpace(inputPath) == "" || strings.TrimSpace(outputPath) == "" {
		return TrimResult{}, fmt.Errorf("video input and output paths are required")
	}
	return t.trimWithProbe(ctx, probe, inputPath, outputPath, segment)
}

// trimWithProbe is the shared execution body: plan, normalize if needed,
// execute ffmpeg, atomically promote. The probe is already validated by the
// caller (Plan fails on an invalid probe via validateSegment).
func (t *VideoTrimmer) trimWithProbe(ctx context.Context, probe VideoProbe, inputPath, outputPath string, segment VideoSegment) (TrimResult, error) {
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
		normalizeArgs := normalizationArgs(t.profile, t.quality, inputPath, normalizedPath)
		if output, err := t.runner.Run(ctx, "ffmpeg", normalizeArgs...); err != nil {
			_ = os.Remove(normalizedPath)
			return TrimResult{}, fmt.Errorf("normalize video: %w: %s", err, strings.TrimSpace(string(output)))
		}
		workingInput = normalizedPath
		plan.TrimArgs = trimArgs(TrimModeFrameAccurateReencode, segment, plan.DurationSeconds, workingInput, outputPath, t.profile, t.quality)
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

	plan.TrimArgs = trimArgs(plan.Mode, segment, plan.DurationSeconds, workingInput, tmpOutputPath, t.profile, t.quality)
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
	BitRate     json.RawMessage `json:"bit_rate"`
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

func jsonInt64(raw json.RawMessage) int64 {
	value := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

// canonicalVideoEncodeArgs pins the encoder arguments that define the
// canonical stream identity. Every compatibility field is READ from the
// profile — never re-declared here. Quality/bitrate knobs come from the
// quality policy and are allowed to move without changing the identity.
//
// The GOP is pinned twice on purpose: the generic `-g`/`-bf`/`-sc_threshold`
// flags express the contract, and the libx264 `keyint=min-keyint=GOPSize` with
// `scenecut=0` removes the adaptive-keyframe behaviour that would otherwise
// silently shift chunk boundaries under W5 reuse. `open-gop` follows the
// profile's ClosedGOP so a closed-GOP profile can never be produced with open
// GOPs.
func canonicalVideoEncodeArgs(profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) []string {
	openGOP := 0
	if !profile.ClosedGOP {
		openGOP = 1
	}
	return []string{
		"-c:v", "libx264",
		"-b:v", quality.VideoBitrate,
		"-maxrate", quality.VideoMaxRate,
		"-bufsize", quality.VideoBufferSize,
		"-pix_fmt", profile.PixelFormat,
		"-profile:v", profile.CodecProfile,
		"-level:v", profile.CodecLevel,
		"-g", strconv.Itoa(profile.GOPSize),
		"-bf", strconv.Itoa(profile.BFrames),
		"-sc_threshold", "0",
		"-x264-params", fmt.Sprintf("scenecut=0:open-gop=%d:keyint=%d:min-keyint=%d", openGOP, profile.GOPSize, profile.GOPSize),
		"-video_track_timescale", strconv.Itoa(profile.TimeBaseDen),
	}
}

// canonicalAudioEncodeArgs derives the audio encoder arguments from the
// quality policy (audio quality is not part of the video stream identity).
func canonicalAudioEncodeArgs(quality contract.PreparationQualityPolicy) []string {
	return []string{
		"-c:a", quality.AudioCodec,
		"-b:a", quality.AudioBitrate,
		"-ar", strconv.Itoa(quality.AudioSampleRate),
		"-ac", strconv.Itoa(quality.AudioChannels),
	}
}

// canonicalScaleFilter builds the letterbox/pad + frame-rate filter from the
// profile. The frame rate is interpolated, so a literal `fps=NN` can never
// reappear here (enforced by scripts/ci/check-architecture.sh rule 14).
func canonicalScaleFilter(profile contract.CanonicalVideoProfileV1) string {
	return fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2,fps=%d/%d",
		profile.Width, profile.Height, profile.Width, profile.Height, profile.FPSNum, profile.FPSDen)
}

func normalizationArgs(profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy, inputPath, outputPath string) []string {
	args := []string{
		"-hide_banner", "-loglevel", "error", "-y",
		"-i", inputPath,
		"-map", "0:v:0", "-map", "0:a?",
		"-vf", canonicalScaleFilter(profile),
	}
	args = append(args, canonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, canonicalAudioEncodeArgs(quality)...)
	return append(args, "-movflags", "+faststart", outputPath)
}

func trimArgs(mode TrimMode, segment VideoSegment, duration float64, inputPath, outputPath string, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) []string {
	common := []string{"-hide_banner", "-loglevel", "error", "-y"}
	if mode == TrimModeStreamCopy {
		return append(common, "-ss", formatSeconds(segment.StartSeconds), "-i", inputPath, "-t", formatSeconds(duration), "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1", outputPath)
	}
	args := append(common, "-i", inputPath, "-ss", formatSeconds(segment.StartSeconds), "-t", formatSeconds(duration), "-map", "0:v:0", "-map", "0:a?")
	args = append(args, "-vf", fmt.Sprintf("fps=%d/%d", profile.FPSNum, profile.FPSDen))
	args = append(args, canonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, canonicalAudioEncodeArgs(quality)...)
	return append(args, "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1", outputPath)
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

// matchesCanonicalProfile reports whether a probed source already carries the
// EXACT canonical stream identity, so packet copy is admissible without
// re-encoding. Compatibility fields come from the profile; the audio shape and
// the bitrate ceiling come from the quality policy.
//
// The keyframe cadence is part of the certification: a source whose GOP does
// not match the profile cannot be assumed chunk-aligned for content-addressed
// reuse (W5), so it is prepared once into the canonical identity instead.
func matchesCanonicalProfile(probe VideoProbe, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) bool {
	bitrateMatches := true
	if probe.VideoBitrateBPS > 0 {
		targetBPS := parseBitrateBPS(quality.VideoMaxRate)
		bitrateMatches = targetBPS > 0 && probe.VideoBitrateBPS <= targetBPS
	}
	return probe.Width == profile.Width && probe.Height == profile.Height &&
		probe.FPSNum == profile.FPSNum && probe.FPSDen == profile.FPSDen &&
		probe.VideoCodec == strings.ToLower(profile.Codec) &&
		probe.PixelFormat == strings.ToLower(profile.PixelFormat) &&
		probe.TimebaseNum == profile.TimeBaseNum && probe.TimebaseDen == profile.TimeBaseDen &&
		probe.AudioCodec == strings.ToLower(quality.AudioCodec) &&
		probe.AudioSampleRate == quality.AudioSampleRate &&
		probe.AudioChannels == quality.AudioChannels &&
		bitrateMatches &&
		canonicalGOPAligned(probe.Keyframes, profile)
}

// canonicalGOPAligned certifies the source keyframe cadence against the
// profile's GOP duration (GOPSize frames at the profile frame rate). A source
// with a single keyframe cannot disprove alignment and is accepted; two or
// more keyframes must land STRICTLY inside a quarter of the canonical GOP
// duration, which rejects a whole different cadence (e.g. GOP 24 = 1.0 s or
// GOP 60 = 2.5 s at the canonical 2.0 s GOP) without rejecting benign encoder
// jitter.
func canonicalGOPAligned(keyframes []float64, profile contract.CanonicalVideoProfileV1) bool {
	if len(keyframes) < 2 || profile.FPSNum <= 0 || profile.GOPSize <= 0 {
		return true
	}
	target := float64(profile.GOPSize) * float64(profile.FPSDen) / float64(profile.FPSNum)
	if target <= 0 {
		return true
	}
	lower, upper := target*0.75, target*1.25
	for i := 1; i < len(keyframes); i++ {
		interval := keyframes[i] - keyframes[i-1]
		if interval <= lower || interval >= upper {
			return false
		}
	}
	return true
}

func parseBitrateBPS(value string) int64 {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0
	}
	multiplier := float64(1)
	switch value[len(value)-1] {
	case 'K':
		multiplier = 1000
		value = value[:len(value)-1]
	case 'M':
		multiplier = 1000 * 1000
		value = value[:len(value)-1]
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || number <= 0 {
		return 0
	}
	return int64(number * multiplier)
}

func isKeyframeBoundary(value float64, keyframes []float64) bool {
	const tolerance = 0.0005
	for _, keyframe := range keyframes {
		if math.Abs(value-keyframe) <= tolerance {
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
