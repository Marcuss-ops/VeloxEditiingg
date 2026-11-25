package assets

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"velox-shared/contract"
)

type videoProbe struct {
	DurationSeconds float64
	FrameCount      int64
	Width           int
	Height          int
	FPSNum          int
	FPSDen          int
	TimebaseNum     int
	TimebaseDen     int
	VideoCodec      string
	VideoProfile    string
	VideoLevel      int
	AudioCodec      string
	PixelFormat     string
	BFrames         int
	ExtradataSHA256 string
	Keyframes       []float64
}

type videoCommandRunner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type execVideoCommandRunner struct{}

func (execVideoCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// videoWindowVerifier owns only the canonical profile and the Master probe used to
// independently validate a worker-registered prepared window. It does not
// materialize or trim source video; cold preparation belongs to worker tasks.
type videoWindowVerifier struct {
	runner  videoCommandRunner
	profile contract.CanonicalVideoProfileV1
	quality contract.PreparationQualityPolicy
}

func newVideoWindowVerifier() *videoWindowVerifier {
	return newVideoWindowVerifierWithPolicy(execVideoCommandRunner{}, contract.CanonicalVideoProfileV1Default, contract.CanonicalPreparationQualityPolicyDefault)
}

func newVideoWindowVerifierWithPolicy(runner videoCommandRunner, profile contract.CanonicalVideoProfileV1, quality contract.PreparationQualityPolicy) *videoWindowVerifier {
	if err := profile.Validate(); err != nil {
		profile = contract.CanonicalVideoProfileV1Default
	}
	if err := quality.Validate(); err != nil {
		quality = contract.CanonicalPreparationQualityPolicyDefault
	}
	return &videoWindowVerifier{runner: runner, profile: profile, quality: quality}
}

func newVideoWindowVerifierForTest(runner videoCommandRunner) *videoWindowVerifier {
	return newVideoWindowVerifierWithPolicy(runner, contract.CanonicalVideoProfileV1Default, contract.CanonicalPreparationQualityPolicyDefault)
}

func (t *videoWindowVerifier) preparationRevision() string {
	if t == nil {
		return ""
	}
	revision, err := contract.VideoWindowPreparationRevision(t.profile, t.quality)
	if err != nil {
		return ""
	}
	return revision
}

func (t *videoWindowVerifier) probe(ctx context.Context, inputPath string) (videoProbe, error) {
	if t == nil || t.runner == nil {
		return videoProbe{}, fmt.Errorf("video probe unavailable")
	}
	if strings.TrimSpace(inputPath) == "" {
		return videoProbe{}, fmt.Errorf("video input path is required")
	}
	args := []string{
		"-v", "error", "-show_data_hash", "sha256", "-skip_frame", "nokey", "-show_frames",
		"-show_entries", "stream=codec_type,codec_name,profile,level,width,height,r_frame_rate,time_base,pix_fmt,duration,nb_frames,has_b_frames,extradata_hash:format=duration:frame=media_type,key_frame,best_effort_timestamp_time",
		"-of", "json", inputPath,
	}
	output, err := t.runner.Run(ctx, "ffprobe", args...)
	if err != nil {
		return videoProbe{}, fmt.Errorf("ffprobe %s: %w: %s", inputPath, err, strings.TrimSpace(string(output)))
	}
	var document ffprobeDocument
	if err := json.Unmarshal(output, &document); err != nil {
		return videoProbe{}, fmt.Errorf("parse ffprobe %s: %w", inputPath, err)
	}
	if len(document.Streams) == 0 {
		return videoProbe{}, fmt.Errorf("ffprobe %s: no video stream", inputPath)
	}
	var videoStream *ffprobeStream
	probe := videoProbe{DurationSeconds: jsonFloat(document.Format.Duration)}
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
			}
		}
	}
	if videoStream == nil {
		return videoProbe{}, fmt.Errorf("ffprobe %s: no video stream", inputPath)
	}
	probe.DurationSeconds = firstPositive(jsonFloat(videoStream.Duration), probe.DurationSeconds)
	probe.Width = videoStream.Width
	probe.Height = videoStream.Height
	probe.VideoCodec = strings.ToLower(strings.TrimSpace(videoStream.CodecName))
	probe.VideoProfile = strings.ToLower(strings.TrimSpace(videoStream.Profile))
	probe.VideoLevel = videoStream.Level
	probe.PixelFormat = strings.ToLower(strings.TrimSpace(videoStream.PixelFormat))
	probe.BFrames = videoStream.BFrames
	probe.ExtradataSHA256 = strings.TrimPrefix(strings.TrimSpace(videoStream.ExtradataHash), "SHA256:")
	probe.FrameCount = jsonInt64(videoStream.FramesCount)
	probe.FPSNum, probe.FPSDen = parseRatio(videoStream.FrameRate)
	probe.TimebaseNum, probe.TimebaseDen = parseRatio(videoStream.TimeBase)
	for _, frame := range document.Frames {
		rawTimestamp := bytes.TrimSpace(frame.Timestamp)
		if strings.ToLower(strings.TrimSpace(frame.MediaType)) != "video" || frame.KeyFrame != 1 || len(rawTimestamp) == 0 || bytes.Equal(rawTimestamp, []byte("null")) {
			continue
		}
		timestamp := jsonFloat(rawTimestamp)
		if timestamp >= 0 {
			probe.Keyframes = append(probe.Keyframes, timestamp)
		}
	}
	if probe.DurationSeconds <= 0 || probe.Width <= 0 || probe.Height <= 0 || probe.FPSNum <= 0 || probe.FPSDen <= 0 || probe.VideoCodec == "" {
		return videoProbe{}, fmt.Errorf("ffprobe %s: incomplete video metadata", inputPath)
	}
	return probe, nil
}

type ffprobeDocument struct {
	Streams []ffprobeStream `json:"streams"`
	Frames  []ffprobeFrame  `json:"frames"`
	Format  ffprobeFormat   `json:"format"`
}

type ffprobeStream struct {
	CodecType     string          `json:"codec_type"`
	CodecName     string          `json:"codec_name"`
	Profile       string          `json:"profile"`
	Level         int             `json:"level"`
	Width         int             `json:"width"`
	Height        int             `json:"height"`
	FrameRate     string          `json:"r_frame_rate"`
	TimeBase      string          `json:"time_base"`
	PixelFormat   string          `json:"pix_fmt"`
	SampleRate    json.RawMessage `json:"sample_rate"`
	Channels      json.RawMessage `json:"channels"`
	Duration      json.RawMessage `json:"duration"`
	FramesCount   json.RawMessage `json:"nb_frames"`
	BFrames       int             `json:"has_b_frames"`
	ExtradataHash string          `json:"extradata_hash"`
}

type ffprobeFrame struct {
	MediaType string          `json:"media_type"`
	KeyFrame  int             `json:"key_frame"`
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
