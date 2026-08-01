// Package quality defines deterministic media quality gates and staged worker
// rollout policy. The worker supplies measured values; the master decides
// whether a build may advance or must be quarantined.
package quality

import "fmt"

type Sample struct {
	DurationSeconds  float64
	FrameCount       int
	ExpectedFrames   int
	AudioSyncMS      float64
	LoudnessLUFS     float64
	BlackFrameRatio  float64
	SilenceRatio     float64
	SubtitleTimingOK bool
	Resolution       string
	Codec            string
	Bitrate          int64
	FFprobeValid     bool
}

type Thresholds struct {
	MaxAudioSyncMS     float64
	MaxBlackFrameRatio float64
	MaxSilenceRatio    float64
	MinLoudnessLUFS    float64
	MaxLoudnessLUFS    float64
}

func (s Sample) Validate(t Thresholds) error {
	if !s.FFprobeValid {
		return fmt.Errorf("quality_ffprobe_invalid")
	}
	if s.FrameCount <= 0 || (s.ExpectedFrames > 0 && s.FrameCount != s.ExpectedFrames) {
		return fmt.Errorf("quality_frame_count_mismatch")
	}
	if t.MaxAudioSyncMS > 0 && s.AudioSyncMS > t.MaxAudioSyncMS {
		return fmt.Errorf("quality_audio_sync")
	}
	if t.MaxBlackFrameRatio > 0 && s.BlackFrameRatio > t.MaxBlackFrameRatio {
		return fmt.Errorf("quality_black_frames")
	}
	if t.MaxSilenceRatio > 0 && s.SilenceRatio > t.MaxSilenceRatio {
		return fmt.Errorf("quality_silence")
	}
	if t.MinLoudnessLUFS != 0 && s.LoudnessLUFS < t.MinLoudnessLUFS {
		return fmt.Errorf("quality_loudness_low")
	}
	if t.MaxLoudnessLUFS != 0 && s.LoudnessLUFS > t.MaxLoudnessLUFS {
		return fmt.Errorf("quality_loudness_high")
	}
	if !s.SubtitleTimingOK {
		return fmt.Errorf("quality_subtitle_timing")
	}
	return nil
}

type RolloutPhase string

const (
	Benchmark  RolloutPhase = "benchmark"
	Canary     RolloutPhase = "canary"
	Quarter    RolloutPhase = "25_percent"
	Half       RolloutPhase = "50_percent"
	Stable     RolloutPhase = "stable"
	Quarantine RolloutPhase = "quarantine"
)

type RolloutPolicy struct{ FailureRateMax, RenderFactorRegressionMax, RSSGrowthMax, TempAmplificationMax, UploadFailureRateMax float64 }
type RolloutObservation struct {
	FailureRate, RenderFactorRegression, RSSGrowth, TempAmplification, UploadFailureRate float64
	QualityPassed                                                                        bool
}

func (p RolloutPolicy) Accept(o RolloutObservation) bool {
	return o.QualityPassed && (p.FailureRateMax <= 0 || o.FailureRate <= p.FailureRateMax) && (p.RenderFactorRegressionMax <= 0 || o.RenderFactorRegression <= p.RenderFactorRegressionMax) && (p.RSSGrowthMax <= 0 || o.RSSGrowth <= p.RSSGrowthMax) && (p.TempAmplificationMax <= 0 || o.TempAmplification <= p.TempAmplificationMax) && (p.UploadFailureRateMax <= 0 || o.UploadFailureRate <= p.UploadFailureRateMax)
}

func NextPhase(current RolloutPhase, observation RolloutObservation, policy RolloutPolicy) RolloutPhase {
	if !policy.Accept(observation) {
		return Quarantine
	}
	switch current {
	case Benchmark:
		return Canary
	case Canary:
		return Quarter
	case Quarter:
		return Half
	case Half:
		return Stable
	default:
		return current
	}
}

var GoldenCases = []string{"basic_render", "long_video", "microcut", "subtitles", "ten_languages", "multiaudio", "missing_font", "corrupt_asset", "transitions", "cache_hit", "cache_miss", "segment_retry"}
