package quality

import "testing"

func TestQualityGateAndCanaryRollback(t *testing.T) {
	sample := Sample{FFprobeValid: true, FrameCount: 100, ExpectedFrames: 100, SubtitleTimingOK: true, LoudnessLUFS: -14}
	if err := sample.Validate(Thresholds{MaxAudioSyncMS: 100, MaxBlackFrameRatio: .1, MaxSilenceRatio: .2, MinLoudnessLUFS: -24, MaxLoudnessLUFS: -8}); err != nil {
		t.Fatal(err)
	}
	if got := NextPhase(Benchmark, RolloutObservation{QualityPassed: true}, RolloutPolicy{}); got != Canary {
		t.Fatalf("phase=%s", got)
	}
	if got := NextPhase(Canary, RolloutObservation{QualityPassed: false}, RolloutPolicy{}); got != Quarantine {
		t.Fatalf("failed quality advanced to %s", got)
	}
	if len(GoldenCases) < 12 {
		t.Fatalf("golden cases=%d", len(GoldenCases))
	}
}
