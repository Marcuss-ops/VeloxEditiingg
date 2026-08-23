package contract

import (
	"errors"
	"strings"
	"testing"
)

// canonicalProbe returns a media probe that matches the canonical V1 profile
// exactly, with the declared video-only stream and the given duration.
func canonicalProbe(durationUS int64) ReplacementMediaProbe {
	return ReplacementMediaProbe{
		Codec:       "h264",
		Profile:     "high",
		PixelFormat: "yuv420p",
		Width:       1920,
		Height:      1080,
		FPSNum:      CanonicalVideoProfileV1Default.FPSNum,
		FPSDen:      CanonicalVideoProfileV1Default.FPSDen,
		DurationUS:  durationUS,
		HasAudio:    false,
	}
}

func TestValidateVisualReplacementMedia_AcceptsCanonicalVideoOnly(t *testing.T) {
	err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, canonicalProbe(5_000_000))
	if err != nil {
		t.Fatalf("canonical video-only replacement rejected: %v", err)
	}
}

func TestValidateVisualReplacementMedia_RejectsAudio(t *testing.T) {
	probe := canonicalProbe(5_000_000)
	probe.HasAudio = true
	err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, probe)
	assertReplacementErr(t, err, VisualReplacementCodeAudioNotAllowed, "vr")
}

func TestValidateVisualReplacementMedia_RejectsSignatureMismatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ReplacementMediaProbe)
	}{
		{"codec h265", func(p *ReplacementMediaProbe) { p.Codec = "h265" }},
		{"resolution 720p", func(p *ReplacementMediaProbe) { p.Width, p.Height = 1280, 720 }},
		{"fps 60", func(p *ReplacementMediaProbe) { p.FPSNum, p.FPSDen = 60, 1 }},
		{"pix_fmt yuv444p", func(p *ReplacementMediaProbe) { p.PixelFormat = "yuv444p" }},
		{"profile baseline", func(p *ReplacementMediaProbe) { p.Profile = "baseline" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := canonicalProbe(5_000_000)
			tc.mutate(&probe)
			err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, probe)
			assertReplacementErr(t, err, CopyOnlyMediaSignatureMismatchCode, "vr")
		})
	}
}

func TestValidateVisualReplacementMedia_AcceptsCodecAliases(t *testing.T) {
	for _, codec := range []string{"libx264", "h264_nvenc", "h264_vaapi", "h264"} {
		probe := canonicalProbe(5_000_000)
		probe.Codec = codec
		if err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, probe); err != nil {
			t.Fatalf("codec %q rejected: %v", codec, err)
		}
	}
}

func TestValidateVisualReplacementMedia_RejectsDurationMismatch(t *testing.T) {
	// Plan §6: declared window is 5s but the prepared media is 4.2s (or 5.1s).
	// Both must fail with VISUAL_REPLACEMENT_DURATION_MISMATCH, never be
	// silently padded/trimmed.
	for _, duration := range []int64{4_200_000, 5_100_000} {
		err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, canonicalProbe(duration))
		assertReplacementErr(t, err, VisualReplacementCodeDurationMismatch, "vr")
	}
}

func TestValidateVisualReplacementMedia_ToleranceBoundary(t *testing.T) {
	// Exactly at the tolerance is admitted; one microsecond past is rejected.
	// This pins the centralized ReplacementDurationToleranceUS as the single
	// boundary so no per-call epsilon can drift in later.
	tolerance := ReplacementDurationToleranceUS
	for _, duration := range []int64{5_000_000 - tolerance, 5_000_000 + tolerance} {
		if err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, canonicalProbe(duration)); err != nil {
			t.Fatalf("duration %d within tolerance rejected: %v", duration, err)
		}
	}
	for _, duration := range []int64{5_000_000 - tolerance - 1, 5_000_000 + tolerance + 1} {
		err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 5_000_000, canonicalProbe(duration))
		assertReplacementErr(t, err, VisualReplacementCodeDurationMismatch, "vr")
	}
}

func TestValidateVisualReplacementMedia_ZeroWindow(t *testing.T) {
	err := ValidateVisualReplacementMedia(CanonicalVideoProfileV1Default, "vr", 0, canonicalProbe(0))
	assertReplacementErr(t, err, VisualReplacementCodeInvalidRange, "vr")
}

// TestCompileRenderPlanV2_VisualReplacement_RejectsDeclaredDurationMismatch
// pins the master-side declared-duration gate: a manifest asset whose
// duration_ms does not equal the replacement window is rejected before any
// task reaches a worker, using the same centralized tolerance.
func TestCompileRenderPlanV2_VisualReplacement_RejectsDeclaredDurationMismatch(t *testing.T) {
	manifest := visualReplacementManifest(t)
	// The prepared asset declares 5000 ms (5s) but the replacement window is
	// 60→66 = 6s. The resolver itself is valid; only the declared duration is
	// wrong, so the compile must surface DURATION_MISMATCH.
	reps := []VisualReplacement{{
		ReplacementID:   "vr_001",
		AssetID:         "prepared",
		SHA256:          strings.Repeat("b", 64),
		TimelineStartUS: 60_000_000,
		TimelineEndUS:   66_000_000,
		ProfileID:       "velox-h264-1080p30-v1",
	}}
	_, err := CompileRenderPlanV2FromManifestWithReplacements(manifest, reps)
	if err == nil {
		t.Fatalf("expected declared duration mismatch, got nil")
	}
	var re *VisualReplacementError
	if !errors.As(err, &re) || re.Code != VisualReplacementCodeDurationMismatch {
		t.Fatalf("expected VisualReplacementError DURATION_MISMATCH, got %T: %v", err, err)
	}
}
