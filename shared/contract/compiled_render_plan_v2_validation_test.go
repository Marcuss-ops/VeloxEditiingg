package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func validPlanForValidation() *CompiledRenderPlanV2 {
	plan := testCompiledRenderPlanV2()
	// The canonical fixture uses a source offset of 33.2s, so provide a
	// source asset long enough to contain that trim window. Validation also
	// requires the timeline binding to be a real SHA256 value.
	plan.Assets[0].DurationUS = 40_000_000
	plan.TimelineSHA256 = strings.Repeat("c", 64)
	plan.FinalAudio.TimelineSHA256 = plan.TimelineSHA256
	return plan
}

func canonicalPlanForValidation(t *testing.T) ([]byte, *CompiledRenderPlanV2) {
	t.Helper()
	plan := validPlanForValidation()
	data, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	return data, plan
}

func requireValidationPath(t *testing.T, err error, wantPath string) {
	t.Helper()
	if err == nil {
		t.Fatalf("validation returned nil, want violation at %s", wantPath)
	}
	var violations CompiledRenderPlanV2ValidationErrors
	if !errors.As(err, &violations) {
		t.Fatalf("error type = %T, want CompiledRenderPlanV2ValidationErrors: %v", err, err)
	}
	for _, violation := range violations {
		if violation.Path == wantPath {
			return
		}
	}
	t.Fatalf("validation paths = %v, want %s", validationPaths(violations), wantPath)
}

func validationPaths(violations CompiledRenderPlanV2ValidationErrors) []string {
	paths := make([]string, 0, len(violations))
	for _, violation := range violations {
		paths = append(paths, violation.Path)
	}
	return paths
}

func TestValidateCompiledRenderPlanV2_AcceptsValidPlan(t *testing.T) {
	plan := validPlanForValidation()
	if err := ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	decoded, err := DecodeCompiledRenderPlanV2(canonical)
	if err != nil {
		t.Fatalf("DecodeCompiledRenderPlanV2: %v", err)
	}
	if decoded.PlanVersion != CompiledPlanVersionV2 || decoded.FinalAudio.Mode != AudioModeFinalAudioCopy {
		t.Fatalf("decoded plan identity = %+v", decoded)
	}
}

func TestValidateCompiledRenderPlanV2_RejectsCoreContractViolations(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		mutate func(*CompiledRenderPlanV2)
	}{
		{"version", "plan_version", func(plan *CompiledRenderPlanV2) { plan.PlanVersion = 1 }},
		{"timeline revision", "timeline_revision", func(plan *CompiledRenderPlanV2) { plan.TimelineRevision = 0 }},
		{"timeline hash", "timeline_sha256", func(plan *CompiledRenderPlanV2) { plan.TimelineSHA256 = "bad" }},
		{"duration", "duration_us", func(plan *CompiledRenderPlanV2) { plan.DurationUS = 0 }},
		{"output width", "output.width", func(plan *CompiledRenderPlanV2) { plan.Output.Width = 0 }},
		{"output fps", "output.fps_den", func(plan *CompiledRenderPlanV2) { plan.Output.FPSDen = 0 }},
		{"audio mode", "final_audio.mode", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.Mode = "AUDIO_MIX" }},
		{"audio codec", "final_audio.codec", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.Codec = "opus" }},
		{"audio sample rate", "final_audio.sample_rate_hz", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.SampleRateHz = 44_100 }},
		{"audio channels", "final_audio.channels", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.Channels = 1 }},
		{"audio hash", "final_audio.sha256", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.SHA256 = "bad" }},
		{"audio timeline revision", "final_audio.timeline_revision", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.TimelineRevision++ }},
		{"audio timeline hash", "final_audio.timeline_sha256", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.TimelineSHA256 = "other" }},
		{"audio duration", "final_audio.duration_us", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.DurationUS += CompiledPlanV2DurationToleranceUS + 1 }},
		{"asset hash", "assets[0].sha256", func(plan *CompiledRenderPlanV2) { plan.Assets[0].SHA256 = "bad" }},
		{"segment hash binding", "video_tracks[0].segments[0].sha256", func(plan *CompiledRenderPlanV2) { plan.VideoTracks[0].Segments[0].SHA256 = strings.Repeat("c", 64) }},
		{"segment source offset", "video_tracks[0].segments[0].source_range_out_of_bounds", func(plan *CompiledRenderPlanV2) { plan.VideoTracks[0].Segments[0].SourceInUS = 40_000_000 }},
		{"segment frame count", "video_tracks[0].segments[0].frame_count", func(plan *CompiledRenderPlanV2) { plan.VideoTracks[0].Segments[0].FrameCount = 0 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validPlanForValidation()
			test.mutate(plan)
			err := ValidateCompiledRenderPlanV2(plan)
			if test.path == "video_tracks[0].segments[0].source_range_out_of_bounds" {
				if err == nil || !strings.Contains(err.Error(), "source_range_out_of_bounds") {
					t.Fatalf("source range error = %v", err)
				}
				return
			}
			requireValidationPath(t, err, test.path)
		})
	}
}

func TestValidateCompiledRenderPlanV2_RejectsReferencesAndBounds(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		mutate func(*CompiledRenderPlanV2)
	}{
		{"missing final audio asset", "final_audio.asset_id", func(plan *CompiledRenderPlanV2) { plan.FinalAudio.AssetID = "missing-audio" }},
		{"wrong final audio kind", "final_audio.asset_id", func(plan *CompiledRenderPlanV2) { plan.Assets[1].Kind = "video" }},
		{"missing segment asset", "video_tracks[0].segments[0].asset_id", func(plan *CompiledRenderPlanV2) { plan.VideoTracks[0].Segments[0].AssetID = "missing-video" }},
		{"wrong segment kind", "video_tracks[0].segments[0].asset_id", func(plan *CompiledRenderPlanV2) { plan.Assets[0].Kind = "audio" }},
		{"timeline beyond output", "video_tracks[0].segments[0]", func(plan *CompiledRenderPlanV2) {
			plan.VideoTracks[0].Segments[0].TimelineStartFrame = 1_799
			plan.VideoTracks[0].Segments[0].FrameCount = 2
		}},
		{"duplicate asset", "assets[1].asset_id", func(plan *CompiledRenderPlanV2) { plan.Assets[1].AssetID = plan.Assets[0].AssetID }},
		{"duplicate segment", "video_tracks[1].segments[0].segment_id", func(plan *CompiledRenderPlanV2) {
			plan.VideoTracks = append(plan.VideoTracks, VideoTrackV2{TrackID: "second", Segments: []VideoSegmentV2{plan.VideoTracks[0].Segments[0]}})
		}},
		{"empty track", "video_tracks[0].segments", func(plan *CompiledRenderPlanV2) { plan.VideoTracks[0].Segments = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validPlanForValidation()
			test.mutate(plan)
			err := ValidateCompiledRenderPlanV2(plan)
			if err == nil || !strings.Contains(err.Error(), test.path) {
				t.Fatalf("validation error = %v, want path/issue containing %q", err, test.path)
			}
		})
	}
}

func TestDecodeCompiledRenderPlanV2_RejectsUnknownAndTrailingJSON(t *testing.T) {
	canonical, _ := canonicalPlanForValidation(t)
	cases := []struct {
		name string
		data string
	}{
		{
			name: "unknown root field",
			data: string(canonical[:len(canonical)-1]) + `,"unexpected":true}`,
		},
		{
			name: "unknown nested field",
			data: strings.Replace(string(canonical), `"output":{"container":"mp4"`, `"output":{"unexpected":true,"container":"mp4"`, 1),
		},
		{
			name: "duplicate root field",
			data: string(canonical[:len(canonical)-1]) + `,"plan_version":2}`,
		},
		{
			name: "trailing json value",
			data: string(canonical) + `{}`,
		},
		{
			name: "non-canonical whitespace",
			data: "\n" + string(canonical),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeCompiledRenderPlanV2([]byte(test.data)); err == nil {
				t.Fatal("invalid document was accepted")
			}
		})
	}
}

func TestValidateCompiledRenderPlanV2Payload_VerifiesExactEnvelopeSHA(t *testing.T) {
	canonical, _ := canonicalPlanForValidation(t)
	sum := sha256.Sum256(canonical)
	validSHA := hex.EncodeToString(sum[:])
	validPayload := map[string]interface{}{
		PayloadKeyCompiledRenderPlanJSON: string(canonical),
		PayloadKeyCompiledRenderPlanSHA:  validSHA,
	}
	if err := ValidateCompiledRenderPlanV2Payload(validPayload); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}

	cases := []struct {
		name    string
		payload map[string]interface{}
	}{
		{"missing json", map[string]interface{}{PayloadKeyCompiledRenderPlanSHA: validSHA}},
		{"missing sha", map[string]interface{}{PayloadKeyCompiledRenderPlanJSON: string(canonical)}},
		{"wrong json type", map[string]interface{}{PayloadKeyCompiledRenderPlanJSON: 42, PayloadKeyCompiledRenderPlanSHA: validSHA}},
		{"wrong sha type", map[string]interface{}{PayloadKeyCompiledRenderPlanJSON: string(canonical), PayloadKeyCompiledRenderPlanSHA: 42}},
		{"malformed sha", map[string]interface{}{PayloadKeyCompiledRenderPlanJSON: string(canonical), PayloadKeyCompiledRenderPlanSHA: "not-a-sha"}},
		{"mismatched sha", map[string]interface{}{PayloadKeyCompiledRenderPlanJSON: string(canonical), PayloadKeyCompiledRenderPlanSHA: strings.Repeat("f", 64)}},
		{"legacy absent", map[string]interface{}{"job_id": "legacy"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCompiledRenderPlanV2Payload(test.payload)
			if test.name == "legacy absent" {
				if err != nil {
					t.Fatalf("legacy payload rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
}

func TestValidateCompiledRenderPlanV2PayloadRejectsHashOfNonCanonicalDocument(t *testing.T) {
	canonical, _ := canonicalPlanForValidation(t)
	nonCanonical := "\n" + string(canonical)
	sum := sha256.Sum256([]byte(nonCanonical))
	payload := map[string]interface{}{
		PayloadKeyCompiledRenderPlanJSON: nonCanonical,
		PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(sum[:]),
	}
	if err := ValidateCompiledRenderPlanV2Payload(payload); err == nil {
		t.Fatal("non-canonical document with matching hash was accepted")
	}
}

func TestValidateCompiledRenderPlanV2_AcceptsDurationAtToleranceBoundary(t *testing.T) {
	plan := validPlanForValidation()
	plan.FinalAudio.DurationUS += CompiledPlanV2DurationToleranceUS
	plan.Assets[1].DurationUS = plan.FinalAudio.DurationUS
	if err := ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("duration at tolerance boundary rejected: %v", err)
	}
}
