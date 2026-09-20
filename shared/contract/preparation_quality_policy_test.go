package contract

import (
	"reflect"
	"testing"
)

// TestPreparationQualityPolicyDefaultValidates pins the canonical quality
// policy so a partially specified policy can never reach ffmpeg.
func TestPreparationQualityPolicyDefaultValidates(t *testing.T) {
	if err := CanonicalPreparationQualityPolicyDefault.Validate(); err != nil {
		t.Fatalf("canonical preparation quality policy invalid: %v", err)
	}
}

// TestPreparationQualityPolicyValidateFailsClosed covers every required knob;
// an empty bitrate would otherwise become a malformed ffmpeg argument and
// silently produce a stream nobody can certify.
func TestPreparationQualityPolicyValidateFailsClosed(t *testing.T) {
	mutations := map[string]func(*PreparationQualityPolicy){
		"video_bitrate":     func(q *PreparationQualityPolicy) { q.VideoBitrate = "" },
		"video_maxrate":     func(q *PreparationQualityPolicy) { q.VideoMaxRate = "" },
		"video_bufsize":     func(q *PreparationQualityPolicy) { q.VideoBufferSize = "  " },
		"audio_codec":       func(q *PreparationQualityPolicy) { q.AudioCodec = "" },
		"audio_bitrate":     func(q *PreparationQualityPolicy) { q.AudioBitrate = "" },
		"audio_sample_rate": func(q *PreparationQualityPolicy) { q.AudioSampleRate = 0 },
		"audio_channels":    func(q *PreparationQualityPolicy) { q.AudioChannels = 0 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			policy := CanonicalPreparationQualityPolicyDefault
			mutate(&policy)
			if err := policy.Validate(); err == nil {
				t.Fatalf("policy with unset %s validated: %+v", name, policy)
			}
		})
	}
}

// TestPreparationQualityPolicyCarriesNoCompatibilityFields is the structural
// half of the single-authority rule: compatibility (dimensions, frame rate,
// pixel format, codec profile/level, GOP, B-frames, closed GOP, time base)
// lives in CanonicalVideoProfileV1 only. If this test fails, a compatibility
// field was added back to the quality policy and the drift that produced the
// historical 24 fps / 30 fps split can return.
func TestPreparationQualityPolicyCarriesNoCompatibilityFields(t *testing.T) {
	// The canonical policy is fully described by these quality knobs; anything
	// else here is compatibility drift.
	qualityKnobs := map[string]bool{
		"VideoBitrate":    true,
		"VideoMaxRate":    true,
		"VideoBufferSize": true,
		"AudioCodec":      true,
		"AudioBitrate":    true,
		"AudioSampleRate": true,
		"AudioChannels":   true,
	}
	structType := reflect.TypeOf(PreparationQualityPolicy{})
	for i := 0; i < structType.NumField(); i++ {
		name := structType.Field(i).Name
		if !qualityKnobs[name] {
			t.Errorf("PreparationQualityPolicy declares %q, which is not a quality knob (compatibility belongs to CanonicalVideoProfileV1)", name)
		}
	}
	// The profile must still carry every compatibility field the quality
	// policy is forbidden from owning.
	profileType := reflect.TypeOf(CanonicalVideoProfileV1{})
	compatibilityFields := []string{"Width", "Height", "FPSNum", "FPSDen", "PixelFormat", "CodecProfile", "CodecLevel", "GOPSize", "BFrames", "ClosedGOP", "TimeBaseNum", "TimeBaseDen"}
	present := make(map[string]bool, profileType.NumField())
	for i := 0; i < profileType.NumField(); i++ {
		present[profileType.Field(i).Name] = true
	}
	for _, field := range compatibilityFields {
		if !present[field] {
			t.Errorf("CanonicalVideoProfileV1 is missing compatibility field %q", field)
		}
	}
}
