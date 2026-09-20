package contract

import (
	"fmt"
	"strings"
)

// PreparationQualityPolicy carries the master-side encoding QUALITY knobs used
// when a source must be produced (or re-produced) into the canonical stream
// identity described by CanonicalVideoProfileV1.
//
// Why this type exists (and why it is separate from the profile):
//
//	compatibility != quality.
//
// CanonicalVideoProfileV1 is an IDENTITY: any change to its fields produces a
// different encoded stream, so it requires a new profile ID and invalidates
// packet-copy reuse. Bitrate/VBV/audio-quality knobs are tuning choices: they
// change the produced bytes but not the stream identity, and they must be
// allowed to move without registering a new profile.
//
// Single-authority rule: this type MUST NOT re-declare a compatibility field.
// Resolution, frame rate, pixel format, codec profile/level, GOP, B-frames and
// time base live in CanonicalVideoProfileV1 only. A preparation path that
// hardcodes any of them (the historical VideoNormalization 30 fps vs the
// canonical 24 fps drift) is a regression and must fail CI — see
// scripts/ci/check-architecture.sh rule 14 and
// TestVideoPreparationUsesCanonicalStreamProfile.
type PreparationQualityPolicy struct {
	VideoBitrate    string
	VideoMaxRate    string
	VideoBufferSize string

	AudioCodec      string
	AudioBitrate    string
	AudioSampleRate int
	AudioChannels   int
}

// CanonicalPreparationQualityPolicyDefault is the quality policy paired with
// CanonicalVideoProfileV1Default. The video ceiling (VideoMaxRate) is what
// decides whether an already-encoded source may be packet-copied or must be
// re-encoded to fit the quality budget.
var CanonicalPreparationQualityPolicyDefault = PreparationQualityPolicy{
	VideoBitrate:    "2.25M",
	VideoMaxRate:    "2.25M",
	VideoBufferSize: "4.5M",
	AudioCodec:      "aac",
	AudioBitrate:    "128k",
	AudioSampleRate: 48000,
	AudioChannels:   2,
}

// Validate fails closed on a partially specified quality policy: an empty
// bitrate string would otherwise reach ffmpeg as a malformed argument and
// produce an asset whose stream identity silently differs from the profile.
func (q PreparationQualityPolicy) Validate() error {
	if strings.TrimSpace(q.VideoBitrate) == "" || strings.TrimSpace(q.VideoMaxRate) == "" || strings.TrimSpace(q.VideoBufferSize) == "" {
		return fmt.Errorf("preparation quality policy: video bitrate/maxrate/bufsize are required")
	}
	if strings.TrimSpace(q.AudioCodec) == "" || strings.TrimSpace(q.AudioBitrate) == "" {
		return fmt.Errorf("preparation quality policy: audio codec/bitrate are required")
	}
	if q.AudioSampleRate <= 0 || q.AudioChannels <= 0 {
		return fmt.Errorf("preparation quality policy: audio sample rate/channels must be positive")
	}
	return nil
}
