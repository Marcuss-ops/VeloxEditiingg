package contract

import (
	"fmt"
	"strings"
)

// CanonicalVideoProfileVersionV1 is the version of the producer/Velox media
// compatibility contract. A profile is an identity, not a rendering hint:
// changing any field that can alter stream compatibility requires a new ID.
const CanonicalVideoProfileVersionV1 = 1

// CanonicalVideoProfileV1 describes the encoded video stream that may enter
// the packet-copy assembly path. Rendering systems outside Velox may use any
// implementation, but they must publish this exact stream contract.
type CanonicalVideoProfileV1 struct {
	ProfileID string `json:"profile_id"`

	Container string `json:"container"`
	Codec     string `json:"codec"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`

	FPSNum int `json:"fps_num"`
	FPSDen int `json:"fps_den"`

	PixelFormat  string `json:"pixel_format"`
	CodecProfile string `json:"codec_profile"`
	CodecLevel   string `json:"codec_level"`

	GOPSize   int  `json:"gop_size"`
	BFrames   int  `json:"b_frames"`
	ClosedGOP bool `json:"closed_gop"`

	TimeBaseNum int `json:"time_base_num"`
	TimeBaseDen int `json:"time_base_den"`

	Version int `json:"version"`
}

const CanonicalVideoProfileIDV1 = "VELOX_ASSEMBLY_READY_V1"

// CanonicalVideoProfileV1Default is the first profile admitted by Velox.
var CanonicalVideoProfileV1Default = CanonicalVideoProfileV1{
	ProfileID: CanonicalVideoProfileIDV1,
	Container: "mp4", Codec: "h264", Width: 1920, Height: 1080,
	FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p",
	CodecProfile: "high", CodecLevel: "4.0", GOPSize: 48,
	BFrames: 0, ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
	Version: CanonicalVideoProfileVersionV1,
}

func (p CanonicalVideoProfileV1) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return fmt.Errorf("canonical video profile: profile_id is required")
	}
	if p.Version != CanonicalVideoProfileVersionV1 {
		return fmt.Errorf("canonical video profile %q: unsupported version %d", p.ProfileID, p.Version)
	}
	if p.Container != "mp4" || p.Codec != "h264" {
		return fmt.Errorf("canonical video profile %q: container/codec must be mp4/h264", p.ProfileID)
	}
	if p.Width <= 0 || p.Height <= 0 || p.FPSNum <= 0 || p.FPSDen <= 0 {
		return fmt.Errorf("canonical video profile %q: dimensions and frame rate must be positive", p.ProfileID)
	}
	if strings.TrimSpace(p.PixelFormat) == "" || strings.TrimSpace(p.CodecProfile) == "" || strings.TrimSpace(p.CodecLevel) == "" {
		return fmt.Errorf("canonical video profile %q: pixel format/profile/level are required", p.ProfileID)
	}
	if p.GOPSize <= 0 || p.BFrames < 0 || p.TimeBaseNum <= 0 || p.TimeBaseDen <= 0 {
		return fmt.Errorf("canonical video profile %q: GOP, B-frames and time base are invalid", p.ProfileID)
	}
	return nil
}

// KnownCanonicalVideoProfileV1 resolves only profiles explicitly admitted by
// Velox. Unknown producer profiles fail closed at admission.
func KnownCanonicalVideoProfileV1(profileID string) (CanonicalVideoProfileV1, error) {
	if strings.TrimSpace(profileID) == CanonicalVideoProfileV1Default.ProfileID {
		return CanonicalVideoProfileV1Default, nil
	}
	return CanonicalVideoProfileV1{}, fmt.Errorf("canonical video profile %q is not registered", profileID)
}

// MatchesOutput checks the legacy V2 output fields as well as the strengthened
// profile fields. Empty optional fields are accepted for legacy render_batch;
// the prepared copy-only executor requires a complete profile separately.
func (p CanonicalVideoProfileV1) MatchesOutput(output OutputContractV2) error {
	if err := p.Validate(); err != nil {
		return err
	}
	codec := strings.ToLower(strings.TrimSpace(output.VideoCodec))
	if output.Container != p.Container || (codec != p.Codec && !(codec == "libx264" && p.Codec == "h264")) || output.Width != p.Width || output.Height != p.Height || output.FPSNum != p.FPSNum || output.FPSDen != p.FPSDen || output.PixelFormat != p.PixelFormat {
		return fmt.Errorf("canonical video profile %q does not match output contract", p.ProfileID)
	}
	if output.ProfileID != "" && output.ProfileID != p.ProfileID {
		return fmt.Errorf("canonical video profile %q does not match output profile_id %q", p.ProfileID, output.ProfileID)
	}
	return nil
}
