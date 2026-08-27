package contract

import (
	"fmt"
	"os"
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

	// ContainerLayout is the MP4 byte layout the profile promises. "progressive"
	// (or empty, the legacy default) is a single-moov file where the muxer
	// seeks backward to rewrite the header — the first prefix is NOT immutable
	// while the trailer runs. "fragmented" is fMP4 (moof/mdat fragments): the
	// muxer writes strictly append-only, so every written prefix is immutable,
	// safe_offset == high_watermark, and an incremental SHA over the final
	// bytes is always valid (no backward seek can invalidate it). This is a
	// stream-compatibility identity field: a layout change requires a new
	// profile ID.
	ContainerLayout string `json:"container_layout"`

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
	ContainerLayout: ContainerLayoutProgressive,
	Version:         CanonicalVideoProfileVersionV1,
}

// ContainerLayout values. "" is accepted as the legacy alias for
// ContainerLayoutProgressive so producers that predate the field keep
// validating.
const (
	ContainerLayoutProgressive = "progressive"
	ContainerLayoutFragmented  = "fragmented"

	// CanonicalVideoProfileFMP4StreamV1 is the fragmented-MP4 streaming
	// profile: same H.264 stream identity as the canonical copy profile, but a
	// container layout the muxer can write strictly append-only. It is
	// REGISTERED but DISABLED by default: admission is refused until the
	// 100-job benchmark confirms the progressive-upload gains that the layout
	// enables (append-only writes → safe_offset == high_watermark → always
	// valid incremental SHA → immediate chunk upload while the render runs).
	// Flip VELOX_FMP4_STREAM_PROFILE=1 only after that benchmark confirms the
	// gains AND the engine mux actually emits fragmented output (see the
	// fail-closed guard in video.assemble.copy.v1).
	CanonicalVideoProfileFMP4StreamV1 = "velox-h264-fmp4-stream-v1"
)

// CanonicalVideoProfileFMP4StreamV1Default is the registered fMP4 streaming
// profile. It intentionally mirrors the canonical copy profile's stream
// identity (same codec/dimensions/GOP/time base) and differs only in the
// container layout — that difference is what makes the byte stream
// append-only and unlocks progressive upload.
var CanonicalVideoProfileFMP4StreamV1Default = CanonicalVideoProfileV1{
	ProfileID: CanonicalVideoProfileFMP4StreamV1,
	Container: "mp4", Codec: "h264", Width: 1920, Height: 1080,
	FPSNum: 24, FPSDen: 1, PixelFormat: "yuv420p",
	CodecProfile: "high", CodecLevel: "4.0", GOPSize: 48,
	BFrames: 0, ClosedGOP: true, TimeBaseNum: 1, TimeBaseDen: 90000,
	ContainerLayout: ContainerLayoutFragmented,
	Version:         CanonicalVideoProfileVersionV1,
}

// FMP4StreamProfileEnabled reports whether the fragmented-MP4 streaming
// profile has been admitted. It is the operator gate that opens ONLY after
// the 100-job benchmark confirms the progressive-upload gains; until then the
// profile stays registered-but-DISABLED and every admission point fails
// closed.
func FMP4StreamProfileEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("VELOX_FMP4_STREAM_PROFILE"))) {
	case "1", "true", "enabled":
		return true
	default:
		return false
	}
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
	layout := strings.ToLower(strings.TrimSpace(p.ContainerLayout))
	if layout == "" {
		layout = ContainerLayoutProgressive // legacy producers predate the field
	}
	if layout != ContainerLayoutProgressive && layout != ContainerLayoutFragmented {
		return fmt.Errorf("canonical video profile %q: container_layout must be %q or %q", p.ProfileID, ContainerLayoutProgressive, ContainerLayoutFragmented)
	}
	return nil
}

// KnownCanonicalVideoProfileV1 resolves only profiles explicitly admitted by
// Velox. Unknown producer profiles fail closed at admission, and the
// registered-but-DISABLED fMP4 streaming profile is refused until the
// 100-job benchmark gate opens (FMP4StreamProfileEnabled).
func KnownCanonicalVideoProfileV1(profileID string) (CanonicalVideoProfileV1, error) {
	trimmed := strings.TrimSpace(profileID)
	if trimmed == CanonicalVideoProfileV1Default.ProfileID {
		return CanonicalVideoProfileV1Default, nil
	}
	if trimmed == CanonicalVideoProfileFMP4StreamV1 {
		if !FMP4StreamProfileEnabled() {
			return CanonicalVideoProfileV1{}, fmt.Errorf("canonical video profile %q is registered but DISABLED pending the 100-job benchmark gate (VELOX_FMP4_STREAM_PROFILE)", trimmed)
		}
		return CanonicalVideoProfileFMP4StreamV1Default, nil
	}
	return CanonicalVideoProfileV1{}, fmt.Errorf("canonical video profile %q is not registered", trimmed)
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
