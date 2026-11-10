package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// CanonicalVideoExtradataSHA256 is the SHA-256 of the exact MP4 avcC payload
// (SPS + PPS + VUI) that every packet-copy assembly input must carry. Profile,
// level, geometry and pixel format are not enough: two High 4.0 streams can
// carry different parameter sets, and concat -c copy writes only the first
// segment's avcC.
const CanonicalVideoExtradataSHA256 = "7201ce80a2b2b32a0c1a89501851099f267c1767d59ff799db744f72032aae5a"

// CanonicalVideoProfileRevision names the exact encoder lane behind the
// canonical avcC. It is folded into cache identities so an encoder-settings
// change cannot reuse bytes whose parameter sets happen to hash the same.
const CanonicalVideoProfileRevision = "velox-avc1-high40-ref1-bt709-v1"

// CanonicalVideoStreamSignatureV1 is the stream-level identity shared by the
// producer (RenderingGen), the transport (PipelineGen) and the assembler
// (Velox). It deliberately excludes packaging and audio layout: an overlay
// source is video-only and is packaged as an overlay artifact, while the
// canonical assembly clip has audio. Comparing only these fields answers the
// copy-compatibility question without pretending the two formats are equal.
//
// The JSON encoding is the canonical serialization. Every participant hashes
// the same field order and names, so a signature produced by one repository
// can be compared byte-for-byte by another.
type CanonicalVideoStreamSignatureV1 struct {
	Version         int    `json:"version"`
	Codec           string `json:"codec"`
	CodecProfile    string `json:"codec_profile"`
	CodecLevel      string `json:"codec_level"`
	PixelFormat     string `json:"pixel_format"`
	Width           int    `json:"width"`
	Height          int    `json:"height"`
	FPSNum          int    `json:"fps_num"`
	FPSDen          int    `json:"fps_den"`
	TimeBaseNum     int    `json:"time_base_num"`
	TimeBaseDen     int    `json:"time_base_den"`
	GOPSize         int    `json:"gop_size"`
	BFrames         int    `json:"b_frames"`
	ClosedGOP       bool   `json:"closed_gop"`
	ExtradataSHA256 string `json:"extradata_sha256"`
	EncoderRevision string `json:"encoder_revision"`
}

// CanonicalVideoStreamSignatureV1Default returns the video stream identity of
// CanonicalVideoProfileV1Default.
func CanonicalVideoStreamSignatureV1Default() CanonicalVideoStreamSignatureV1 {
	p := CanonicalVideoProfileV1Default
	return CanonicalVideoStreamSignatureV1{
		Version:         1,
		Codec:           p.Codec,
		CodecProfile:    p.CodecProfile,
		CodecLevel:      p.CodecLevel,
		PixelFormat:     p.PixelFormat,
		Width:           p.Width,
		Height:          p.Height,
		FPSNum:          p.FPSNum,
		FPSDen:          p.FPSDen,
		TimeBaseNum:     p.TimeBaseNum,
		TimeBaseDen:     p.TimeBaseDen,
		GOPSize:         p.GOPSize,
		BFrames:         p.BFrames,
		ClosedGOP:       p.ClosedGOP,
		ExtradataSHA256: CanonicalVideoExtradataSHA256,
		EncoderRevision: CanonicalVideoProfileRevision,
	}
}

// Validate fails closed on a signature that cannot identify a canonical
// packet-copy stream. An empty extradata hash is never a wildcard.
func (s CanonicalVideoStreamSignatureV1) Validate() error {
	if s.Version != 1 {
		return fmt.Errorf("canonical video stream signature: unsupported version %d", s.Version)
	}
	if strings.TrimSpace(s.Codec) == "" || strings.TrimSpace(s.CodecProfile) == "" ||
		strings.TrimSpace(s.CodecLevel) == "" || strings.TrimSpace(s.PixelFormat) == "" {
		return fmt.Errorf("canonical video stream signature: codec identity is incomplete")
	}
	if s.Width <= 0 || s.Height <= 0 || s.FPSNum <= 0 || s.FPSDen <= 0 ||
		s.TimeBaseNum <= 0 || s.TimeBaseDen <= 0 || s.GOPSize <= 0 || s.BFrames < 0 {
		return fmt.Errorf("canonical video stream signature: geometry, cadence or GOP is invalid")
	}
	if !isLowerSHA256(s.ExtradataSHA256) {
		return fmt.Errorf("canonical video stream signature: extradata_sha256 must be a lowercase SHA-256")
	}
	if strings.TrimSpace(s.EncoderRevision) == "" {
		return fmt.Errorf("canonical video stream signature: encoder_revision is required")
	}
	return nil
}

// SHA256 is the canonical content identity of the stream signature.
func (s CanonicalVideoStreamSignatureV1) SHA256() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("canonical video stream signature: encode: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
