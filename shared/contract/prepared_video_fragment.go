package contract

import (
	"fmt"
	"strings"
)

const PreparedVideoFragmentSchemaVersionV1 = 1

// PreparedVideoFragmentManifestV1 is the only contract Velox needs from an
// external renderer. It describes a verified, already-encoded asset and its
// binding to one immutable editorial timeline.
type PreparedVideoFragmentManifestV1 struct {
	SchemaVersion int `json:"schema_version"`

	ArtifactID string `json:"artifact_id"`
	AssetKey   string `json:"asset_key"`

	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`

	ProfileID string `json:"profile_id"`

	DurationUS int64 `json:"duration_us"`
	FrameCount int64 `json:"frame_count"`

	TimelineRevision int64  `json:"timeline_revision"`
	TimelineSHA256   string `json:"timeline_sha256"`

	TimelineStartFrame int64 `json:"timeline_start_frame"`

	FirstFrameKeyframe bool `json:"first_frame_keyframe"`
	ClosedGOP          bool `json:"closed_gop"`
}

func (m PreparedVideoFragmentManifestV1) Validate() error {
	if m.SchemaVersion != PreparedVideoFragmentSchemaVersionV1 {
		return fmt.Errorf("prepared video fragment: unsupported schema_version %d", m.SchemaVersion)
	}
	if strings.TrimSpace(m.ArtifactID) == "" || strings.TrimSpace(m.AssetKey) == "" {
		return fmt.Errorf("prepared video fragment: artifact_id and asset_key are required")
	}
	if !isLowerSHA256(m.SHA256) {
		return fmt.Errorf("prepared video fragment: sha256 must be 64 lowercase hexadecimal characters")
	}
	if m.SizeBytes <= 0 || m.DurationUS <= 0 || m.FrameCount <= 0 {
		return fmt.Errorf("prepared video fragment: size, duration and frame_count must be positive")
	}
	if strings.TrimSpace(m.ProfileID) == "" {
		return fmt.Errorf("prepared video fragment: profile_id is required")
	}
	if m.TimelineRevision <= 0 || !isLowerSHA256(m.TimelineSHA256) {
		return fmt.Errorf("prepared video fragment: timeline binding is invalid")
	}
	if m.TimelineStartFrame < 0 {
		return fmt.Errorf("prepared video fragment: timeline_start_frame must be non-negative")
	}
	if !m.FirstFrameKeyframe {
		return fmt.Errorf("prepared video fragment: first_frame_keyframe must be true")
	}
	if !m.ClosedGOP {
		return fmt.Errorf("prepared video fragment: closed_gop must be true")
	}
	return nil
}

// AssetRefV2 converts the external manifest into the worker-facing typed
// asset reference. The timeline-specific fields remain explicit so the
// packet-copy executor can certify the binding without understanding the
// producer's rendering process.
func (m PreparedVideoFragmentManifestV1) AssetRefV2(mime string) (AssetRefV2, error) {
	if err := m.Validate(); err != nil {
		return AssetRefV2{}, err
	}
	return AssetRefV2{
		AssetID: m.ArtifactID, AssetKey: m.AssetKey, SHA256: m.SHA256, SizeBytes: m.SizeBytes,
		Kind: "prepared_video_fragment", MIME: mime, DurationUS: m.DurationUS,
		ProfileID: m.ProfileID, FrameCount: m.FrameCount,
		TimelineRevision: m.TimelineRevision, TimelineSHA256: m.TimelineSHA256,
		TimelineStartFrame: m.TimelineStartFrame, FirstFrameKeyframe: m.FirstFrameKeyframe,
		ClosedGOP: m.ClosedGOP,
	}, nil
}
