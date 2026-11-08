package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

const (
	VideoPrepareWindowPayloadKey = "video_prepare_window"
	VideoPrepareWindowVersion    = 1
	VideoPrepareWindowExecutorID = "video.prepare.window.v1"
)

// VideoPrepareWindowV1 is the immutable input contract for worker-side
// canonical window preparation. Source remains a path-free asset reference;
// frame coordinates are source-frame indices, while OutputFrameCount is the
// exact number of canonical output frames to encode.
type VideoPrepareWindowV1 struct {
	Version int        `json:"version"`
	Source  AssetRefV2 `json:"source"`
	// SourceIdentityDiscovery asks the worker to establish the immutable
	// source SHA-256 by downloading and verifying the declared source once.
	// The Master uses it only when the source identity is not yet certified;
	// the worker returns the discovered SHA in its certificate and registers
	// the prepared fragment under the content identity built from it.
	SourceIdentityDiscovery bool   `json:"source_identity_discovery,omitempty"`
	SourceStartFrame        int64  `json:"source_start_frame"`
	SourceFrameCount        int64  `json:"source_frame_count"`
	SourceFPSNum            int    `json:"source_fps_num"`
	SourceFPSDen            int    `json:"source_fps_den"`
	SourceStartUS           int64  `json:"source_start_us,omitempty"`
	SourceDurationUS        int64  `json:"source_duration_us,omitempty"`
	OutputFrameCount        int64  `json:"output_frame_count"`
	TimelineRevision        int64  `json:"timeline_revision"`
	TimelineSHA256          string `json:"timeline_sha256"`
	TimelineStartFrame      int64  `json:"timeline_start_frame"`
	OutputProfileID         string `json:"output_profile_id"`
	OutputProfileRevision   string `json:"output_profile_revision"`
}

// VideoPrepareWindowTaskV1 is a Master-side deterministic fan-out entry.
// Identity is the content-preparation key; SegmentIDs lists every timeline
// placement that reuses that one fragment task.
type VideoPrepareWindowTaskV1 struct {
	Identity   string               `json:"identity"`
	Request    VideoPrepareWindowV1 `json:"request"`
	SegmentIDs []string             `json:"segment_ids"`
}

// VideoWindowOutputEvidenceV1 is the worker's ffprobe evidence for the
// canonical fragment bitstream. The Master rechecks the fields available from
// its independent probe before registering the immutable blob.
type VideoWindowOutputEvidenceV1 struct {
	Codec              string `json:"codec"`
	Profile            string `json:"profile"`
	Level              int    `json:"level"`
	Width              int    `json:"width"`
	Height             int    `json:"height"`
	PixelFormat        string `json:"pixel_format"`
	FPSNum             int    `json:"fps_num"`
	FPSDen             int    `json:"fps_den"`
	TimeBaseNum        int    `json:"time_base_num"`
	TimeBaseDen        int    `json:"time_base_den"`
	BFrames            int    `json:"b_frames"`
	FrameCount         int64  `json:"frame_count"`
	FirstFrameKeyframe bool   `json:"first_frame_keyframe"`
	ExtradataSHA256    string `json:"extradata_sha256"`
}

// VideoPrepareWindowCertificateV1 binds a prepared content blob and its
// independent stream evidence to the immutable source window and profile.
// Timeline fields describe the attempt that emitted the certificate; they do
// not participate in VideoPrepareWindowCacheIdentity.
type VideoPrepareWindowCertificateV1 struct {
	Version               int                         `json:"version"`
	SourceSHA256          string                      `json:"source_sha256"`
	SourceStartFrame      int64                       `json:"source_start_frame"`
	SourceFrameCount      int64                       `json:"source_frame_count"`
	SourceFPSNum          int                         `json:"source_fps_num"`
	SourceFPSDen          int                         `json:"source_fps_den"`
	SourceStartUS         int64                       `json:"source_start_us,omitempty"`
	SourceDurationUS      int64                       `json:"source_duration_us,omitempty"`
	OutputFrameCount      int64                       `json:"output_frame_count"`
	OutputProfileID       string                      `json:"output_profile_id"`
	OutputProfileRevision string                      `json:"output_profile_revision"`
	EncoderVersion        string                      `json:"encoder_version"`
	EncoderBuildSHA256    string                      `json:"encoder_build_sha256"`
	OutputSHA256          string                      `json:"output_sha256"`
	OutputSizeBytes       int64                       `json:"output_size_bytes"`
	OutputEvidence        VideoWindowOutputEvidenceV1 `json:"output_evidence"`
	ClosedGOPAttested     bool                        `json:"closed_gop_attested"`
	SourceAssetID         string                      `json:"source_asset_id"`
	SourceSizeBytes       int64                       `json:"source_size_bytes"`
	TimelineRevision      int64                       `json:"timeline_revision"`
	TimelineSHA256        string                      `json:"timeline_sha256"`
	TimelineStartFrame    int64                       `json:"timeline_start_frame"`
	CacheHit              bool                        `json:"cache_hit"`
}

func (c VideoPrepareWindowCertificateV1) ValidateFor(r VideoPrepareWindowV1) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if c.Version != VideoPrepareWindowVersion ||
		c.SourceAssetID != r.Source.AssetID || c.SourceSizeBytes != r.Source.SizeBytes ||
		c.SourceStartFrame != r.SourceStartFrame || c.SourceFrameCount != r.SourceFrameCount ||
		c.SourceFPSNum != r.SourceFPSNum || c.SourceFPSDen != r.SourceFPSDen ||
		c.SourceStartUS != r.SourceStartUS || c.SourceDurationUS != r.SourceDurationUS ||
		c.OutputFrameCount != r.OutputFrameCount || c.OutputProfileID != r.OutputProfileID ||
		c.OutputProfileRevision != r.OutputProfileRevision || c.TimelineRevision != r.TimelineRevision ||
		c.TimelineSHA256 != r.TimelineSHA256 || c.TimelineStartFrame != r.TimelineStartFrame {
		return fmt.Errorf("video.prepare.window.v1: certificate identity does not match request")
	}
	if r.SourceIdentityDiscovery {
		if !isLowerSHA256(c.SourceSHA256) {
			return fmt.Errorf("video.prepare.window.v1: discovered source SHA-256 is required in the certificate")
		}
	} else if c.SourceSHA256 != r.Source.SHA256 {
		return fmt.Errorf("video.prepare.window.v1: certificate identity does not match request")
	}
	if strings.TrimSpace(c.EncoderVersion) == "" || !isLowerSHA256(c.EncoderBuildSHA256) ||
		!isLowerSHA256(c.OutputSHA256) || c.OutputSizeBytes <= 0 || !c.ClosedGOPAttested {
		return fmt.Errorf("video.prepare.window.v1: certificate lacks encoder, output, or closed-GOP evidence")
	}
	e := c.OutputEvidence
	if e.Codec != "h264" || e.FrameCount != r.OutputFrameCount || !e.FirstFrameKeyframe || !isLowerSHA256(e.ExtradataSHA256) ||
		e.Width <= 0 || e.Height <= 0 || e.FPSNum <= 0 || e.FPSDen <= 0 || e.TimeBaseNum <= 0 || e.TimeBaseDen <= 0 {
		return fmt.Errorf("video.prepare.window.v1: certificate output evidence is incomplete")
	}
	return nil
}

func (r VideoPrepareWindowV1) Validate() error {
	if r.Version != VideoPrepareWindowVersion {
		return fmt.Errorf("video.prepare.window.v1: unsupported version %d", r.Version)
	}
	if strings.TrimSpace(r.Source.AssetID) == "" || strings.TrimSpace(r.Source.Kind) != "video" {
		return fmt.Errorf("video.prepare.window.v1: video source asset_id and kind are required")
	}
	if r.SourceIdentityDiscovery {
		if strings.TrimSpace(r.Source.SHA256) != "" {
			return fmt.Errorf("video.prepare.window.v1: source identity discovery forbids a pre-declared SHA-256")
		}
	} else if !isLowerSHA256(r.Source.SHA256) || len(r.Source.SHA256) != sha256.Size*2 {
		return fmt.Errorf("video.prepare.window.v1: source SHA-256 is required")
	}
	if r.Source.SizeBytes <= 0 || r.OutputFrameCount <= 0 {
		return fmt.Errorf("video.prepare.window.v1: source size and output frame count are required")
	}
	frameWindow := r.SourceStartFrame >= 0 && r.SourceFrameCount > 0 && r.SourceFPSNum > 0 && r.SourceFPSDen > 0 && r.SourceStartUS == 0 && r.SourceDurationUS == 0
	timeWindow := r.SourceStartUS >= 0 && r.SourceDurationUS > 0 && r.SourceStartFrame == 0 && r.SourceFrameCount == 0 && r.SourceFPSNum == 0 && r.SourceFPSDen == 0
	if frameWindow == timeWindow {
		return fmt.Errorf("video.prepare.window.v1: provide exactly one valid source frame or timestamp interval")
	}
	if r.TimelineRevision <= 0 || r.TimelineStartFrame < 0 || !isLowerSHA256(r.TimelineSHA256) {
		return fmt.Errorf("video.prepare.window.v1: valid timeline identity and frame placement are required")
	}
	if strings.TrimSpace(r.OutputProfileID) == "" || !strings.HasPrefix(r.OutputProfileRevision, "video-window-preparer.v1:") {
		return fmt.Errorf("video.prepare.window.v1: output profile identity and revision are required")
	}
	revisionSHA := strings.TrimPrefix(r.OutputProfileRevision, "video-window-preparer.v1:")
	if !isLowerSHA256(revisionSHA) {
		return fmt.Errorf("video.prepare.window.v1: output profile revision must include a lowercase SHA-256")
	}
	if frameWindow && r.SourceFrameCount > math.MaxInt64-r.SourceStartFrame {
		return fmt.Errorf("video.prepare.window.v1: source frame interval overflows")
	}
	if timeWindow && r.SourceDurationUS > math.MaxInt64-r.SourceStartUS {
		return fmt.Errorf("video.prepare.window.v1: source timestamp interval overflows")
	}
	return nil
}

// CanonicalJSON validates and returns the stable wire representation used in
// TaskSpec hashing and task retry identity.
func (r VideoPrepareWindowV1) CanonicalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

// VideoPrepareWindowCacheIdentity is the canonical content-preparation key.
// Timeline placement is deliberately excluded so an identical certified
// source window can be reused at another position or in another job.
func VideoPrepareWindowCacheIdentity(r VideoPrepareWindowV1) (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	if r.SourceIdentityDiscovery {
		return "", fmt.Errorf("video.prepare.window.v1: content cache identity requires a discovered source SHA-256")
	}
	identity := struct {
		SourceSHA        string `json:"source_sha256"`
		Start            int64  `json:"source_start_frame"`
		Count            int64  `json:"source_frame_count"`
		SourceFPSNum     int    `json:"source_fps_num"`
		SourceFPSDen     int    `json:"source_fps_den"`
		SourceStartUS    int64  `json:"source_start_us,omitempty"`
		SourceDurationUS int64  `json:"source_duration_us,omitempty"`
		OutputFrames     int64  `json:"output_frame_count"`
		ProfileID        string `json:"output_profile_id"`
		ProfileRevision  string `json:"profile_revision"`
	}{r.Source.SHA256, r.SourceStartFrame, r.SourceFrameCount, r.SourceFPSNum, r.SourceFPSDen,
		r.SourceStartUS, r.SourceDurationUS, r.OutputFrameCount, r.OutputProfileID, r.OutputProfileRevision}
	data, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("video.prepare.window.v1: encode cache identity: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// VideoPrepareWindowDiscoveryIdentity is the deterministic identity used by
// the Master while a source SHA-256 is still unknown. It binds the declared
// source (asset, Drive id, size) and window so duplicate placements share one
// discovery task; it is never used as a content-cache key.
func VideoPrepareWindowDiscoveryIdentity(r VideoPrepareWindowV1) (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	if !r.SourceIdentityDiscovery {
		return "", fmt.Errorf("video.prepare.window.v1: discovery identity requires source identity discovery")
	}
	identity := struct {
		Discovery        bool   `json:"source_identity_discovery"`
		AssetID          string `json:"source_asset_id"`
		DriveFileID      string `json:"source_drive_file_id,omitempty"`
		SizeBytes        int64  `json:"source_size_bytes"`
		Start            int64  `json:"source_start_frame"`
		Count            int64  `json:"source_frame_count"`
		SourceFPSNum     int    `json:"source_fps_num"`
		SourceFPSDen     int    `json:"source_fps_den"`
		SourceStartUS    int64  `json:"source_start_us,omitempty"`
		SourceDurationUS int64  `json:"source_duration_us,omitempty"`
		OutputFrames     int64  `json:"output_frame_count"`
		ProfileID        string `json:"output_profile_id"`
		ProfileRevision  string `json:"profile_revision"`
	}{true, r.Source.AssetID, r.Source.DriveFileID, r.Source.SizeBytes,
		r.SourceStartFrame, r.SourceFrameCount, r.SourceFPSNum, r.SourceFPSDen,
		r.SourceStartUS, r.SourceDurationUS, r.OutputFrameCount, r.OutputProfileID, r.OutputProfileRevision}
	data, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("video.prepare.window.v1: encode discovery identity: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// VideoWindowPreparationRevision binds cached fragments to the algorithm,
// canonical bitstream identity and quality settings used to create them.
func VideoWindowPreparationRevision(profile CanonicalVideoProfileV1, quality PreparationQualityPolicy) (string, error) {
	if err := profile.Validate(); err != nil {
		return "", err
	}
	if err := quality.Validate(); err != nil {
		return "", err
	}
	encoderArgsDigest, err := CanonicalVideoEncoderArgsDigest(profile, quality)
	if err != nil {
		return "", err
	}
	identity := struct {
		Algorithm         string                   `json:"algorithm"`
		Profile           CanonicalVideoProfileV1  `json:"profile"`
		Quality           PreparationQualityPolicy `json:"quality"`
		EncoderArgsSHA256 string                   `json:"encoder_args_sha256"`
	}{Algorithm: "video-window-preparer.v1", Profile: profile, Quality: quality, EncoderArgsSHA256: encoderArgsDigest}
	data, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "video-window-preparer.v1:" + hex.EncodeToString(sum[:]), nil
}
