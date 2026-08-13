package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// CompiledPlanVersionV2 identifies the producer-compiled execution-plan
// contract. CompiledRenderPlanV2 intentionally lives beside, rather than
// replacing, the existing V1 master/worker render-plan mirrors.
const CompiledPlanVersionV2 = 2

// AudioModeFinalAudioCopyV2 declares that the final audio asset is already
// mixed and must be muxed without another audio mix or encode.
const (
	// AudioModeFinalAudioCopy is the wire value for an already-finalized
	// audio asset that must be muxed without another mix or encode.
	AudioModeFinalAudioCopy = "FINAL_AUDIO_COPY"

	// AudioModeFinalAudioCopyV2 is retained as an explicit V2-named alias for
	// callers that prefer versioned constants; both names have one wire value.
	AudioModeFinalAudioCopyV2 = AudioModeFinalAudioCopy
)

// CompiledRenderPlanV2 is the canonical, producer-compiled execution plan.
// It contains editorial decisions and verified asset identity only; job and
// attempt identity remain in the surrounding TaskSpec so retries and workers
// can consume the same plan bytes and plan hash.
type CompiledRenderPlanV2 struct {
	PlanVersion      int              `json:"plan_version"`
	TimelineRevision int64            `json:"timeline_revision"`
	TimelineSHA256   string           `json:"timeline_sha256"`
	DurationUS       int64            `json:"duration_us"`
	Output           OutputContractV2 `json:"output"`
	FinalAudio       FinalAudioV2     `json:"final_audio"`
	VideoTracks      []VideoTrackV2   `json:"video_tracks"`
	Assets           []AssetRefV2     `json:"assets"`
}

// OutputContractV2 describes the normalized output timeline and container.
type OutputContractV2 struct {
	Container   string `json:"container"`
	VideoCodec  string `json:"video_codec"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	FPSNum      int    `json:"fps_num"`
	FPSDen      int    `json:"fps_den"`
	PixelFormat string `json:"pixel_format,omitempty"`

	// Strengthened canonical-profile fields. They are optional on legacy V2
	// documents so existing render_batch plans remain wire-compatible; the
	// packet-copy executor requires ProfileID and validates the full profile.
	ProfileID    string `json:"profile_id,omitempty"`
	CodecProfile string `json:"codec_profile,omitempty"`
	CodecLevel   string `json:"codec_level,omitempty"`
	GOPSize      int    `json:"gop_size,omitempty"`
	BFrames      int    `json:"b_frames,omitempty"`
	ClosedGOP    bool   `json:"closed_gop,omitempty"`
	TimeBaseNum  int    `json:"time_base_num,omitempty"`
	TimeBaseDen  int    `json:"time_base_den,omitempty"`
}

// FinalAudioV2 identifies the one already-finalized audio source for the
// output. Its timeline revision and hash bind it to the same editorial
// timeline as the enclosing plan.
type FinalAudioV2 struct {
	Mode string `json:"mode"`

	AssetID   string `json:"asset_id"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`

	Codec        string `json:"codec"`
	SampleRateHz int    `json:"sample_rate_hz"`
	Channels     int    `json:"channels"`

	DurationUS int64 `json:"duration_us"`

	TimelineRevision int64  `json:"timeline_revision"`
	TimelineSHA256   string `json:"timeline_sha256"`
}

// VideoTrackV2 groups ordered video segments belonging to one output track.
// Segment order is semantic and therefore remains exactly as supplied by the
// producer; only the set-like assets list is reordered during canonicalizing.
type VideoTrackV2 struct {
	TrackID  string           `json:"track_id"`
	Segments []VideoSegmentV2 `json:"segments"`
}

// VideoSegmentV2 places a source window on the CFR output timeline. Output
// placement uses frames; source trimming remains in microseconds so source
// media does not need to be assumed CFR.
type VideoSegmentV2 struct {
	SegmentID string `json:"segment_id"`

	AssetID string `json:"asset_id"`
	SHA256  string `json:"sha256"`

	TimelineStartFrame int64 `json:"timeline_start_frame"`
	FrameCount         int64 `json:"frame_count"`

	SourceInUS       int64 `json:"source_in_us"`
	SourceDurationUS int64 `json:"source_duration_us"`
}

// AssetRefV2 is a typed, path-free reference to one verified asset used by
// the plan. The worker resolves AssetID to a local path at runtime.
type AssetRefV2 struct {
	AssetID  string `json:"asset_id"`
	AssetKey string `json:"asset_key,omitempty"`

	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`

	Kind string `json:"kind"`
	MIME string `json:"mime,omitempty"`

	DurationUS int64 `json:"duration_us,omitempty"`
	Width      int   `json:"width,omitempty"`
	Height     int   `json:"height,omitempty"`

	// Prepared-fragment admission metadata. Optional for ordinary V2 assets;
	// video.assemble.copy.v1 requires it for every externally prepared video.
	ProfileID          string `json:"profile_id,omitempty"`
	FrameCount         int64  `json:"frame_count,omitempty"`
	TimelineRevision   int64  `json:"timeline_revision,omitempty"`
	TimelineSHA256     string `json:"timeline_sha256,omitempty"`
	TimelineStartFrame int64  `json:"timeline_start_frame,omitempty"`
	FirstFrameKeyframe bool   `json:"first_frame_keyframe,omitempty"`
	ClosedGOP          bool   `json:"closed_gop,omitempty"`
}

// CanonicalJSON returns the deterministic V2 document used for persistence
// and hashing. Asset references are sorted by AssetID because their order is
// set-like; track and segment order is preserved because it is semantic.
// Nil slices are normalized to empty arrays so equivalent empty collections
// never alternate between JSON null and [].
func (p *CompiledRenderPlanV2) CanonicalJSON() ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("compiled render plan v2: nil plan")
	}

	canonical := *p
	canonical.VideoTracks = cloneVideoTracks(p.VideoTracks)
	canonical.Assets = append([]AssetRefV2(nil), p.Assets...)
	sort.SliceStable(canonical.Assets, func(i, j int) bool {
		left, right := canonical.Assets[i], canonical.Assets[j]
		if left.AssetID != right.AssetID {
			return left.AssetID < right.AssetID
		}
		// Asset IDs are expected to be unique, but deterministic tie-breaking
		// keeps canonicalization stable even before semantic validation runs.
		return assetRefV2SortKey(left) < assetRefV2SortKey(right)
	})
	if canonical.VideoTracks == nil {
		canonical.VideoTracks = []VideoTrackV2{}
	}
	if canonical.Assets == nil {
		canonical.Assets = []AssetRefV2{}
	}

	data, err := json.Marshal(&canonical)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: canonical marshal: %w", err)
	}
	return data, nil
}

// PlanSHA256 returns SHA256(CanonicalJSON()).
func (p *CompiledRenderPlanV2) PlanSHA256() (string, error) {
	data, err := p.CanonicalJSON()
	if err != nil {
		return "", err
	}
	return HashCompiledPlanV2(data), nil
}

// HashCompiledPlanV2 computes the SHA256 digest of already-canonical V2 JSON.
func HashCompiledPlanV2(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func assetRefV2SortKey(asset AssetRefV2) string {
	data, _ := json.Marshal(asset)
	return string(data)
}

func cloneVideoTracks(input []VideoTrackV2) []VideoTrackV2 {
	if input == nil {
		return nil
	}
	output := make([]VideoTrackV2, len(input))
	copy(output, input)
	for i := range output {
		if output[i].Segments == nil {
			output[i].Segments = []VideoSegmentV2{}
		} else {
			output[i].Segments = append([]VideoSegmentV2(nil), output[i].Segments...)
		}
	}
	return output
}
