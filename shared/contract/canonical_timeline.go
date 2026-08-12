package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// CanonicalTimeline is the producer-owned editorial timeline. It is the
// single source of truth from which audio and video execution plans are
// compiled. All time values are integer microseconds; ordering of every
// slice is semantic and is preserved during canonicalization.
type CanonicalTimeline struct {
	Revision   int64                `json:"revision"`
	DurationUS int64                `json:"duration_us"`
	Segments   []TimelineSegment    `json:"segments"`
	Voiceovers []TimelineAudioTrack `json:"voiceovers"`
	Music      []TimelineAudioTrack `json:"music"`
	SFX        []TimelineAudioTrack `json:"sfx"`
}

// TimelineSegment is one editorial video placement. SourceOut is intentionally
// not stored: source duration is the sole source-window length, avoiding two
// independently-derived end timestamps.
type TimelineSegment struct {
	SegmentID string `json:"segment_id"`
	AssetID   string `json:"asset_id"`

	TimelineStartUS    int64 `json:"timeline_start_us"`
	TimelineDurationUS int64 `json:"timeline_duration_us"`

	SourceInUS       int64 `json:"source_in_us"`
	SourceDurationUS int64 `json:"source_duration_us"`

	AudioEnabled bool    `json:"audio_enabled"`
	AudioGain    float64 `json:"audio_gain"`
}

// TimelineAudioTrack identifies an already-placed audio source in the
// canonical timeline. The same integer timing model is consumed by the audio
// compiler and the render-plan compiler; no worker-side timing inference is
// permitted.
type TimelineAudioTrack struct {
	TrackID string `json:"track_id"`
	AssetID string `json:"asset_id"`

	TimelineStartUS    int64 `json:"timeline_start_us"`
	TimelineDurationUS int64 `json:"timeline_duration_us"`

	SourceInUS       int64   `json:"source_in_us"`
	SourceDurationUS int64   `json:"source_duration_us"`
	Gain             float64 `json:"gain"`
}

// Validate checks the structural and temporal invariants that can be checked
// without resolving media assets. Asset existence, hashes, and source bounds
// against media metadata belong to the later asset/plan validation boundary.
func (t *CanonicalTimeline) Validate() error {
	if t == nil {
		return fmt.Errorf("canonical timeline: nil timeline")
	}
	var violations []string
	if t.Revision <= 0 {
		violations = append(violations, "revision must be positive")
	}
	if t.DurationUS <= 0 {
		violations = append(violations, "duration_us must be positive")
	}

	segmentIDs := make(map[string]struct{}, len(t.Segments))
	for index, segment := range t.Segments {
		path := fmt.Sprintf("segments[%d]", index)
		validateTimelinePlacement(&violations, path, segment.SegmentID, segment.AssetID,
			segment.TimelineStartUS, segment.TimelineDurationUS,
			segment.SourceInUS, segment.SourceDurationUS, segment.AudioGain)
		validateTimelineBounds(&violations, path, segment.TimelineStartUS, segment.TimelineDurationUS, t.DurationUS)
		if segment.SegmentID != "" {
			if _, exists := segmentIDs[segment.SegmentID]; exists {
				violations = append(violations, path+".segment_id must be unique")
			} else {
				segmentIDs[segment.SegmentID] = struct{}{}
			}
		}
	}

	for _, group := range []struct {
		name   string
		tracks []TimelineAudioTrack
	}{
		{name: "voiceovers", tracks: t.Voiceovers},
		{name: "music", tracks: t.Music},
		{name: "sfx", tracks: t.SFX},
	} {
		trackIDs := make(map[string]struct{}, len(group.tracks))
		for index, track := range group.tracks {
			path := fmt.Sprintf("%s[%d]", group.name, index)
			validateTimelinePlacement(&violations, path, track.TrackID, track.AssetID,
				track.TimelineStartUS, track.TimelineDurationUS,
				track.SourceInUS, track.SourceDurationUS, track.Gain)
			validateTimelineBounds(&violations, path, track.TimelineStartUS, track.TimelineDurationUS, t.DurationUS)
			if track.TrackID != "" {
				if _, exists := trackIDs[track.TrackID]; exists {
					violations = append(violations, path+".track_id must be unique within its group")
				} else {
					trackIDs[track.TrackID] = struct{}{}
				}
			}
		}
	}

	if len(violations) > 0 {
		return fmt.Errorf("canonical timeline: %s", strings.Join(violations, "; "))
	}
	return nil
}

func validateTimelineBounds(violations *[]string, path string, start, duration, timelineDuration int64) {
	if start >= 0 && duration > 0 && timelineDuration > 0 {
		const maxInt64 = int64(^uint64(0) >> 1)
		if start <= maxInt64-duration && start+duration > timelineDuration {
			*violations = append(*violations, path+" timeline range exceeds duration_us")
		}
	}
}

func validateTimelinePlacement(violations *[]string, path, identity, assetID string, timelineStart, timelineDuration, sourceIn, sourceDuration int64, gain float64) {
	if strings.TrimSpace(identity) == "" {
		*violations = append(*violations, path+" identity is required")
	}
	if strings.TrimSpace(assetID) == "" {
		*violations = append(*violations, path+".asset_id is required")
	}
	if timelineStart < 0 {
		*violations = append(*violations, path+".timeline_start_us must be non-negative")
	}
	if timelineDuration <= 0 {
		*violations = append(*violations, path+".timeline_duration_us must be positive")
	}
	if sourceIn < 0 {
		*violations = append(*violations, path+".source_in_us must be non-negative")
	}
	if sourceDuration <= 0 {
		*violations = append(*violations, path+".source_duration_us must be positive")
	}
	if math.IsNaN(gain) || math.IsInf(gain, 0) {
		*violations = append(*violations, path+" gain must be finite")
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if timelineStart >= 0 && timelineDuration > 0 && timelineStart > maxInt64-timelineDuration {
		*violations = append(*violations, path+" timeline range overflows int64")
	}
	if sourceIn >= 0 && sourceDuration > 0 && sourceIn > maxInt64-sourceDuration {
		*violations = append(*violations, path+" source range overflows int64")
	}
}

// CanonicalJSON returns the exact JSON document used for the timeline
// identity. Nil collections are normalized to empty arrays, while semantic
// ordering is preserved. The timeline is validated before it can be hashed.
func (t *CanonicalTimeline) CanonicalJSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	canonical := *t
	canonical.Segments = append([]TimelineSegment{}, t.Segments...)
	canonical.Voiceovers = append([]TimelineAudioTrack{}, t.Voiceovers...)
	canonical.Music = append([]TimelineAudioTrack{}, t.Music...)
	canonical.SFX = append([]TimelineAudioTrack{}, t.SFX...)
	return json.Marshal(&canonical)
}

// TimelineSHA256 returns SHA256(CanonicalJSON()).
func (t *CanonicalTimeline) TimelineSHA256() (string, error) {
	canonical, err := t.CanonicalJSON()
	if err != nil {
		return "", err
	}
	return HashCanonicalTimeline(canonical), nil
}

// HashCanonicalTimeline computes the SHA256 digest of already-canonical
// timeline JSON. The helper is useful when the canonical bytes are persisted
// alongside the timeline document.
func HashCanonicalTimeline(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
