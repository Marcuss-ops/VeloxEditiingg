package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func testCanonicalTimeline() *CanonicalTimeline {
	return &CanonicalTimeline{
		Revision:   7,
		DurationUS: 60_000_000,
		Segments: []TimelineSegment{
			{
				SegmentID:          "comedian-a",
				AssetID:            "video-a",
				TimelineStartUS:    12_400_000,
				TimelineDurationUS: 5_600_000,
				SourceInUS:         33_200_000,
				SourceDurationUS:   5_600_000,
				AudioEnabled:       true,
				AudioGain:          0.75,
			},
			{
				SegmentID:          "comedian-b",
				AssetID:            "video-b",
				TimelineStartUS:    22_000_000,
				TimelineDurationUS: 6_500_000,
				SourceInUS:         7_100_000,
				SourceDurationUS:   6_500_000,
				AudioEnabled:       false,
			},
		},
		Voiceovers: []TimelineAudioTrack{{
			TrackID:            "vo-main",
			AssetID:            "voiceover-a",
			TimelineStartUS:    0,
			TimelineDurationUS: 60_000_000,
			SourceInUS:         0,
			SourceDurationUS:   60_000_000,
			Gain:               1,
		}},
		Music: []TimelineAudioTrack{{
			TrackID:            "music-bed",
			AssetID:            "music-a",
			TimelineStartUS:    0,
			TimelineDurationUS: 60_000_000,
			SourceInUS:         0,
			SourceDurationUS:   60_000_000,
			Gain:               0.2,
		}},
		SFX: []TimelineAudioTrack{{
			TrackID:            "sfx-hit",
			AssetID:            "sfx-a",
			TimelineStartUS:    35_000_000,
			TimelineDurationUS: 1_000_000,
			SourceInUS:         2_000_000,
			SourceDurationUS:   1_000_000,
		}},
	}
}

func TestCanonicalTimeline_CanonicalJSONIsDeterministic(t *testing.T) {
	timeline := testCanonicalTimeline()
	first, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	second, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("second CanonicalJSON: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("same timeline produced different JSON:\nfirst=%s\nsecond=%s", first, second)
	}

	want := `{"revision":7,"duration_us":60000000,"segments":[{"segment_id":"comedian-a","asset_id":"video-a","timeline_start_us":12400000,"timeline_duration_us":5600000,"source_in_us":33200000,"source_duration_us":5600000,"audio_enabled":true,"audio_gain":0.75},{"segment_id":"comedian-b","asset_id":"video-b","timeline_start_us":22000000,"timeline_duration_us":6500000,"source_in_us":7100000,"source_duration_us":6500000,"audio_enabled":false,"audio_gain":0}],"voiceovers":[{"track_id":"vo-main","asset_id":"voiceover-a","timeline_start_us":0,"timeline_duration_us":60000000,"source_in_us":0,"source_duration_us":60000000,"gain":1}],"music":[{"track_id":"music-bed","asset_id":"music-a","timeline_start_us":0,"timeline_duration_us":60000000,"source_in_us":0,"source_duration_us":60000000,"gain":0.2}],"sfx":[{"track_id":"sfx-hit","asset_id":"sfx-a","timeline_start_us":35000000,"timeline_duration_us":1000000,"source_in_us":2000000,"source_duration_us":1000000,"gain":0}]}`
	if string(first) != want {
		t.Fatalf("canonical JSON mismatch:\ngot:  %s\nwant: %s", first, want)
	}
	if strings.Contains(string(first), "source_out") {
		t.Fatalf("canonical timeline must not carry redundant source_out: %s", first)
	}
}

func TestCanonicalTimeline_SHA256MatchesCanonicalBytes(t *testing.T) {
	timeline := testCanonicalTimeline()
	canonical, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	got, err := timeline.TimelineSHA256()
	if err != nil {
		t.Fatalf("TimelineSHA256: %v", err)
	}
	sum := sha256.Sum256(canonical)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("TimelineSHA256 = %q, standard SHA256 = %q", got, want)
	}
	if got != HashCanonicalTimeline(canonical) {
		t.Fatalf("HashCanonicalTimeline disagrees with TimelineSHA256: %q != %q", got, HashCanonicalTimeline(canonical))
	}
	if len(got) != 64 || strings.ToLower(got) != got {
		t.Fatalf("timeline SHA256 = %q, want lowercase 64-character digest", got)
	}

	timeline.Segments[0].TimelineStartUS++
	changed, err := timeline.TimelineSHA256()
	if err != nil {
		t.Fatalf("TimelineSHA256 after editorial change: %v", err)
	}
	if changed == got {
		t.Fatal("timeline SHA256 did not change after changing editorial placement")
	}
}

func TestCanonicalTimeline_CanonicalizationDoesNotMutateCaller(t *testing.T) {
	timeline := testCanonicalTimeline()
	originalSegmentID := timeline.Segments[0].SegmentID
	originalVoiceoverID := timeline.Voiceovers[0].TrackID
	if _, err := timeline.CanonicalJSON(); err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	if timeline.Segments[0].SegmentID != originalSegmentID || timeline.Voiceovers[0].TrackID != originalVoiceoverID {
		t.Fatal("CanonicalJSON mutated the caller's timeline")
	}
}

func TestCanonicalTimeline_NormalizesNilCollectionsToArrays(t *testing.T) {
	timeline := &CanonicalTimeline{Revision: 1, DurationUS: 1_000_000}
	canonical, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	text := string(canonical)
	for _, key := range []string{"segments", "voiceovers", "music", "sfx"} {
		if strings.Contains(text, `"`+key+`":null`) {
			t.Fatalf("%s was encoded as null: %s", key, text)
		}
		if !strings.Contains(text, `"`+key+`":[]`) {
			t.Fatalf("%s was not normalized to an empty array: %s", key, text)
		}
	}
}

func TestCanonicalTimeline_RejectsInvalidTemporalContract(t *testing.T) {
	tests := []struct {
		name string
		edit func(*CanonicalTimeline)
		want string
	}{
		{"missing revision", func(t *CanonicalTimeline) { t.Revision = 0 }, "revision must be positive"},
		{"duplicate segment", func(t *CanonicalTimeline) { t.Segments[1].SegmentID = t.Segments[0].SegmentID }, "segment_id must be unique"},
		{"timeline out of bounds", func(t *CanonicalTimeline) { t.Segments[0].TimelineStartUS = t.DurationUS }, "timeline range exceeds duration_us"},
		{"negative source", func(t *CanonicalTimeline) { t.Segments[0].SourceInUS = -1 }, "source_in_us must be non-negative"},
		{"non-finite gain", func(t *CanonicalTimeline) { t.Segments[0].AudioGain = math.NaN() }, "gain must be finite"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			timeline := testCanonicalTimeline()
			test.edit(timeline)
			if err := timeline.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, test.want)
			}
			if _, err := timeline.TimelineSHA256(); err == nil {
				t.Fatal("TimelineSHA256() = nil, want invalid timeline to fail closed")
			}
		})
	}
}

func TestCanonicalTimeline_JSONRoundTripPreservesIdentity(t *testing.T) {
	timeline := testCanonicalTimeline()
	canonical, err := timeline.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	var decoded CanonicalTimeline
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	decodedCanonical, err := decoded.CanonicalJSON()
	if err != nil {
		t.Fatalf("decoded CanonicalJSON: %v", err)
	}
	if string(decodedCanonical) != string(canonical) {
		t.Fatalf("round trip changed canonical JSON:\ngot:  %s\nwant: %s", decodedCanonical, canonical)
	}
	firstHash, _ := timeline.TimelineSHA256()
	secondHash, _ := decoded.TimelineSHA256()
	if firstHash != secondHash {
		t.Fatalf("round trip changed timeline SHA: %q != %q", firstHash, secondHash)
	}
}
