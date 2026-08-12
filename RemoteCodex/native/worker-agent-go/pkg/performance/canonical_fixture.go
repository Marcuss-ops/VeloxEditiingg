package performance

// canonical_fixture.go owns the generation SPEC of the Formula 1 test
// track — COPY_ONLY_CANONICAL_5M_V1 (plan §14): ~5 minutes of 24 H264
// 1920x1080 CFR clips plus an AAC 48 kHz stereo final audio track, zero
// filters / zero subtitles / zero transformations, all assets warm
// cache.
//
// The spec is the CROSS-HOST-DETERMINISTIC definition of the asset set:
// it pins clip count, per-clip frame count (CFR-exact), canvas, codecs,
// timebase and per-clip source variation, so two machines building the
// track from the same spec generate the same *kind* of assets.
// Byte-level per-asset SHAs depend on the exact encoder build, so they
// live in the machine-generated manifest (fixture_manifest.go), NOT in
// the registry. The registry pins AssetSHA256 = SpecSHA256(): the
// digest of this spec — "commit A → fixture" and "commit B → fixture"
// identify the same asset set on every host, which is exactly the
// comparability the plan requires.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// CanonicalVideoSpec pins the canonical clip video stream.
type CanonicalVideoSpec struct {
	Codec       string `json:"codec"`        // h264
	Width       int    `json:"width"`        // 1920
	Height      int    `json:"height"`       // 1080
	FPS         int    `json:"fps"`          // 30, constant frame rate
	PixelFormat string `json:"pixel_format"` // yuv420p
	// Timebase is the canonical MP4 video track timescale (1/15360 is
	// the standard H.264 30fps timescale — plan §14 "canonical
	// timebase"). Pinned via -video_track_timescale at generation so
	// every clip shares it exactly.
	Timebase string `json:"timebase"`
}

// CanonicalAudioSpec pins the canonical audio stream (per-clip AND the
// final audio track).
type CanonicalAudioSpec struct {
	Codec      string `json:"codec"`       // aac
	SampleRate int    `json:"sample_rate"` // 48000
	Channels   int    `json:"channels"`    // 2 (stereo)
}

// CanonicalClipSpec describes ONE clip of the track. Clips differ only
// by deterministic source variation (solid color + sine frequency) so
// the set is byte-distinct per clip while remaining spec-pinned.
type CanonicalClipSpec struct {
	Index            int    `json:"index"`
	Frames           int    `json:"frames"` // CFR-exact frame count
	ColorRGB         int    `json:"color_rgb"`
	AudioFrequencyHz int    `json:"audio_frequency_hz"`
	Source           string `json:"source"` // lavfi source: color+sine
}

// CanonicalFixtureSpec is the full, deterministic definition of the
// track. It is a value type on purpose: SpecSHA256 is computed over its
// canonical JSON, so ANY field change yields a different digest and
// forces a fixture version bump.
type CanonicalFixtureSpec struct {
	Version        int                 `json:"version"`
	DurationSec    int                 `json:"duration_sec"`
	ClipCount      int                 `json:"clip_count"`
	PerClipFrames  int                 `json:"per_clip_frames"`
	Video          CanonicalVideoSpec  `json:"video"`
	Audio          CanonicalAudioSpec  `json:"audio"`
	ZeroFilters    bool                `json:"zero_filters"`
	ZeroSubtitles  bool                `json:"zero_subtitles"`
	ZeroTransforms bool                `json:"zero_transforms"`
	CacheMode      CacheMode           `json:"cache_mode"`
	Clips          []CanonicalClipSpec `json:"clips"`
}

// CanonicalFixtureSpecV1 returns the canonical track definition: 24
// clips × 375 frames (12.5 s at 30 fps) = 9000 frames = exactly 300 s
// of CFR content, H264 1920x1080 yuv420p, AAC 48 kHz stereo, warm
// cache.
func CanonicalFixtureSpecV1() CanonicalFixtureSpec {
	const (
		clipCount     = 24
		perClipFrames = 375 // 12.5 s at 30 fps, CFR-exact
	)
	video := CanonicalVideoSpec{
		Codec: "h264", Width: 1920, Height: 1080, FPS: 30,
		PixelFormat: "yuv420p", Timebase: "1/15360",
	}
	audio := CanonicalAudioSpec{Codec: "aac", SampleRate: 48000, Channels: 2}
	clips := make([]CanonicalClipSpec, 0, clipCount)
	for i := 0; i < clipCount; i++ {
		// Deterministic per-clip variation: RGB walks three co-prime
		// sequences, sine frequency walks 220→680 Hz. No filters, no
		// randomness, every clip byte-distinct.
		clips = append(clips, CanonicalClipSpec{
			Index:            i + 1,
			Frames:           perClipFrames,
			ColorRGB:         (i*3)%256<<16 | (i*7)%256<<8 | (i*11)%256,
			AudioFrequencyHz: 220 + 20*i,
			Source:           "color+sine",
		})
	}
	return CanonicalFixtureSpec{
		Version:        1,
		DurationSec:    300,
		ClipCount:      clipCount,
		PerClipFrames:  perClipFrames,
		Video:          video,
		Audio:          audio,
		ZeroFilters:    true,
		ZeroSubtitles:  true,
		ZeroTransforms: true,
		CacheMode:      CacheModeWarm,
		Clips:          clips,
	}
}

// SpecSHA256 returns the stable digest of the whole spec. This is the
// value pinned as AssetSHA256 on the canonical fixture — the
// cross-host identity of the track's asset set.
func (s CanonicalFixtureSpec) SpecSHA256() string {
	data, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ClipDurationSec returns the exact per-clip duration (frames / fps).
func (s CanonicalFixtureSpec) ClipDurationSec() float64 {
	return float64(s.PerClipFrames) / float64(s.Video.FPS)
}

// TotalFrames returns clipCount × perClipFrames (CFR-exact workload
// size).
func (s CanonicalFixtureSpec) TotalFrames() int { return s.ClipCount * s.PerClipFrames }

// VerifyFixtureSpec checks a registered fixture against the canonical
// spec. The canonical fixture is the ONLY one that may carry the spec
// digest; every identity field must agree. Returns nil when consistent.
func VerifyFixtureSpec(fixture BenchmarkFixture, spec CanonicalFixtureSpec) []string {
	var problems []string
	if fixture.ID != FixtureCopyOnlyCanonical5MV1 {
		problems = append(problems, fmt.Sprintf("spec applies to %s, got %s", FixtureCopyOnlyCanonical5MV1, fixture.ID))
	}
	if fixture.Kind != FixtureKindCopyOnly {
		problems = append(problems, fmt.Sprintf("kind must be %s, got %s", FixtureKindCopyOnly, fixture.Kind))
	}
	if fixture.CacheMode != CacheModeWarm {
		problems = append(problems, fmt.Sprintf("cache mode must be %s, got %s", CacheModeWarm, fixture.CacheMode))
	}
	if fixture.DurationSec != spec.DurationSec {
		problems = append(problems, fmt.Sprintf("duration_sec %d != spec %d", fixture.DurationSec, spec.DurationSec))
	}
	if fixture.ClipCount != spec.ClipCount {
		problems = append(problems, fmt.Sprintf("clip_count %d != spec %d", fixture.ClipCount, spec.ClipCount))
	}
	if fixture.AssetSHA256 != spec.SpecSHA256() {
		problems = append(problems, "asset_sha256 is not the canonical spec digest — regenerate the fixture from the spec")
	}
	return problems
}
