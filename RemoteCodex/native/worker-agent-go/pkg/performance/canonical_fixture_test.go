package performance

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCanonicalSpecPinsTheTrack pins the Formula 1 test track
// definition (plan §14): ~5 min of 24 H264 1920x1080 CFR clips + AAC
// 48 kHz stereo final audio, zero filters/subtitles/transformations,
// warm cache, canonical timebase, CFR-exact 375 frames per clip.
func TestCanonicalSpecPinsTheTrack(t *testing.T) {
	s := CanonicalFixtureSpecV1()

	require.Equal(t, 1, s.Version)
	require.Equal(t, 300, s.DurationSec)
	require.Equal(t, 24, s.ClipCount)
	require.Equal(t, 375, s.PerClipFrames)
	require.Equal(t, 24*375, s.TotalFrames())
	require.InDelta(t, 12.5, s.ClipDurationSec(), 1e-9)

	require.Equal(t, "h264", s.Video.Codec)
	require.Equal(t, 1920, s.Video.Width)
	require.Equal(t, 1080, s.Video.Height)
	require.Equal(t, 30, s.Video.FPS)
	require.Equal(t, "yuv420p", s.Video.PixelFormat)
	require.Equal(t, "1/15360", s.Video.Timebase)

	require.Equal(t, "aac", s.Audio.Codec)
	require.Equal(t, 48000, s.Audio.SampleRate)
	require.Equal(t, 2, s.Audio.Channels)

	require.True(t, s.ZeroFilters)
	require.True(t, s.ZeroSubtitles)
	require.True(t, s.ZeroTransforms)
	require.Equal(t, CacheModeWarm, s.CacheMode)
}

// TestCanonicalSpecClipsAreDeterministicAndDistinct pins that the 24
// clips are spec-defined (deterministic) yet byte-distinct: every clip
// carries the CFR-exact frame count and unique color/frequency.
func TestCanonicalSpecClipsAreDeterministicAndDistinct(t *testing.T) {
	a := CanonicalFixtureSpecV1()
	b := CanonicalFixtureSpecV1()
	require.Equal(t, a, b, "the spec must be value-deterministic")
	require.Equal(t, a.SpecSHA256(), b.SpecSHA256(), "the spec digest must be stable across constructions")
	require.Len(t, a.Clips, 24)

	colors := map[int]bool{}
	freqs := map[int]bool{}
	for i, c := range a.Clips {
		require.Equal(t, i+1, c.Index)
		require.Equal(t, 375, c.Frames)
		require.Equal(t, "color+sine", c.Source)
		require.False(t, colors[c.ColorRGB], "clips must be byte-distinct (color)")
		require.False(t, freqs[c.AudioFrequencyHz], "clips must be byte-distinct (audio)")
		colors[c.ColorRGB] = true
		freqs[c.AudioFrequencyHz] = true
	}
	require.Equal(t, 24, len(colors))
	require.Equal(t, 24, len(freqs))
}

// TestCanonicalSpecDigestChangesWithContent pins that editing any spec
// field yields a different digest — the registry pin (AssetSHA256)
// forces a fixture version bump on any track change.
func TestCanonicalSpecDigestChangesWithContent(t *testing.T) {
	base := CanonicalFixtureSpecV1().SpecSHA256()

	mut := CanonicalFixtureSpecV1()
	mut.Video.Width = 1280
	require.NotEqual(t, base, mut.SpecSHA256())

	mut2 := CanonicalFixtureSpecV1()
	mut2.Audio.SampleRate = 44100
	require.NotEqual(t, base, mut2.SpecSHA256())
}

// TestCanonicalFixtureRegistryWiring pins that the registry's canonical
// fixture carries the spec digest as AssetSHA256 and satisfies
// VerifyFixtureSpec.
func TestCanonicalFixtureRegistryWiring(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, ok := reg.Fixture(FixtureCopyOnlyCanonical5MV1)
	require.True(t, ok)

	spec := CanonicalFixtureSpecV1()
	require.Equal(t, spec.SpecSHA256(), f.AssetSHA256,
		"AssetSHA256 must be pinned to the canonical spec digest")
	require.Empty(t, VerifyFixtureSpec(f, spec))

	// Other fixtures must NOT carry the canonical spec digest (the
	// spec is exclusive to the Formula 1 track).
	other, _ := reg.Fixture(FixtureCopy5MHigh)
	require.Empty(t, other.AssetSHA256)
	require.NotEmpty(t, VerifyFixtureSpec(other, spec), "the spec must reject non-canonical fixtures")
}

// TestVerifyFixtureSpecRejectsDrift pins that identity drift between
// the registry fixture and the spec is caught (duration, clip count,
// cache mode, kind, digest).
func TestVerifyFixtureSpecRejectsDrift(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	cases := []struct {
		name string
		mut  func(*BenchmarkFixture)
	}{
		{"duration", func(f *BenchmarkFixture) { f.DurationSec = 301 }},
		{"clip_count", func(f *BenchmarkFixture) { f.ClipCount = 23 }},
		{"cache", func(f *BenchmarkFixture) { f.CacheMode = CacheModeCold }},
		{"kind", func(f *BenchmarkFixture) { f.Kind = FixtureKindComposite }},
		{"digest", func(f *BenchmarkFixture) { f.AssetSHA256 = "deadbeef" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := f
			tc.mut(&mutated)
			require.NotEmpty(t, VerifyFixtureSpec(mutated, spec))
		})
	}
}
