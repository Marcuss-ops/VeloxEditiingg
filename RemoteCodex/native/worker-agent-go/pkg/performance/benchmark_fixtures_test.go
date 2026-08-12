package performance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBenchmarkFixtureRegistry_CanonicalSet pins the canonical fixture
// set: the plan's fixtures exist, carry their immutable identity
// (duration, clip count, kind, cache mode) and the Phase-1 copy-only
// invariants are enforced budgets while timing ceilings stay TBD.
func TestBenchmarkFixtureRegistry_CanonicalSet(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	require.Equal(t, 9, reg.Count())

	want := []struct {
		id          BenchmarkFixtureID
		kind        FixtureKind
		cache       CacheMode
		durationSec int
		clipCount   int
	}{
		{FixtureCopy5MLow, FixtureKindCopyOnly, CacheModeWarm, 300, 5},
		{FixtureCopy5MHigh, FixtureKindCopyOnly, CacheModeWarm, 300, 24},
		{FixtureCopy10MLow, FixtureKindCopyOnly, CacheModeWarm, 600, 10},
		{FixtureCopy10MHigh, FixtureKindCopyOnly, CacheModeWarm, 600, 48},
		{FixtureFinalAudio5M, FixtureKindFinalAudio, CacheModeWarm, 300, 5},
		{FixtureComposite5MLow, FixtureKindComposite, CacheModeWarm, 300, 5},
		{FixtureComposite5MHigh, FixtureKindComposite, CacheModeWarm, 300, 24},
		{FixtureCopyOnlyCanonical5MV1, FixtureKindCopyOnly, CacheModeWarm, 300, 24},
		{FixtureComplexCanonical5MV1, FixtureKindComposite, CacheModeWarm, 300, 24},
	}
	for _, w := range want {
		f, ok := reg.Fixture(w.id)
		require.Truef(t, ok, "fixture %q must be registered", w.id)
		require.Equal(t, w.kind, f.Kind)
		require.Equal(t, w.cache, f.CacheMode)
		require.Equal(t, w.durationSec, f.DurationSec)
		require.Equal(t, w.clipCount, f.ClipCount)
	}

	// Unknown ID: not found.
	_, ok := reg.Fixture("NOT_A_FIXTURE")
	require.False(t, ok)

	// All() is sorted and complete.
	all := reg.All()
	require.Len(t, all, 9)
	for i := 1; i < len(all); i++ {
		require.Less(t, string(all[i-1].ID), string(all[i].ID), "All() must be sorted by ID")
	}
}

// TestBenchmarkFixtureRegistry_CopyOnlyInvariants pins the Phase-1
// invariants on the copy-only fixtures: zero decode/encode/audio-encode
// and zero external ffmpeg/ffprobe/temp-segment files are ENFORCED
// budgets (Set=true, Value=0), write amplification is capped at 1.5,
// and the wall-clock ceilings stay TBD until the baseline is measured.
func TestBenchmarkFixtureRegistry_CopyOnlyInvariants(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	require.True(t, f.Budget.Correctness.ArtifactSHARequired)
	for name, b := range map[string]BudgetMax{
		"video_decode_frames": f.Budget.Correctness.VideoDecodeFramesMax,
		"video_encode_frames": f.Budget.Correctness.VideoEncodeFramesMax,
		"audio_encode_passes": f.Budget.Correctness.AudioEncodePassesMax,
		"ffmpeg_exec":         f.Budget.Architecture.FfmpegExecMax,
		"ffprobe_exec":        f.Budget.Architecture.FfprobeExecMax,
		"temp_segment_files":  f.Budget.Architecture.TempSegmentFilesMax,
		"external_exec":       f.Budget.Architecture.ExternalExecMax,
	} {
		require.Truef(t, b.Set, "%s must be an enforced budget on the canonical fixture", name)
		require.Zerof(t, b.Value, "%s must be a zero invariant on the canonical fixture", name)
	}
	require.True(t, f.Budget.IO.WriteAmplificationMax.Set)
	require.InDelta(t, 1.5, f.Budget.IO.WriteAmplificationMax.Value, 1e-9)
	require.False(t, f.Budget.Performance.P50WallMSMax.Set, "timing ceilings stay TBD until baseline")
	require.False(t, f.Budget.Performance.P95WallMSMax.Set, "timing ceilings stay TBD until baseline")

	// Composite fixtures carry no zero invariants (encode is legitimate).
	c, _ := reg.Fixture(FixtureComposite5MHigh)
	require.False(t, c.Budget.Correctness.VideoEncodeFramesMax.Set)
	require.False(t, c.Budget.Architecture.FfmpegExecMax.Set)
}

// TestBenchmarkFixtureRegistry_Immutability pins the immutability
// boundary: Fixture() returns a copy, so mutating the returned value can
// never corrupt the registry.
func TestBenchmarkFixtureRegistry_Immutability(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()

	orig, _ := reg.Fixture(FixtureCopy5MHigh)
	origDigest := orig.DigestSHA256()

	// Attempt to mutate the registry through a returned copy.
	evil := orig
	evil.ClipCount = 1
	evil.Budget.Correctness.VideoDecodeFramesMax = UnsetMax()
	evil.DurationSec = 42

	again, _ := reg.Fixture(FixtureCopy5MHigh)
	require.Equal(t, 24, again.ClipCount, "registry must be immune to mutation of returned copies")
	require.Equal(t, 300, again.DurationSec)
	require.Equal(t, origDigest, again.DigestSHA256(), "fixture digest must be stable across lookups")
}

// TestBenchmarkFixture_DigestChangesWithContent pins that the digest
// covers the WHOLE fixture: editing any identity or budget field yields
// a different digest (forcing a new fixture version, per the plan).
func TestBenchmarkFixture_DigestChangesWithContent(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopy10MHigh)
	d := f.DigestSHA256()

	mutated := f
	mutated.Budget.Performance.P95WallMSMax = MaxInt64(5000)
	require.NotEqual(t, d, mutated.DigestSHA256())

	other, _ := reg.Fixture(FixtureCopy5MHigh)
	require.NotEqual(t, d, other.DigestSHA256(), "different fixtures must have different digests")
}

// TestEvaluateFixture_CopyOnlyPass pins the happy path: a receipt that
// respects the copy-only invariants (zero decode/encode/passes/execs,
// write amplification <= 1.5) produces no violations.
func TestEvaluateFixture_CopyOnlyPass(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	receipt := NewPerformanceReceiptV1()
	receipt.Timing.WallMs = 4000
	receipt.Media.FramesDecoded = 0
	receipt.Media.Frames = 0
	receipt.Media.EncodePasses = 0
	receipt.Process.FfmpegExecCount = 0
	receipt.Process.FfprobeExecCount = 0
	receipt.Derived.WriteAmplification = 1.2

	require.Empty(t, EvaluateFixture(f, receipt))
}

// TestEvaluateFixture_CopyOnlyViolations pins the violation output for
// every enforced budget: KPI names, observed values and targets.
func TestEvaluateFixture_CopyOnlyViolations(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	receipt := NewPerformanceReceiptV1()
	receipt.Timing.WallMs = 10
	receipt.Media.FramesDecoded = 12
	receipt.Media.Frames = 300
	receipt.Media.EncodePasses = 1
	receipt.Process.FfmpegExecCount = 2
	receipt.Process.FfprobeExecCount = 1
	receipt.Process.ExternalProcessCount = 3
	receipt.Derived.WriteAmplification = 2.0

	v := EvaluateFixture(f, receipt)
	require.Len(t, v, 7)
	got := map[string]BudgetViolation{}
	for _, bv := range v {
		got[bv.KPI] = bv
	}
	require.Equal(t, float64(12), got["correctness.video_decode_frames"].Value)
	require.Zero(t, got["correctness.video_decode_frames"].Target)
	require.Equal(t, float64(300), got["correctness.video_encode_frames"].Value)
	require.Equal(t, float64(1), got["correctness.audio_encode_passes"].Value)
	require.Equal(t, float64(2), got["architecture.ffmpeg_exec"].Value)
	require.Equal(t, float64(1), got["architecture.ffprobe_exec"].Value)
	require.Equal(t, float64(3), got["architecture.execve"].Value)
	require.InDelta(t, 2.0, got["io.write_amplification"].Value, 1e-9)
	require.InDelta(t, 1.5, got["io.write_amplification"].Target, 1e-9)
}

// TestEvaluateFixture_EnforcesWallCeiling pins that a per-run wall clock
// above the enforced p95 ceiling is a violation, and one below is not.
func TestEvaluateFixture_EnforcesWallCeiling(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopy5MLow)
	f.Budget.Performance.P95WallMSMax = MaxInt64(5000) // pin a ceiling for this test

	over := NewPerformanceReceiptV1()
	over.Timing.WallMs = 5200
	v := EvaluateFixture(f, over)
	require.Len(t, v, 1)
	require.Equal(t, "performance.wall_ms", v[0].KPI)
	require.InDelta(t, 5200, v[0].Value, 1e-9)
	require.InDelta(t, 5000, v[0].Target, 1e-9)

	under := NewPerformanceReceiptV1()
	under.Timing.WallMs = 4800
	require.Empty(t, EvaluateFixture(f, under))
}

// TestEvaluateFixture_TBDBudgetsIgnored pins the fail-open behavior for
// unset thresholds: a composite fixture (all budgets TBD) can never
// produce violations, and a nil receipt is a no-op.
func TestEvaluateFixture_TBDBudgetsIgnored(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	c, _ := reg.Fixture(FixtureComposite5MLow)

	receipt := NewPerformanceReceiptV1()
	receipt.Timing.WallMs = 999_999
	receipt.Media.FramesDecoded = 500_000
	receipt.Media.Frames = 500_000
	receipt.Media.EncodePasses = 2
	receipt.Process.FfmpegExecCount = 64
	receipt.Derived.WriteAmplification = 9.9
	require.Empty(t, EvaluateFixture(c, receipt), "TBD budgets must never produce violations")

	require.Nil(t, EvaluateFixture(c, nil))
}

// TestBenchmarkFixture_JSONRoundTrip pins that a fixture survives
// serialization unchanged — the BenchmarkRunner loads fixtures from
// JSON, so the wire format must preserve the immutability contract.
func TestBenchmarkFixture_JSONRoundTrip(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureFinalAudio5M)

	data, err := json.Marshal(f)
	require.NoError(t, err)
	var back BenchmarkFixture
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, f, back)
	require.Equal(t, f.DigestSHA256(), back.DigestSHA256())
}
