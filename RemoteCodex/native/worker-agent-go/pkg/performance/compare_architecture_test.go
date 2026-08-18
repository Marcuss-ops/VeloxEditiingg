package performance

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// architectureRun builds a single-observation BenchmarkRun with the receipt
// fields the architecture comparison consumes (wall, CPU, frames, execve,
// staged bytes, finalization). The summary is computed through the runner's
// own summarizeObservations so the comparison consumes exactly what
// velox-benchmark would produce.
func architectureRun(id BenchmarkFixtureID, wallMS, cpuMS, framesEncoded, framesDecoded, execve, assetBytesCopied, artifactMS int64) *BenchmarkRun {
	r := NewPerformanceReceiptV1()
	r.Timing.WallMs = wallMS
	r.CPU.CPUTotalMs = cpuMS
	r.Media.Frames = framesEncoded
	r.Media.FramesDecoded = framesDecoded
	r.Process.ExternalProcessCount = execve
	r.IO.AssetBytesCopied = assetBytesCopied
	r.Timing.ArtifactTotalMs = artifactMS
	obs := []BenchmarkRunObservation{{RunIndex: 0, WallMS: wallMS, Receipt: r}}
	return &BenchmarkRun{
		BenchmarkRunID: "bench-arch",
		FixtureID:      id,
		GitCommit:      "commit-arch",
		CacheMode:      CacheModeWarm,
		WorkerID:       "worker-arch",
		Receipts:       obs,
		Summary:        summarizeObservations(obs),
	}
}

func copyOnlyArchitectureRun() *BenchmarkRun {
	// 2.5 s wall, 100 ms CPU, zero decode/encode/execve/staging, 10 ms mux.
	return architectureRun(FixtureCopyOnlyCanonical5MV1, 2500, 100, 0, 0, 0, 0, 10)
}

func complexArchitectureRun() *BenchmarkRun {
	// 20 s wall, 50 s CPU (decode→scale→encode), 9000 frames, 24 execve,
	// staged bytes, 2 s finalization.
	return architectureRun(FixtureComplexCanonical5MV1, 20000, 50000, 9000, 9000, 24, 600_000, 2000)
}

// TestCompareArchitecture_CopyOnlyWins pins the happy path: the copy-only
// fixture is zero on frames/execve and strictly cheaper on wall/CPU, the
// complex fixture genuinely encodes, and the report is a clean win.
func TestCompareArchitecture_CopyOnlyWins(t *testing.T) {
	c, err := CompareArchitecture(copyOnlyArchitectureRun(), complexArchitectureRun())
	require.NoError(t, err)
	require.Equal(t, FixtureCopyOnlyCanonical5MV1, c.CopyOnlyFixtureID)
	require.Equal(t, FixtureComplexCanonical5MV1, c.ComplexFixtureID)

	require.Equal(t, float64(2500), c.WallMSP50.CopyOnly)
	require.Equal(t, float64(20000), c.WallMSP50.Complex)
	require.InDelta(t, 0.125, c.WallMSP50.Ratio, 1e-9)
	require.InDelta(t, 0.002, c.CPUTotalMS.Ratio, 1e-9)

	require.Equal(t, float64(0), c.FramesEncoded.CopyOnly)
	require.Equal(t, float64(9000), c.FramesEncoded.Complex)
	require.Equal(t, float64(0), c.FramesEncoded.Ratio)
	require.Equal(t, float64(0), c.ExternalExecve.CopyOnly)
	require.Equal(t, float64(0), c.AssetBytesCopied.CopyOnly)

	require.Empty(t, c.InvariantViolations)
	require.True(t, c.CopyOnlyWins)
	require.Contains(t, c.FormatTable(), "copy-only wins")
	require.Contains(t, c.FormatTable(), "frames encoded")
}

// TestCompareArchitecture_DetectsHiddenTranscode pins the §16 release gate:
// a copy-only run that re-encodes a single frame is flagged as a violation
// and the win verdict flips to false, even if wall/CPU look fine.
func TestCompareArchitecture_DetectsHiddenTranscode(t *testing.T) {
	copyOnly := copyOnlyArchitectureRun()
	copyOnly.Receipts[0].Receipt.Media.Frames = 1
	copyOnly.Receipts[0].Receipt.Media.FramesDecoded = 1

	c, err := CompareArchitecture(copyOnly, complexArchitectureRun())
	require.NoError(t, err)
	require.False(t, c.CopyOnlyWins)
	require.Contains(t, c.InvariantViolations, "copy-only frames_encoded != 0 (hidden transcode)")
	require.Contains(t, c.InvariantViolations, "copy-only frames_decoded != 0 (hidden decode)")
	require.Contains(t, c.FormatTable(), "INVARIANT VIOLATIONS")
}

// TestCompareArchitecture_DetectsZeroSpawnViolation pins the zero-spawn
// architecture invariant on the copy-only path.
func TestCompareArchitecture_DetectsZeroSpawnViolation(t *testing.T) {
	copyOnly := copyOnlyArchitectureRun()
	copyOnly.Receipts[0].Receipt.Process.ExternalProcessCount = 1

	c, err := CompareArchitecture(copyOnly, complexArchitectureRun())
	require.NoError(t, err)
	require.False(t, c.CopyOnlyWins)
	require.Contains(t, c.InvariantViolations, "copy-only external_execve != 0 (zero-spawn contract violated)")
}

// TestCompareArchitecture_DetectsZeroEncodeComplex pins that a complex run
// that somehow did no encode work invalidates the comparison (it must
// actually exercise decode→encode to be a meaningful baseline).
func TestCompareArchitecture_DetectsZeroEncodeComplex(t *testing.T) {
	complex := complexArchitectureRun()
	complex.Receipts[0].Receipt.Media.Frames = 0

	c, err := CompareArchitecture(copyOnlyArchitectureRun(), complex)
	require.NoError(t, err)
	require.False(t, c.CopyOnlyWins)
	require.Contains(t, c.InvariantViolations, "complex frames_encoded == 0 (complex fixture must actually encode)")
}

// TestCompareArchitecture_RejectsWrongFixtures pins the guardrails: this
// comparison only ever means copy-only vs full encode, so any other fixture
// pairing is a caller error.
func TestCompareArchitecture_RejectsWrongFixtures(t *testing.T) {
	_, err := CompareArchitecture(complexArchitectureRun(), complexArchitectureRun())
	require.ErrorContains(t, err, "copy-only side must be")

	_, err = CompareArchitecture(copyOnlyArchitectureRun(), copyOnlyArchitectureRun())
	require.ErrorContains(t, err, "complex side must be")

	_, err = CompareArchitecture(nil, complexArchitectureRun())
	require.ErrorContains(t, err, "required")
}

// TestArchitectureComparison_JSONRoundTrip pins the machine-readable wire
// format of the architecture report.
func TestArchitectureComparison_JSONRoundTrip(t *testing.T) {
	c, err := CompareArchitecture(copyOnlyArchitectureRun(), complexArchitectureRun())
	require.NoError(t, err)

	data, err := json.Marshal(c)
	require.NoError(t, err)
	var back ArchitectureComparison
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, *c, back)
	require.True(t, back.CopyOnlyWins)
}
