package performance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFixtureGate_CopyOnlyPass pins the happy path: a receipt that
// respects the copy-only invariants and a clean filesystem produce no
// gate violations.
func TestFixtureGate_CopyOnlyPass(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	receipt := NewPerformanceReceiptV1()
	receipt.Media.FramesDecoded = 0
	receipt.Media.Frames = 0
	receipt.Process.ExternalProcessCount = 0

	require.Empty(t, CheckFixtureGate(f, receipt, GateEvidence{}))
}

// TestFixtureGate_ExecveForbidden pins the fail-hard execve invariant:
// any external process spawn breaks the gate, with a CI-friendly
// message.
func TestFixtureGate_ExecveForbidden(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopy5MHigh)

	receipt := NewPerformanceReceiptV1()
	receipt.Process.ExternalProcessCount = 2

	v := CheckFixtureGate(f, receipt, GateEvidence{})
	require.Len(t, v, 1)
	require.Equal(t, "execve", v[0].Invariant)
	require.Equal(t, "<= 0", v[0].Expected)
	require.Equal(t, "2", v[0].Got)
	require.Contains(t, v[0].Error(), "fixture gate execve")
}

// TestFixtureGate_EncodeDecodeForbidden pins the encode/decode
// invariants: frame encoding and frame decoding are both fail-hard for
// copy-only.
func TestFixtureGate_EncodeDecodeForbidden(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopy5MLow)

	receipt := NewPerformanceReceiptV1()
	receipt.Media.Frames = 100
	receipt.Media.FramesDecoded = 50

	v := CheckFixtureGate(f, receipt, GateEvidence{})
	require.Len(t, v, 2)
	invariants := map[string]GateViolation{v[0].Invariant: v[0], v[1].Invariant: v[1]}
	require.Contains(t, invariants, "encode")
	require.Contains(t, invariants, "decode")
	require.Equal(t, "100", invariants["encode"].Got)
	require.Equal(t, "50", invariants["decode"].Got)
}

// TestFixtureGate_UnexpectedTempFiles pins the temp-file invariants:
// a segment/temp file count over the budget AND any leftover file when
// the budget is a zero invariant both break the gate.
func TestFixtureGate_UnexpectedTempFiles(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureCopyOnlyCanonical5MV1)

	receipt := NewPerformanceReceiptV1()

	// Count over the zero budget.
	v := CheckFixtureGate(f, receipt, GateEvidence{TempSegmentFiles: 3})
	require.Len(t, v, 1)
	require.Equal(t, "temp_segment_files", v[0].Invariant)

	// Leftover files when the budget is a zero invariant: names are
	// reported sorted.
	v = CheckFixtureGate(f, receipt, GateEvidence{TempFiles: []string{"seg_2.ts", "seg_1.ts"}})
	require.Len(t, v, 1)
	require.Equal(t, "unexpected_temp_file", v[0].Invariant)
	require.Equal(t, "seg_1.ts, seg_2.ts", v[0].Got)

	// Combined: count over budget AND leftover names → 2 violations.
	v = CheckFixtureGate(f, receipt, GateEvidence{TempSegmentFiles: 3, TempFiles: []string{"seg_1.ts"}})
	require.Len(t, v, 2)
	invariants := map[string]bool{v[0].Invariant: true, v[1].Invariant: true}
	require.True(t, invariants["temp_segment_files"])
	require.True(t, invariants["unexpected_temp_file"])
}

// TestFixtureGate_ArtifactSHA pins the deterministic artifact check:
// compared only when BOTH the fixture SHA is pinned and the evidence
// carries one; a mismatch breaks the gate, equality passes, and an
// unpinned fixture SHA is skipped.
func TestFixtureGate_ArtifactSHA(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	f, _ := reg.Fixture(FixtureFinalAudio5M)
	pinned := f
	pinned.ExpectedArtifactSHA256 = "abc123def"
	receipt := NewPerformanceReceiptV1()

	// Mismatch → violation.
	v := CheckFixtureGate(pinned, receipt, GateEvidence{ArtifactSHA256: "deadbeef"})
	require.Len(t, v, 1)
	require.Equal(t, "artifact_sha", v[0].Invariant)
	require.Equal(t, "abc123def", v[0].Expected)
	require.Equal(t, "deadbeef", v[0].Got)

	// Match → pass.
	require.Empty(t, CheckFixtureGate(pinned, receipt, GateEvidence{ArtifactSHA256: "abc123def"}))

	// Fixture SHA not pinned → SHA is skipped (other invariants still
	// evaluated: the receipt is clean here, so no violations).
	require.Empty(t, CheckFixtureGate(f, receipt, GateEvidence{ArtifactSHA256: "whatever"}))
}

// TestFixtureGate_TBDBudgetsIgnored pins the fail-open behavior for
// fixtures without enforced invariants: a composite receipt that would
// be catastrophic for copy-only produces no violations.
func TestFixtureGate_TBDBudgetsIgnored(t *testing.T) {
	reg := NewBenchmarkFixtureRegistry()
	c, _ := reg.Fixture(FixtureComposite5MLow)

	receipt := NewPerformanceReceiptV1()
	receipt.Media.Frames = 500_000
	receipt.Media.FramesDecoded = 500_000
	receipt.Process.ExternalProcessCount = 64

	require.Empty(t, CheckFixtureGate(c, receipt, GateEvidence{TempFiles: []string{"x.ts"}}))
	require.Nil(t, CheckFixtureGate(c, nil, GateEvidence{}))
}

// TestFixtureGate_ErrorFormat pins the CI-friendly one-line rendering.
func TestFixtureGate_ErrorFormat(t *testing.T) {
	v := GateViolation{Invariant: "encode", Expected: "<= 0", Got: "12", Message: "copy-only invariant: frame encoding forbidden"}
	s := v.Error()
	require.Contains(t, s, "fixture gate encode")
	require.Contains(t, s, "expected <= 0")
	require.Contains(t, s, "got 12")
	require.True(t, strings.HasPrefix(s, "fixture gate"), "message must start with the gate prefix for CI grep")
}
