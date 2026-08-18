package performance

// compare_architecture.go owns CompareArchitecture (plan §17): the
// cross-fixture comparison between the COPY_ONLY_CANONICAL_5M_V1 track
// (zero decode/encode packet copy) and the COMPLEX_CANONICAL_5M_V1 track
// (decode → scale → encode + multi-track mix). It is deliberately NOT
// CompareBenchmarkRuns: that function compares two runs of the SAME fixture
// (baseline vs candidate regression), while this one compares two DIFFERENT
// fixtures to certify the architectural win — the copy-only path must be
// cheaper on wall/CPU, zero on frames/execve, and never re-encode.
//
// Aggregate semantics mirror compare_runs.go: wall uses the run summary p50,
// and the deterministic architecture counters (frames encoded/decoded,
// external execve, asset bytes staged) are aggregated as the MAX over
// successful observations so any violating run dominates the report. CPU and
// artifact finalization are also MAX-aggregated (they are dominated by the
// encode side, and a single slow observation is the interesting signal).

import (
	"fmt"
	"strings"
)

// ArchitectureMetric is one copy-only vs complex comparison point.
// Ratio is copyOnly / complex (lower-is-better): 0 means the copy-only
// path did zero of that work.
type ArchitectureMetric struct {
	CopyOnly float64 `json:"copy_only"`
	Complex  float64 `json:"complex"`
	Ratio    float64 `json:"ratio"`
}

// ArchitectureComparison is the machine-readable cross-fixture report
// consumed by the dedicated benchmark worker.
type ArchitectureComparison struct {
	CopyOnlyFixtureID BenchmarkFixtureID `json:"copy_only_fixture_id"`
	ComplexFixtureID  BenchmarkFixtureID `json:"complex_fixture_id"`

	WallMSP50        ArchitectureMetric `json:"wall_ms_p50"`
	CPUTotalMS       ArchitectureMetric `json:"cpu_total_ms"`
	FramesEncoded    ArchitectureMetric `json:"frames_encoded"`
	FramesDecoded    ArchitectureMetric `json:"frames_decoded"`
	ExternalExecve   ArchitectureMetric `json:"external_execve"`
	AssetBytesCopied ArchitectureMetric `json:"asset_bytes_copied"`
	ArtifactTotalMS  ArchitectureMetric `json:"artifact_total_ms"`

	// InvariantViolations lists every architectural invariant that broke:
	// a copy-only run that re-encoded (frames/decoded > 0), spawned an
	// external process, or was not strictly cheaper on wall/CPU; or a
	// complex run that did not actually encode.
	InvariantViolations []string `json:"invariant_violations,omitempty"`

	// CopyOnlyWins is true when every invariant holds: the copy-only path
	// is zero-re-encode, zero-spawn and strictly cheaper than the complex
	// path, while the complex path genuinely encoded.
	CopyOnlyWins bool `json:"copy_only_wins"`
}

// CompareArchitecture compares a COPY_ONLY_CANONICAL_5M_V1 run against a
// COMPLEX_CANONICAL_5M_V1 run. The two sides must be non-nil and carry the
// exact canonical fixture IDs — anything else is a caller error, not a
// violation (this report only ever means "copy-only vs full encode").
func CompareArchitecture(copyOnly, complex *BenchmarkRun) (*ArchitectureComparison, error) {
	if copyOnly == nil || complex == nil {
		return nil, fmt.Errorf("compare architecture: copy-only and complex runs are required")
	}
	if copyOnly.FixtureID != FixtureCopyOnlyCanonical5MV1 {
		return nil, fmt.Errorf("compare architecture: copy-only side must be %s, got %s", FixtureCopyOnlyCanonical5MV1, copyOnly.FixtureID)
	}
	if complex.FixtureID != FixtureComplexCanonical5MV1 {
		return nil, fmt.Errorf("compare architecture: complex side must be %s, got %s", FixtureComplexCanonical5MV1, complex.FixtureID)
	}

	c := &ArchitectureComparison{
		CopyOnlyFixtureID: copyOnly.FixtureID,
		ComplexFixtureID:  complex.FixtureID,
		WallMSP50:         architectureMetric(copyOnly.Summary.WallMSP50, complex.Summary.WallMSP50),
		CPUTotalMS: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, cpuTotalOf),
			maxInvariantMetric(complex.Receipts, cpuTotalOf)),
		FramesEncoded: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, framesEncodedOf),
			maxInvariantMetric(complex.Receipts, framesEncodedOf)),
		FramesDecoded: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, framesDecodedOf),
			maxInvariantMetric(complex.Receipts, framesDecodedOf)),
		ExternalExecve: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, externalExecveOf),
			maxInvariantMetric(complex.Receipts, externalExecveOf)),
		AssetBytesCopied: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, assetBytesCopiedOf),
			maxInvariantMetric(complex.Receipts, assetBytesCopiedOf)),
		ArtifactTotalMS: architectureMetric(
			maxInvariantMetric(copyOnly.Receipts, artifactTotalOf),
			maxInvariantMetric(complex.Receipts, artifactTotalOf)),
	}

	var violations []string
	if c.FramesEncoded.CopyOnly != 0 {
		violations = append(violations, "copy-only frames_encoded != 0 (hidden transcode)")
	}
	if c.FramesDecoded.CopyOnly != 0 {
		violations = append(violations, "copy-only frames_decoded != 0 (hidden decode)")
	}
	if c.ExternalExecve.CopyOnly != 0 {
		violations = append(violations, "copy-only external_execve != 0 (zero-spawn contract violated)")
	}
	if c.FramesEncoded.Complex == 0 {
		violations = append(violations, "complex frames_encoded == 0 (complex fixture must actually encode)")
	}
	if c.WallMSP50.Complex > 0 && c.WallMSP50.CopyOnly >= c.WallMSP50.Complex {
		violations = append(violations, "copy-only wall time is not cheaper than complex")
	}
	if c.CPUTotalMS.Complex > 0 && c.CPUTotalMS.CopyOnly >= c.CPUTotalMS.Complex {
		violations = append(violations, "copy-only CPU is not cheaper than complex")
	}
	c.InvariantViolations = violations
	c.CopyOnlyWins = len(violations) == 0
	return c, nil
}

// architectureMetric builds one copy-only vs complex point with the
// copyOnly/complex ratio (0 when the complex denominator is 0).
func architectureMetric(copyOnly, complex float64) ArchitectureMetric {
	m := ArchitectureMetric{CopyOnly: copyOnly, Complex: complex}
	if complex != 0 {
		m.Ratio = copyOnly / complex
	}
	return m
}

// cpuTotalOf extracts the engine-tree CPU total (milliseconds) from a receipt.
func cpuTotalOf(r *PerformanceReceiptV1) float64 { return float64(r.CPU.CPUTotalMs) }

// framesEncodedOf extracts the encoded-frame count from a receipt.
func framesEncodedOf(r *PerformanceReceiptV1) float64 { return float64(r.Media.Frames) }

// framesDecodedOf extracts the decoded-frame count from a receipt.
func framesDecodedOf(r *PerformanceReceiptV1) float64 { return float64(r.Media.FramesDecoded) }

// assetBytesCopiedOf extracts the staged asset bytes from a receipt.
func assetBytesCopiedOf(r *PerformanceReceiptV1) float64 { return float64(r.IO.AssetBytesCopied) }

// artifactTotalOf extracts the finalization time from a receipt.
func artifactTotalOf(r *PerformanceReceiptV1) float64 { return float64(r.Timing.ArtifactTotalMs) }

// FormatTable renders the copy-only vs complex architecture table.
func (c *ArchitectureComparison) FormatTable() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s vs %s\n\n", c.CopyOnlyFixtureID, c.ComplexFixtureID)
	fmt.Fprintf(&b, "METRIC                    COPY_ONLY     COMPLEX      RATIO\n")
	row := func(name string, m ArchitectureMetric) {
		fmt.Fprintf(&b, "  %-22s %11s %12s %9s\n", name, formatValue(m.CopyOnly), formatValue(m.Complex), formatRatio(m.Ratio))
	}
	row("wall p50 (ms)", c.WallMSP50)
	row("cpu total (ms)", c.CPUTotalMS)
	row("frames encoded", c.FramesEncoded)
	row("frames decoded", c.FramesDecoded)
	row("external execve", c.ExternalExecve)
	row("asset bytes copied", c.AssetBytesCopied)
	row("artifact total (ms)", c.ArtifactTotalMS)
	if len(c.InvariantViolations) > 0 {
		fmt.Fprintf(&b, "\nINVARIANT VIOLATIONS:\n")
		for _, v := range c.InvariantViolations {
			fmt.Fprintf(&b, "  - %s\n", v)
		}
	}
	if c.CopyOnlyWins {
		fmt.Fprintf(&b, "\nVERDICT: copy-only wins (zero re-encode, cheaper wall/CPU)\n")
	} else {
		fmt.Fprintf(&b, "\nVERDICT: architecture invariants violated\n")
	}
	return b.String()
}

// formatRatio renders a ratio as a percentage string (0.25 → "25%").
func formatRatio(r float64) string {
	return fmt.Sprintf("%.1f%%", r*100)
}
