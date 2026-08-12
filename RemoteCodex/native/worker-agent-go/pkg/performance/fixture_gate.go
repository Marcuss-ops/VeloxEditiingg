package performance

// fixture_gate.go — the CI deterministic gate (plan §17). On SHARED CI
// runners the timing budgets (EvaluateFixture) are too noisy to gate
// on, so the gate fails HARD on deterministic invariants instead:
// execve forbidden, encode forbidden, decode forbidden, unexpected temp
// files, unexpected artifact SHA. None of these depend on neighbor
// load, CPU model or storage speed — they are safe on any runner.
//
// Distinction (the plan's two-tier gate): EvaluateFixture checks the
// BUDGETS (wall-clock p50/p95, amplification…) and belongs on the
// dedicated benchmark worker; CheckFixtureGate checks the INVARIANTS
// and is the CI gate. Both reuse the same BudgetMax thresholds.

import (
	"fmt"
	"sort"
	"strings"
)

// GateEvidence carries the deterministic facts a receipt cannot express
// (the receipt is pre-manifest and carries no filesystem snapshot).
// The BenchmarkRunner collects it after the render.
type GateEvidence struct {
	// ArtifactSHA256 is the SHA-256 of the produced artifact
	// (publisher manifest), compared against the fixture's pinned
	// ExpectedArtifactSHA256.
	ArtifactSHA256 string `json:"artifact_sha256,omitempty"`
	// TempSegmentFiles is the count of segment/temp files the runner
	// observed on the filesystem during the render (workdir sweep).
	TempSegmentFiles int `json:"temp_segment_files"`
	// TempFiles names every unexpected temp file the runner observed;
	// empty means none were left behind.
	TempFiles []string `json:"temp_files,omitempty"`
}

// GateViolation is one fail-hard invariant breach.
type GateViolation struct {
	Invariant string `json:"invariant"`
	Expected  string `json:"expected"`
	Got       string `json:"got"`
	Message   string `json:"message"`
}

// Error renders the violation as a single-line CI-friendly message.
func (v GateViolation) Error() string {
	return fmt.Sprintf("fixture gate %s: expected %s, got %s (%s)", v.Invariant, v.Expected, v.Got, v.Message)
}

// CheckFixtureGate is the fail-hard deterministic gate. A nil receipt
// is a no-op (a failed render has no invariants to check). Only
// enforced (Set=true) thresholds are checked; TBD thresholds are
// skipped. The artifact SHA is compared only when BOTH the fixture SHA
// is pinned and the evidence carries one — a fixture marked
// ArtifactSHARequired but without a pinned SHA cannot gate on SHA yet
// (the SHAs are pinned at asset registration).
func CheckFixtureGate(fixture BenchmarkFixture, receipt *PerformanceReceiptV1, evidence GateEvidence) []GateViolation {
	if receipt == nil {
		return nil
	}
	var v []GateViolation
	b := fixture.Budget

	// execve forbidden: the engine must not spawn external tool
	// processes (copy-only Phase-1: zero external execve).
	if exceeded, enforced := enforcedExceeded(b.Architecture.ExternalExecMax, float64(receipt.Process.ExternalProcessCount)); enforced && exceeded {
		v = append(v, gateViolation("execve", b.Architecture.ExternalExecMax, receipt.Process.ExternalProcessCount,
			"copy-only invariant: external execve forbidden"))
	}
	// encode forbidden.
	if exceeded, enforced := enforcedExceeded(b.Correctness.VideoEncodeFramesMax, float64(receipt.Media.Frames)); enforced && exceeded {
		v = append(v, gateViolation("encode", b.Correctness.VideoEncodeFramesMax, receipt.Media.Frames,
			"copy-only invariant: frame encoding forbidden"))
	}
	// decode forbidden.
	if exceeded, enforced := enforcedExceeded(b.Correctness.VideoDecodeFramesMax, float64(receipt.Media.FramesDecoded)); enforced && exceeded {
		v = append(v, gateViolation("decode", b.Correctness.VideoDecodeFramesMax, receipt.Media.FramesDecoded,
			"copy-only invariant: frame decoding forbidden"))
	}
	// unexpected temp files: count over budget, or ANY file when the
	// budget is a zero invariant (the copy-only fixtures' TempSegmentFilesMax
	// is exactly 0, so any leftover file is flagged by the name check).
	if exceeded, enforced := enforcedExceeded(b.Architecture.TempSegmentFilesMax, float64(evidence.TempSegmentFiles)); enforced && exceeded {
		v = append(v, gateViolation("temp_segment_files", b.Architecture.TempSegmentFilesMax, int64(evidence.TempSegmentFiles),
			"unexpected temp segment files"))
	}
	if b.Architecture.TempSegmentFilesMax.Set && int64(b.Architecture.TempSegmentFilesMax.Value) == 0 && len(evidence.TempFiles) > 0 {
		names := append([]string(nil), evidence.TempFiles...)
		sort.Strings(names)
		v = append(v, GateViolation{
			Invariant: "unexpected_temp_file",
			Expected:  "no temp files on disk",
			Got:       strings.Join(names, ", "),
			Message:   "copy-only invariant: no staging/materialization files allowed",
		})
	}
	// unexpected artifact SHA.
	if fixture.ExpectedArtifactSHA256 != "" && evidence.ArtifactSHA256 != "" && fixture.ExpectedArtifactSHA256 != evidence.ArtifactSHA256 {
		v = append(v, GateViolation{
			Invariant: "artifact_sha",
			Expected:  fixture.ExpectedArtifactSHA256,
			Got:       evidence.ArtifactSHA256,
			Message:   "deterministic artifact mismatch",
		})
	}
	return v
}

// gateViolation builds a threshold-style violation (used by the
// int64-budget invariants).
func gateViolation(invariant string, max BudgetMax, actual int64, msg string) GateViolation {
	return GateViolation{
		Invariant: invariant,
		Expected:  fmt.Sprintf("<= %d", int64(max.Value)),
		Got:       fmt.Sprintf("%d", actual),
		Message:   msg,
	}
}
