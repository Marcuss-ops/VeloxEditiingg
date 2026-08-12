// Command velox-fixture-gate is the CI hook for the deterministic
// fixture gate (plan §17): it checks a PerformanceReceiptV1 (plus the
// runner-collected GateEvidence) against the canonical fixture's
// fail-hard invariants — execve/encode/decode forbidden, unexpected
// temp files, unexpected artifact SHA — and exits non-zero on any
// violation. Timing budgets are NOT gated here (they belong on the
// dedicated benchmark worker via EvaluateFixture).
//
// Usage:
//
//	velox-fixture-gate -fixture COPY_5M_HIGH \
//	  -receipt receipt.json -evidence evidence.json
//
// The receipt is a serialized PerformanceReceiptV1; the evidence is a
// serialized performance.GateEvidence.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"velox-worker-agent/pkg/performance"
)

func main() {
	fixtureID := flag.String("fixture", "", "canonical fixture ID (e.g. COPY_5M_HIGH)")
	receiptPath := flag.String("receipt", "", "path to PerformanceReceiptV1 JSON")
	evidencePath := flag.String("evidence", "", "path to GateEvidence JSON")
	flag.Parse()

	if *fixtureID == "" || *receiptPath == "" || *evidencePath == "" {
		fmt.Fprintln(os.Stderr, "usage: velox-fixture-gate -fixture ID -receipt receipt.json -evidence evidence.json")
		os.Exit(2)
	}

	fixture, ok := performance.NewBenchmarkFixtureRegistry().Fixture(performance.BenchmarkFixtureID(*fixtureID))
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown fixture %q\n", *fixtureID)
		os.Exit(2)
	}
	if fixture.ExpectedArtifactSHA256 == "" && fixture.Budget.Correctness.ArtifactSHARequired {
		fmt.Fprintf(os.Stderr, "NOTE: fixture %s requires a pinned artifact SHA but none is registered — the artifact_sha invariant is SKIPPED (pin the SHAs at asset registration)\n", fixture.ID)
	}

	receiptData, err := os.ReadFile(*receiptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read receipt: %v\n", err)
		os.Exit(2)
	}
	var receipt performance.PerformanceReceiptV1
	if err := json.Unmarshal(receiptData, &receipt); err != nil {
		fmt.Fprintf(os.Stderr, "parse receipt: %v\n", err)
		os.Exit(2)
	}

	evidenceData, err := os.ReadFile(*evidencePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read evidence: %v\n", err)
		os.Exit(2)
	}
	var evidence performance.GateEvidence
	if err := json.Unmarshal(evidenceData, &evidence); err != nil {
		fmt.Fprintf(os.Stderr, "parse evidence: %v\n", err)
		os.Exit(2)
	}

	violations := performance.CheckFixtureGate(fixture, &receipt, evidence)
	if len(violations) == 0 {
		fmt.Printf("fixture gate %s: PASS (digest %s)\n", fixture.ID, fixture.DigestSHA256())
		return
	}
	for _, violation := range violations {
		fmt.Fprintf(os.Stderr, "FAIL: %s\n", violation.Error())
	}
	os.Exit(1)
}
