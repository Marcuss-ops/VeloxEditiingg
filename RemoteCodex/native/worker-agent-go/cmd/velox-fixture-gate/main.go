// Command velox-fixture-gate is the CI hook for the fixture gate
// (plan §17) with an explicit TWO-TIER split:
//
// TIER 1 — deterministic (-tier deterministic, the DEFAULT): checks a
// PerformanceReceiptV1 (plus the runner-collected GateEvidence) against
// the canonical fixture's fail-hard invariants — execve/encode/decode
// forbidden, unexpected temp files, unexpected artifact SHA. None of
// these depend on neighbor load, CPU model or storage speed, so this
// tier is SAFE ON ANY RUNNER, including shared CI.
//
// TIER 2 — performance (-tier performance): checks a whole
// BenchmarkRun JSON (velox-benchmark output) against the fixture's
// timing/throughput/CPU/amplification budgets. These KPIs are too
// noisy on shared runners, so this tier must run ONLY on the dedicated
// SELF-HOSTED benchmark worker (.github/workflows/benchmark-worker.yml,
// runs-on: [self-hosted, benchmark]).
//
// Usage:
//
//	velox-fixture-gate -tier deterministic -fixture COPY_5M_HIGH \
//	  -receipt receipt.json -evidence evidence.json
//	velox-fixture-gate -tier performance -fixture COPY_ONLY_CANONICAL_5M_V1 \
//	  -run run.json
//
// Exit codes: 0 pass / 1 violations / 2 usage.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"velox-worker-agent/pkg/performance"
)

func main() {
	tier := flag.String("tier", string(performance.TierDeterministic), "gate tier: deterministic (any runner) | performance (dedicated benchmark worker only)")
	fixtureID := flag.String("fixture", "", "canonical fixture ID (e.g. COPY_5M_HIGH)")
	receiptPath := flag.String("receipt", "", "tier deterministic: path to PerformanceReceiptV1 JSON")
	evidencePath := flag.String("evidence", "", "tier deterministic: path to GateEvidence JSON")
	runPath := flag.String("run", "", "tier performance: path to BenchmarkRun JSON (velox-benchmark output)")
	flag.Parse()

	if *fixtureID == "" {
		fmt.Fprintln(os.Stderr, "usage: velox-fixture-gate -tier deterministic -fixture ID -receipt receipt.json -evidence evidence.json | -tier performance -fixture ID -run run.json")
		os.Exit(2)
	}
	fixture, ok := performance.NewBenchmarkFixtureRegistry().Fixture(performance.BenchmarkFixtureID(*fixtureID))
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown fixture %q\n", *fixtureID)
		os.Exit(2)
	}

	switch performance.GateTier(*tier) {
	case performance.TierDeterministic:
		runDeterministic(fixture, *receiptPath, *evidencePath)
	case performance.TierPerformance:
		runPerformance(fixture, *runPath)
	default:
		fmt.Fprintf(os.Stderr, "unknown tier %q (want deterministic | performance)\n", *tier)
		os.Exit(2)
	}
}

// runDeterministic is the TIER-1 gate (shared-CI safe): fail-hard
// invariants only.
func runDeterministic(fixture performance.BenchmarkFixture, receiptPath, evidencePath string) {
	if receiptPath == "" || evidencePath == "" {
		fmt.Fprintln(os.Stderr, "tier deterministic requires -receipt and -evidence")
		os.Exit(2)
	}
	if fixture.ExpectedArtifactSHA256 == "" && fixture.Budget.Correctness.ArtifactSHARequired {
		fmt.Fprintf(os.Stderr, "NOTE: fixture %s requires a pinned artifact SHA but none is registered — the artifact_sha invariant is SKIPPED (pin the SHAs at asset registration)\n", fixture.ID)
	}

	receiptData, err := os.ReadFile(receiptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read receipt: %v\n", err)
		os.Exit(2)
	}
	var receipt performance.PerformanceReceiptV1
	if err := json.Unmarshal(receiptData, &receipt); err != nil {
		fmt.Fprintf(os.Stderr, "parse receipt: %v\n", err)
		os.Exit(2)
	}

	evidenceData, err := os.ReadFile(evidencePath)
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
		fmt.Printf("fixture gate %s: PASS (tier deterministic, digest %s)\n", fixture.ID, fixture.DigestSHA256())
		return
	}
	for _, violation := range violations {
		fmt.Fprintf(os.Stderr, "FAIL: %s\n", violation.Error())
	}
	os.Exit(1)
}

// runPerformance is the TIER-2 gate: dedicated self-hosted benchmark
// worker only. Evaluates the whole run against the fixture's
// performance + I/O budgets.
func runPerformance(fixture performance.BenchmarkFixture, runPath string) {
	if runPath == "" {
		fmt.Fprintln(os.Stderr, "tier performance requires -run (a BenchmarkRun JSON from velox-benchmark)")
		os.Exit(2)
	}
	runData, err := os.ReadFile(runPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read run: %v\n", err)
		os.Exit(2)
	}
	var run performance.BenchmarkRun
	if err := json.Unmarshal(runData, &run); err != nil {
		fmt.Fprintf(os.Stderr, "parse run: %v\n", err)
		os.Exit(2)
	}

	violations, err := performance.CheckPerformanceBudgets(fixture, &run)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	if len(violations) == 0 {
		fmt.Printf("fixture gate %s: PASS (tier performance, budgets digest %s)\n", fixture.ID, fixture.DigestSHA256())
		return
	}
	for _, v := range violations {
		fmt.Fprintf(os.Stderr, "FAIL: performance budget %s: value %.3f, target %.3f (%s)\n", v.KPI, v.Value, v.Target, v.Message)
	}
	os.Exit(1)
}
