// Command velox-benchmark runs a canonical fixture multiple times
// through the BenchmarkRunner and emits the machine-readable
// BenchmarkRun JSON (plan §9/§21): benchmark_run_id, git_commit,
// worker_id, host fingerprint, cold/warm cache, concurrency, per-run
// receipts and the deterministic artifact SHA.
//
// The render backend is injected: today the only built-in is -stub
// (synthetic receipts, for exercising the runner/report pipeline); the
// production RenderRunner drives pipeline.Runner →
// Assembler.Assemble once the fixture assets are registered.
//
// Usage:
//
//	velox-benchmark -fixture COPY_5M_LOW -runs 5 -concurrency 2 \
//	  -worker-id w1 -git-commit $(git rev-parse HEAD) -cache warm \
//	  -stub [-out run.json]
//
// Exit codes: 0 = report produced, 1 = runner error, 2 = usage.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"velox-worker-agent/pkg/performance"
)

func main() {
	fixtureID := flag.String("fixture", "", "canonical fixture ID (e.g. COPY_5M_LOW)")
	runs := flag.Int("runs", 3, "number of renders")
	concurrency := flag.Int("concurrency", 1, "maximum concurrent renders")
	cache := flag.String("cache", "warm", "cache mode: warm | cold")
	workerID := flag.String("worker-id", "", "worker id (default: hostname)")
	gitCommit := flag.String("git-commit", "", "git commit of the build (default: $VELOX_GIT_COMMIT or unknown)")
	release := flag.String("release", "", "release tag of the build")
	engineDigest := flag.String("engine-digest", "", "engine binary digest")
	stub := flag.Bool("stub", false, "use the stub renderer (synthetic receipts; NOT a real benchmark)")
	outPath := flag.String("out", "", "write the JSON report to this file (default: stdout)")
	flag.Parse()

	if *fixtureID == "" {
		fmt.Fprintln(os.Stderr, "usage: velox-benchmark -fixture ID [-runs N] [-concurrency N] [-cache warm|cold] [-worker-id ID] [-git-commit SHA] [-stub] [-out FILE]")
		os.Exit(2)
	}
	if *cache != "warm" && *cache != "cold" {
		fmt.Fprintf(os.Stderr, "invalid cache mode %q (warm|cold)\n", *cache)
		os.Exit(2)
	}
	if *gitCommit == "" {
		*gitCommit = os.Getenv("VELOX_GIT_COMMIT")
	}

	fixture, ok := performance.NewBenchmarkFixtureRegistry().Fixture(performance.BenchmarkFixtureID(*fixtureID))
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown fixture %q\n", *fixtureID)
		os.Exit(2)
	}

	var renderer performance.RenderRunner
	if *stub {
		renderer = stubRenderer{}
	} else {
		fmt.Fprintln(os.Stderr, "no render backend configured: pass -stub (synthetic) until the production RenderRunner lands with registered fixture assets")
		os.Exit(2)
	}

	runner := &performance.BenchmarkRunner{
		Fixture:      fixture,
		Runs:         *runs,
		Concurrency:  *concurrency,
		CacheMode:    performance.CacheMode(*cache),
		WorkerID:     *workerID,
		GitCommit:    *gitCommit,
		Release:      *release,
		EngineDigest: *engineDigest,
		Renderer:     renderer,
	}

	report, err := runner.Run(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark run failed: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal report: %v\n", err)
		os.Exit(1)
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s (%d bytes)\n", *outPath, len(data))
	} else {
		fmt.Println(string(data))
	}
}

// stubRenderer produces synthetic receipts that satisfy the copy-only
// invariants, for exercising the runner/report pipeline. It is NOT a
// real benchmark: wall clock is a fixed placeholder and the artifact
// SHA is derived from the fixture digest.
type stubRenderer struct{}

func (stubRenderer) Render(_ context.Context, fixture performance.BenchmarkFixture) (performance.BenchmarkRenderResult, error) {
	receipt := performance.NewPerformanceReceiptV1()
	receipt.Identity.BenchmarkFixtureID = string(fixture.ID)
	receipt.Identity.WorkerID = "stub"
	receipt.Timing.WallMs = 2000
	receipt.Media.FramesDecoded = 0
	receipt.Media.Frames = 0
	receipt.Media.EncodePasses = 0
	receipt.Process.ExternalProcessCount = 0
	receipt.Derived.AccountedRatio = 0.97
	receipt.Derived.WriteAmplification = 1.0
	receipt.Derived.ProcessesPerClip = 0
	sha := "stub-" + fixture.DigestSHA256()[:16]
	return performance.BenchmarkRenderResult{
		Receipt:        receipt,
		ArtifactSHA256: sha,
		Evidence:       performance.GateEvidence{ArtifactSHA256: sha},
	}, nil
}
