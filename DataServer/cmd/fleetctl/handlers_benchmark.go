// Package main — fleetctl benchmark sub-command handler.
//
// Usage:
//
//	fleetctl benchmark <worker_id> [--fixture ID] [--max-concurrency N] [--runs N] [--cache-mode cold|warm]
//
// Triggers a concurrent benchmark on the specified worker via the admin API
// and prints the sweet spot result.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

// runBenchmark triggers a concurrent benchmark on the given worker and prints
// the sweet spot result. Flags: --fixture, --max-concurrency, --runs,
// --cache-mode.
func runBenchmark(client *fleetClient, args []string) int {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fixtureID := fs.String("fixture", "", "benchmark fixture ID (required)")
	maxConcurrency := fs.Int("max-concurrency", 4, "maximum concurrency level to test (1-8)")
	runsPerLevel := fs.Int("runs", 3, "number of runs per concurrency level (1-10)")
	cacheMode := fs.String("cache-mode", "cold_cache", "cache mode: cold_cache or warm_cache")
	if err := fs.Parse(args); err != nil {
	fmt.Fprintf(os.Stderr, "fleetctl benchmark: %v\n", err)
	return ExitMisuse
}

	workerID := fs.Arg(0)
	if workerID == "" {
	fmt.Fprintln(os.Stderr, "fleetctl benchmark: worker_id is required (first positional argument)")
	fmt.Fprintln(os.Stderr, "Usage: fleetctl benchmark <worker_id> [--fixture ID] [--max-concurrency N] [--runs N]")
	return ExitMisuse
}
	if *fixtureID == "" {
	fmt.Fprintln(os.Stderr, "fleetctl benchmark: --fixture is required")
	return ExitMisuse
	}

	// Clamp ranges
	if *maxConcurrency < 1 {
		*maxConcurrency = 1
	}
	if *maxConcurrency > 8 {
		*maxConcurrency = 8
	}
	if *runsPerLevel < 1 {
		*runsPerLevel = 1
	}
	if *runsPerLevel > 10 {
		*runsPerLevel = 10
	}

	type benchmarkRequest struct {
		FixtureID      string `json:"fixture_id"`
		WorkerID       string `json:"worker_id"`
		MaxConcurrency int    `json:"max_concurrency"`
		RunsPerLevel   int    `json:"runs_per_level"`
		CacheMode      string `json:"cache_mode"`
	}

	reqBody := benchmarkRequest{
		FixtureID:      *fixtureID,
		WorkerID:       workerID,
		MaxConcurrency: *maxConcurrency,
		RunsPerLevel:   *runsPerLevel,
		CacheMode:      *cacheMode,
	}

	fmt.Fprintf(os.Stderr, "fleetctl benchmark: starting concurrent benchmark on worker %s\n", workerID)
	fmt.Fprintf(os.Stderr, "  fixture=%s  max-concurrency=%d  runs-per-level=%d  cache=%s\n",
		*fixtureID, *maxConcurrency, *runsPerLevel, *cacheMode)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var result struct {
		BenchmarkRunID string `json:"benchmark_run_id"`
		SweetSpot      int    `json:"sweet_spot"`
		LimitingFactor string `json:"limiting_factor"`
		Summary        string `json:"summary"`
		StartedAt      string `json:"started_at"`
		CompletedAt    string `json:"completed_at"`
		Levels         []struct {
			Level            int     `json:"level"`
			SuccessfulRuns   int     `json:"successful_runs"`
			FailedRuns       int     `json:"failed_runs"`
			Throughput       float64 `json:"throughput"`
			AvgRenderWallMS  float64 `json:"avg_render_wall_ms"`
			AvgUploadWallMS  float64 `json:"avg_upload_wall_ms"`
			PeakRAMBytes     int64   `json:"peak_ram_bytes"`
		} `json:"levels"`
		Gains []struct {
			FromLevel   int     `json:"from_level"`
			ToLevel     int     `json:"to_level"`
			GainPercent float64 `json:"gain_percent"`
			IsEfficient bool    `json:"is_efficient"`
		} `json:"gains"`
	}

	status, err := client.doJSON(ctx, "POST", "/api/v1/admin/benchmarks/concurrent", reqBody, &result)
	if err != nil {
	fmt.Fprintf(os.Stderr, "fleetctl benchmark: request failed: %v\n", err)
	return ExitUnexpected
}
	if status != http.StatusOK {
	fmt.Fprintf(os.Stderr, "fleetctl benchmark: HTTP %d\n", status)
	return MapHTTPStatusToOpExit(status)
	}

	// Print the summary
	fmt.Println(result.Summary)

	// Also print a compact machine-readable result
	fmt.Printf("\nSweet Spot: %d concurrent jobs\n", result.SweetSpot)
	fmt.Printf("Limiting Factor: %s\n", result.LimitingFactor)

	if len(result.Gains) > 0 {
		fmt.Println("\nMarginal throughput gains:")
		for _, g := range result.Gains {
			icon := "✗"
			if g.IsEfficient {
				icon = "✓"
			}
			fmt.Printf("  %d → %d: %+.1f%% %s\n", g.FromLevel, g.ToLevel, g.GainPercent, icon)
		}
	}

	return ExitOK
}
