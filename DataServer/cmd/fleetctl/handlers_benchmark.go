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
			Level           int     `json:"level"`
			SuccessfulRuns  int     `json:"successful_runs"`
			FailedRuns      int     `json:"failed_runs"`
			Throughput      float64 `json:"throughput"`
			AvgRenderWallMS float64 `json:"avg_render_wall_ms"`
			AvgUploadWallMS float64 `json:"avg_upload_wall_ms"`
			PeakRAMBytes    int64   `json:"peak_ram_bytes"`
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

// runBenchmarkCollect triggers a benchmark via the /validate endpoint,
// which runs the benchmark, validates the scorecard prediction, persists
// the result, and returns threshold tuning suggestions.
func runBenchmarkCollect(client *fleetClient, args []string) int {
	fs := flag.NewFlagSet("benchmark-collect", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fixtureID := fs.String("fixture", "", "benchmark fixture ID (required)")
	maxConcurrency := fs.Int("max-concurrency", 4, "maximum concurrency level to test (1-8)")
	runsPerLevel := fs.Int("runs", 3, "number of runs per concurrency level (1-10)")
	cacheMode := fs.String("cache-mode", "cold_cache", "cache mode: cold_cache or warm_cache")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl benchmark-collect: %v\n", err)
		return ExitMisuse
	}

	workerID := fs.Arg(0)
	if workerID == "" {
		fmt.Fprintln(os.Stderr, "fleetctl benchmark-collect: worker_id is required")
		return ExitMisuse
	}
	if *fixtureID == "" {
		fmt.Fprintln(os.Stderr, "fleetctl benchmark-collect: --fixture is required")
		return ExitMisuse
	}

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

	type validateRequest struct {
		FixtureID      string `json:"fixture_id"`
		WorkerID       string `json:"worker_id"`
		MaxConcurrency int    `json:"max_concurrency"`
		RunsPerLevel   int    `json:"runs_per_level"`
		CacheMode      string `json:"cache_mode"`
	}

	reqBody := validateRequest{
		FixtureID:      *fixtureID,
		WorkerID:       workerID,
		MaxConcurrency: *maxConcurrency,
		RunsPerLevel:   *runsPerLevel,
		CacheMode:      *cacheMode,
	}

	fmt.Fprintf(os.Stderr, "fleetctl benchmark-collect: running benchmark + validation on worker %s\n", workerID)
	fmt.Fprintf(os.Stderr, "  fixture=%s  max-concurrency=%d  runs-per-level=%d  cache=%s\n",
		*fixtureID, *maxConcurrency, *runsPerLevel, *cacheMode)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	var result struct {
		BenchmarkRunID string `json:"benchmark_run_id"`
		SweetSpot      int    `json:"sweet_spot"`
		LimitingFactor string `json:"limiting_factor"`
		Summary        string `json:"summary"`
		StartedAt      string `json:"started_at"`
		CompletedAt    string `json:"completed_at"`
		Levels         []struct {
			Level           int     `json:"level"`
			SuccessfulRuns  int     `json:"successful_runs"`
			FailedRuns      int     `json:"failed_runs"`
			Throughput      float64 `json:"throughput"`
			AvgRenderWallMS float64 `json:"avg_render_wall_ms"`
			AvgUploadWallMS float64 `json:"avg_upload_wall_ms"`
			PeakRAMBytes    int64   `json:"peak_ram_bytes"`
		} `json:"levels"`
		Gains []struct {
			FromLevel   int     `json:"from_level"`
			ToLevel     int     `json:"to_level"`
			GainPercent float64 `json:"gain_percent"`
			IsEfficient bool    `json:"is_efficient"`
		} `json:"gains"`
		Validation struct {
			PredictedSweetSpot     int     `json:"predicted_sweet_spot"`
			ObservedSweetSpot      int     `json:"observed_sweet_spot"`
			Accuracy               string  `json:"accuracy"`
			SuggestedRAMSafety     float64 `json:"suggested_ram_safety"`
			SuggestedDiskSafety    float64 `json:"suggested_disk_safety"`
			SuggestedNetworkSafety float64 `json:"suggested_network_safety"`
			Rationale              string  `json:"rationale"`
		} `json:"validation"`
	}

	status, err := client.doLongJSON(ctx, "POST", "/api/v1/admin/benchmarks/validate", reqBody, &result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleetctl benchmark-collect: request failed: %v\n", err)
		return ExitUnexpected
	}
	if status != http.StatusOK {
		fmt.Fprintf(os.Stderr, "fleetctl benchmark-collect: HTTP %d\n", status)
		return MapHTTPStatusToOpExit(status)
	}

	// Print summary
	fmt.Println(result.Summary)

	// Print sweet spot
	fmt.Printf("\nSweet Spot: %d concurrent jobs\n", result.SweetSpot)
	fmt.Printf("Limiting Factor: %s\n", result.LimitingFactor)

	// Print throughput curve
	if len(result.Gains) > 0 {
		fmt.Println("\nThroughput gains:")
		for _, g := range result.Gains {
			icon := "✗"
			if g.IsEfficient {
				icon = "✓"
			}
			fmt.Printf("  %d → %d: %+.1f%% %s\n", g.FromLevel, g.ToLevel, g.GainPercent, icon)
		}
	}

	// Print scorecard validation
	fmt.Println("\n--- Scorecard Validation ---")
	fmt.Printf("  Predicted sweet spot: %d\n", result.Validation.PredictedSweetSpot)
	fmt.Printf("  Observed sweet spot:  %d\n", result.Validation.ObservedSweetSpot)
	fmt.Printf("  Accuracy:             %s\n", result.Validation.Accuracy)
	fmt.Printf("  Rationale:            %s\n", result.Validation.Rationale)

	// Print threshold tuning suggestions
	fmt.Println("\n--- Threshold Tuning Suggestions ---")
	fmt.Printf("  RAM safety:     %.0f%% (default: 75%%)\n", result.Validation.SuggestedRAMSafety*100)
	fmt.Printf("  Disk safety:    %.0f%% (default: 75%%)\n", result.Validation.SuggestedDiskSafety*100)
	fmt.Printf("  Network safety: %.0f%% (default: 80%%)\n", result.Validation.SuggestedNetworkSafety*100)

	// Machine-readable output
	fmt.Printf("\nJSON: benchmark_run_id=%s validated=true accuracy=%s\n",
		result.BenchmarkRunID, result.Validation.Accuracy)

	return ExitOK
}
