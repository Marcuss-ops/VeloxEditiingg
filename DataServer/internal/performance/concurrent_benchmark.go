package performance

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkFixtureID identifies a canonical benchmark fixture.
type BenchmarkFixtureID string

// CacheMode controls cold vs warm cache behavior.
type CacheMode string

const (
	CacheModeCold CacheMode = "cold_cache"
	CacheModeWarm CacheMode = "warm_cache"
)

// BenchmarkFixture describes a benchmark fixture to render.
type BenchmarkFixture struct {
	ID BenchmarkFixtureID
}

// BenchmarkRenderReceipt captures the resource metrics from one render.
type BenchmarkRenderReceipt struct {
	PeakRAMBytes   int64  `json:"peak_ram_bytes"`
	RenderWallMS   int64  `json:"render_wall_ms"`
	UploadWallMS   int64  `json:"upload_wall_ms"`
	ArtifactSHA256 string `json:"artifact_sha256"`
	WallMS         int64  `json:"wall_ms"`
}

// BenchmarkRenderResult is the outcome of one render.
type BenchmarkRenderResult struct {
	Receipt        *BenchmarkRenderReceipt `json:"receipt,omitempty"`
	ArtifactSHA256 string                  `json:"artifact_sha256,omitempty"`
}

// RenderRunner executes ONE benchmark render of a fixture.
type RenderRunner interface {
	Render(ctx context.Context, fixture BenchmarkFixture) (BenchmarkRenderResult, error)
}

// ConcurrentBenchmarkConfig configures the concurrent benchmark run.
type ConcurrentBenchmarkConfig struct {
	// FixtureID is the canonical benchmark fixture to run.
	FixtureID BenchmarkFixtureID `json:"fixture_id"`

	// MaxConcurrency is the maximum concurrent job level to test (default: 4).
	MaxConcurrency int `json:"max_concurrency"`

	// RunsPerLevel is the number of runs at each concurrency level (default: 3).
	RunsPerLevel int `json:"runs_per_level"`

	// CooldownBetweenLevels is the pause between concurrency levels (default: 10s).
	CooldownBetweenLevels time.Duration `json:"cooldown_between_levels"`

	// CacheMode controls cold vs warm cache behavior.
	CacheMode CacheMode `json:"cache_mode"`
}

// ConcurrencyLevelResult holds the aggregated results for one concurrency level.
type ConcurrencyLevelResult struct {
	Level            int     `json:"level"`
	TotalRuns        int     `json:"total_runs"`
	SuccessfulRuns   int     `json:"successful_runs"`
	FailedRuns       int     `json:"failed_runs"`
	TotalWallMS      int64   `json:"total_wall_ms"`
	AvgWallMS        float64 `json:"avg_wall_ms"`
	P50WallMS        float64 `json:"p50_wall_ms"`
	P95WallMS        float64 `json:"p95_wall_ms"`
	Throughput       float64 `json:"throughput"` // jobs per second
	PeakRAMBytes     int64   `json:"peak_ram_bytes"`
	AvgRAMBytes      int64   `json:"avg_ram_bytes"`
	RenderWallMS     float64 `json:"avg_render_wall_ms"`
	UploadWallMS     float64 `json:"avg_upload_wall_ms"`
	RenderPerJobMS   float64 `json:"render_per_job_ms"`
	UploadPerJobMS   float64 `json:"upload_per_job_ms"`
	RenderJobsActive int32   `json:"avg_render_jobs_active"`
	PrefetchActive   int32   `json:"avg_prefetch_jobs_active"`
	PublisherActive  int32   `json:"avg_publisher_jobs_active"`
}

// ThroughputGain measures the marginal throughput gain from adding one more
// concurrent job level.
type ThroughputGain struct {
	FromLevel    int     `json:"from_level"`
	ToLevel      int     `json:"to_level"`
	GainPercent  float64 `json:"gain_percent"`
	AbsoluteGain float64 `json:"absolute_gain_jobs_per_sec"`
	IsEfficient  bool    `json:"is_efficient"` // true if gain > 10%
}

// ConcurrentBenchmarkResult is the complete result of a concurrent benchmark run.
type ConcurrentBenchmarkResult struct {
	BenchmarkRunID string                    `json:"benchmark_run_id"`
	FixtureID      BenchmarkFixtureID        `json:"fixture_id"`
	WorkerID       string                    `json:"worker_id"`
	Config         ConcurrentBenchmarkConfig `json:"config"`
	Levels         []ConcurrencyLevelResult  `json:"levels"`
	Gains          []ThroughputGain          `json:"gains"`
	SweetSpot      int                       `json:"sweet_spot"` // recommended concurrency
	LimitingFactor string                    `json:"limiting_factor"`
	Summary        string                    `json:"summary"`
	StartedAt      time.Time                 `json:"started_at"`
	CompletedAt    time.Time                 `json:"completed_at"`
	Duration       time.Duration             `json:"duration"`
}

// RunConcurrentBenchmark executes the benchmark harness: runs the fixture at
// concurrency levels 1 through MaxConcurrency, collects resource metrics and
// throughput, and determines the sweet spot.
//
// The render function is called for each individual render. It receives a
// context, the fixture, and should return wall time and resource observations.
// This keeps the harness decoupled from the actual render pipeline.
func RunConcurrentBenchmark(
	ctx context.Context,
	renderer RenderRunner,
	fixture BenchmarkFixture,
	config ConcurrentBenchmarkConfig,
	workerID string,
) (*ConcurrentBenchmarkResult, error) {
	if renderer == nil {
		return nil, errors.New("benchmark renderer is not configured")
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 4
	}
	if config.RunsPerLevel <= 0 {
		config.RunsPerLevel = 3
	}
	if config.CooldownBetweenLevels <= 0 {
		config.CooldownBetweenLevels = 10 * time.Second
	}

	result := &ConcurrentBenchmarkResult{
		BenchmarkRunID: generateRunID(),
		FixtureID:      fixture.ID,
		WorkerID:       workerID,
		Config:         config,
		StartedAt:      time.Now().UTC(),
	}

	for level := 1; level <= config.MaxConcurrency; level++ {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		levelResult := runConcurrencyLevel(ctx, renderer, fixture, level, config.RunsPerLevel)
		result.Levels = append(result.Levels, *levelResult)

		// Cooldown between levels (skip after last level)
		if level < config.MaxConcurrency {
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(config.CooldownBetweenLevels):
			}
		}
	}

	// Compute throughput gains
	result.Gains = computeThroughputGains(result.Levels)

	// Determine sweet spot
	result.SweetSpot, result.LimitingFactor = determineSweetSpot(result.Levels, result.Gains)

	// Generate summary
	result.Summary = generateBenchmarkSummary(result)

	result.CompletedAt = time.Now().UTC()
	result.Duration = result.CompletedAt.Sub(result.StartedAt)

	return result, nil
}

// runConcurrencyLevel runs the fixture `runs` times concurrently at the given
// level and aggregates the results.
func runConcurrencyLevel(
	ctx context.Context,
	renderer RenderRunner,
	fixture BenchmarkFixture,
	level, runs int,
) *ConcurrencyLevelResult {
	result := &ConcurrencyLevelResult{
		Level:     level,
		TotalRuns: runs,
	}

	var (
		wallMSValues []int64
		peakRAM      int64
		totalRAM     int64
		ramSamples   int64
		totalRender  float64
		totalUpload  float64
		renderCount  int
		uploadCount  int
		mu           sync.Mutex
		wg           sync.WaitGroup
		sem          = make(chan struct{}, level)
		successCount atomic.Int64
		failCount    atomic.Int64
	)

	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(runIdx int) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			start := time.Now()
			result, err := renderer.Render(ctx, fixture)
			elapsed := time.Since(start).Milliseconds()

			if err != nil {
				failCount.Add(1)
				return
			}

			successCount.Add(1)

			mu.Lock()
			wallMSValues = append(wallMSValues, elapsed)

			if result.Receipt != nil {
				// RAM metrics
				if result.Receipt.PeakRAMBytes > peakRAM {
					peakRAM = result.Receipt.PeakRAMBytes
				}
				totalRAM += result.Receipt.PeakRAMBytes
				ramSamples++

				// Render wall time
				if result.Receipt.RenderWallMS > 0 {
					totalRender += float64(result.Receipt.RenderWallMS)
					renderCount++
				}

				// Upload wall time
				if result.Receipt.UploadWallMS > 0 {
					totalUpload += float64(result.Receipt.UploadWallMS)
					uploadCount++
				}
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	result.SuccessfulRuns = int(successCount.Load())
	result.FailedRuns = int(failCount.Load())
	result.PeakRAMBytes = peakRAM

	if ramSamples > 0 {
		result.AvgRAMBytes = totalRAM / ramSamples
	}

	// Wall time stats
	if len(wallMSValues) > 0 {
		sort.Slice(wallMSValues, func(i, j int) bool { return wallMSValues[i] < wallMSValues[j] })

		var total int64
		for _, v := range wallMSValues {
			total += v
		}
		result.TotalWallMS = total
		result.AvgWallMS = float64(total) / float64(len(wallMSValues))
		result.P50WallMS = percentile(wallMSValues, 50)
		result.P95WallMS = percentile(wallMSValues, 95)

		// Throughput = successful runs / total wall time (in seconds)
		if result.TotalWallMS > 0 {
			result.Throughput = float64(result.SuccessfulRuns) / (float64(result.TotalWallMS) / 1000.0)
		}
	}

	// Per-job averages
	if renderCount > 0 {
		result.RenderWallMS = totalRender / float64(renderCount)
		result.RenderPerJobMS = result.RenderWallMS
	}
	if uploadCount > 0 {
		result.UploadWallMS = totalUpload / float64(uploadCount)
		result.UploadPerJobMS = result.UploadWallMS
	}

	return result
}

// computeThroughputGains calculates the marginal throughput gain from adding
// one more concurrent job level.
func computeThroughputGains(levels []ConcurrencyLevelResult) []ThroughputGain {
	gains := make([]ThroughputGain, 0, len(levels)-1)
	for i := 1; i < len(levels); i++ {
		prev := levels[i-1]
		curr := levels[i]

		gain := ThroughputGain{
			FromLevel: prev.Level,
			ToLevel:   curr.Level,
		}

		if prev.Throughput > 0 {
			gain.AbsoluteGain = curr.Throughput - prev.Throughput
			gain.GainPercent = (gain.AbsoluteGain / prev.Throughput) * 100
		}

		// A gain is "efficient" if it's more than 10% improvement
		gain.IsEfficient = gain.GainPercent > 10

		gains = append(gains, gain)
	}
	return gains
}

// determineSweetSpot finds the optimal concurrency level based on throughput
// gains. The sweet spot is the last level where the gain is still efficient
// (>10% improvement).
func determineSweetSpot(levels []ConcurrencyLevelResult, gains []ThroughputGain) (int, string) {
	if len(levels) == 0 {
		return 1, "INSUFFICIENT_DATA"
	}

	// Find the last efficient gain
	sweetSpot := 1
	limitingFactor := "BASELINE"

	for _, gain := range gains {
		if gain.IsEfficient {
			sweetSpot = gain.ToLevel
		}
	}

	// Determine the limiting factor at the sweet spot
	if sweetSpot < len(levels) {
		// The level AFTER the sweet spot has diminishing returns
		// Check what resource is most constrained
		if sweetSpot < len(levels) {
			nextLevel := levels[sweetSpot] // 0-indexed, sweetSpot is 1-indexed
			if nextLevel.AvgRAMBytes > 0 {
				// Check RAM pressure
				ramPressure := float64(nextLevel.PeakRAMBytes) / float64(nextLevel.AvgRAMBytes+1)
				if ramPressure > 1.5 {
					limitingFactor = "RAM"
				}
			}
		}
	}

	// If no efficient gains found, check if the first level is already optimal
	if sweetSpot == 1 && len(gains) > 0 && gains[0].GainPercent < 5 {
		limitingFactor = "ALL_DIMINISHING"
	}

	return sweetSpot, limitingFactor
}

// generateBenchmarkSummary creates a human-readable summary of the benchmark.
func generateBenchmarkSummary(result *ConcurrentBenchmarkResult) string {
	if len(result.Levels) == 0 {
		return "No benchmark data collected"
	}

	summary := fmt.Sprintf("Concurrent Benchmark Results for worker %s\n", result.WorkerID)
	summary += fmt.Sprintf("Fixture: %s | Runs per level: %d\n", result.Config.FixtureID, result.Config.RunsPerLevel)
	summary += "\nConcurrency | Throughput | Avg Render | Avg Upload | Peak RAM\n"
	summary += "------------|------------|------------|------------|----------\n"

	for _, level := range result.Levels {
		summary += fmt.Sprintf("     %d      |  %.2f/s   |  %.0f ms   |  %.0f ms   | %s\n",
			level.Level,
			level.Throughput,
			level.RenderPerJobMS,
			level.UploadPerJobMS,
			formatBytes(level.PeakRAMBytes),
		)
	}

	summary += "\nThroughput Gains:\n"
	for _, gain := range result.Gains {
		efficiency := "✓"
		if !gain.IsEfficient {
			efficiency = "✗"
		}
		summary += fmt.Sprintf("  %d → %d: %+.1f%% (%s)\n",
			gain.FromLevel, gain.ToLevel, gain.GainPercent, efficiency)
	}

	summary += fmt.Sprintf("\nSweet Spot: %d concurrent jobs (limiting: %s)\n", result.SweetSpot, result.LimitingFactor)

	return summary
}

// generateRunID creates a unique run ID for the benchmark.
func generateRunID() string {
	return fmt.Sprintf("bench-%d", time.Now().UnixNano())
}

// percentile computes the nearest-rank percentile from a sorted slice.
func percentile(sorted []int64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(float64(p) / 100.0 * float64(len(sorted))))
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return float64(sorted[rank-1])
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
