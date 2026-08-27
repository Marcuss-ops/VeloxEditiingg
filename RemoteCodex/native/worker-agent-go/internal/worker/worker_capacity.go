package worker

import (
	"math"
	"runtime"

	"velox-worker-agent/pkg/video/pipeline"
)

// countRenderOccupyingTasks is the admission view of activeTasks. Publishing
// and commit-wait continue to be visible as active lifecycle work, but they
// no longer consume a render slot: executeTask releases the render limiter
// before entering publication. Keeping this predicate in one place prevents
// the claim loop from recreating a worker-wide busy gate around publishers.
func countRenderOccupyingTasks(tasks map[string]*ActiveTaskExecution) int {
	count := 0
	for _, task := range tasks {
		if task == nil || task.OperationalPhase == pipeline.PhasePublishing || task.OperationalPhase == pipeline.PhaseCommitWait || task.OperationalPhase == pipeline.PhaseDone {
			continue
		}
		count++
	}
	return count
}

// detectMaxParallelJobs calculates the optimal concurrency based on hardware.
// Formula: clamp(NumCPU / 2, min=1, max=8).
//
// ⚠️ This is a FALLBACK only: if cfg.MaxActiveJobs > 0 (which includes the
// default value 1 from DefaultConfig), worker_init.go uses the configured
// value instead. Operators who want hardware-detected concurrency must
// explicitly set max_active_jobs=0 in their config.
//
// Used at worker init time to size the concurrency limiter; runtime
// capacity is read from w.concurrencyLimiter.MaxActiveJobs() everywhere
// else (single source of truth for max_parallel_jobs).
func detectMaxParallelJobs() int {
	cpuCount := runtime.NumCPU()
	if cpuCount <= 0 {
		cpuCount = 2
	}
	parallel := int(math.Max(1, math.Min(8, float64(cpuCount/2))))
	return parallel
}
