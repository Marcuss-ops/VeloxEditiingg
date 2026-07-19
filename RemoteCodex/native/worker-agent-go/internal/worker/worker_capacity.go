package worker

import (
	"math"
	"runtime"
)

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
