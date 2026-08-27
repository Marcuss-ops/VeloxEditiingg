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

// TaskPhase classifies a task's executor into a concurrency pool.
type TaskPhase string

const (
	PhaseRender    TaskPhase = "render"
	PhasePrefetch  TaskPhase = "prefetch"
	PhasePublisher TaskPhase = "publisher"
	PhaseUnknown   TaskPhase = ""
)

// classifyExecutor returns the TaskPhase for an executor ID.
func classifyExecutor(executorID string) TaskPhase {
	switch {
	case executorID == "render_batch" || len(executorID) > 12 && executorID[:12] == "render_batch@":
		return PhaseRender
	case executorID == "asset_prefetch" || len(executorID) > 14 && executorID[:14] == "asset_prefetch@":
		return PhasePrefetch
	case executorID == "artifact_publish" || len(executorID) > 16 && executorID[:16] == "artifact_publish@":
		return PhasePublisher
	default:
		return PhaseRender // conservative fallback: unknown executors consume render slots
	}
}

// canAcceptPhase checks whether the worker can accept a new task in the
// given phase. Uses per-phase slot limits when configured; falls back to
// the flat maxActiveJobs limit when per-phase slots are zero.
func (w *Worker) canAcceptPhase(phase TaskPhase) bool {
	switch phase {
	case PhaseRender:
		if w.config.RenderSlots > 0 {
			return int(w.activeRender.Load()) < w.config.RenderSlots
		}
	case PhasePrefetch:
		if w.config.PrefetchSlots > 0 {
			return int(w.activePrefetch.Load()) < w.config.PrefetchSlots
		}
	case PhasePublisher:
		if w.config.PublisherSlots > 0 {
			return int(w.activePublisher.Load()) < w.config.PublisherSlots
		}
	}
	// Fallback: flat limit
	return int(w.activeRender.Load()+w.activePrefetch.Load()+w.activePublisher.Load()) < w.config.MaxActiveJobs
}

// incrementPhase increments the per-phase active counter.
func (w *Worker) incrementPhase(phase TaskPhase) {
	switch phase {
	case PhaseRender:
		w.activeRender.Add(1)
	case PhasePrefetch:
		w.activePrefetch.Add(1)
	case PhasePublisher:
		w.activePublisher.Add(1)
	default:
		w.activeRender.Add(1)
	}
}

// decrementPhase decrements the per-phase active counter.
func (w *Worker) decrementPhase(phase TaskPhase) {
	switch phase {
	case PhaseRender:
		w.activeRender.Add(-1)
	case PhasePrefetch:
		w.activePrefetch.Add(-1)
	case PhasePublisher:
		w.activePublisher.Add(-1)
	default:
		w.activeRender.Add(-1)
	}
}
