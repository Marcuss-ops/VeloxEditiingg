package worker

import (
	"math"
	"runtime"

	"velox-worker-agent/pkg/video/pipeline"
)

// CapacitySnapshot is the point-in-time diagnostic view used when a task is
// refused by admission. The fields intentionally include both phase gates
// and the legacy flat gate: until they are replaced by one resolver, a
// capacity_full log must make disagreements between them observable.
type CapacitySnapshot struct {
	Phase                TaskPhase
	ActiveRender         int32
	RenderSlots          int
	ActivePrefetch       int32
	PrefetchSlots        int
	ActivePublisher      int32
	PublisherSlots       int
	ActiveTasks          int
	RenderOccupyingTasks int
	PendingTasks         int
	MaxActiveJobs        int
	LimiterActive        int32
	LimiterMax           int
	PhaseAvailable       bool
	FlatAvailable        bool
}

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
	return w.capacitySnapshot(phase).PhaseAvailable
}

// capacitySnapshot captures all currently competing admission authorities.
// It is diagnostic rather than a reservation: values can change immediately
// after the snapshot is taken, as they can for any non-blocking admission
// check.
func (w *Worker) capacitySnapshot(phase TaskPhase) CapacitySnapshot {
	s := CapacitySnapshot{
		Phase:           phase,
		ActiveRender:    w.activeRender.Load(),
		RenderSlots:     w.config.RenderSlots,
		ActivePrefetch:  w.activePrefetch.Load(),
		PrefetchSlots:   w.config.PrefetchSlots,
		ActivePublisher: w.activePublisher.Load(),
		PublisherSlots:  w.config.PublisherSlots,
		MaxActiveJobs:   w.config.MaxActiveJobs,
	}

	w.activeTasksMu.RLock()
	s.ActiveTasks = len(w.activeTasks)
	s.RenderOccupyingTasks = countRenderOccupyingTasks(w.activeTasks)
	w.activeTasksMu.RUnlock()
	w.pendingTasksMu.Lock()
	s.PendingTasks = len(w.pendingTasks)
	w.pendingTasksMu.Unlock()

	if w.concurrencyLimiter != nil {
		stats := w.concurrencyLimiter.Stats()
		s.LimiterActive = stats.ActiveJobs
		s.LimiterMax = stats.MaxActiveJobs
	}

	switch phase {
	case PhaseRender:
		if s.RenderSlots > 0 {
			s.PhaseAvailable = s.ActiveRender < int32(s.RenderSlots)
		} else {
			s.PhaseAvailable = s.ActiveRender+s.ActivePrefetch+s.ActivePublisher < int32(s.MaxActiveJobs)
		}
	case PhasePrefetch:
		if s.PrefetchSlots > 0 {
			s.PhaseAvailable = s.ActivePrefetch < int32(s.PrefetchSlots)
		} else {
			s.PhaseAvailable = s.ActiveRender+s.ActivePrefetch+s.ActivePublisher < int32(s.MaxActiveJobs)
		}
	case PhasePublisher:
		if s.PublisherSlots > 0 {
			s.PhaseAvailable = s.ActivePublisher < int32(s.PublisherSlots)
		} else {
			s.PhaseAvailable = s.ActiveRender+s.ActivePrefetch+s.ActivePublisher < int32(s.MaxActiveJobs)
		}
	default:
		s.PhaseAvailable = s.ActiveRender+s.ActivePrefetch+s.ActivePublisher < int32(s.MaxActiveJobs)
	}
	s.FlatAvailable = s.RenderOccupyingTasks+s.PendingTasks < s.MaxActiveJobs
	return s
}

func (w *Worker) logCapacityFull(s CapacitySnapshot, gate string) {
	w.logger.Info("[ADMISSION] capacity_full gate=%s phase=%s active_render=%d/render_slots=%d active_prefetch=%d/prefetch_slots=%d active_publisher=%d/publisher_slots=%d active_tasks=%d render_occupying_tasks=%d pending_tasks=%d max_active_jobs=%d limiter_active=%d/limiter_max=%d phase_available=%t flat_available=%t",
		gate, s.Phase,
		s.ActiveRender, s.RenderSlots,
		s.ActivePrefetch, s.PrefetchSlots,
		s.ActivePublisher, s.PublisherSlots,
		s.ActiveTasks, s.RenderOccupyingTasks, s.PendingTasks,
		s.MaxActiveJobs, s.LimiterActive, s.LimiterMax,
		s.PhaseAvailable, s.FlatAvailable)
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
