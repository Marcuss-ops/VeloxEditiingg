package worker

import (
	"time"

	"velox-worker-agent/internal/artifactgraph"
)

// task_lifecycle.go owns the active-task map registration and cleanup and
// the per-attempt intermediate-file profiling summary. The dispatch path in
// task_dispatch.go mutates the maps this file manages while a task is in
// flight.

// registerActiveTask builds the ActiveTaskExecution entry, inserts
// it under activeTasksMu, and returns the pointer. The caller MUST
// call wakeHeartbeat immediately after, then assign
// activeTask.Cancel = jobCancel — preserving the original ordering
// where the heartbeat goroutine sees the new entry BEFORE the cancel
// function is wired up.
func (w *Worker) registerActiveTask(taskID, attemptID string, pte *PendingTaskExecution) *ActiveTaskExecution {
	activeTask := &ActiveTaskExecution{
		TaskID:    taskID,
		AttemptID: attemptID,
		JobID:     pte.JobID,
		Task:      pte,
		LeaseID:   pte.LeaseID,
		StartedAt: time.Now(),
	}
	w.activeTasksMu.Lock()
	w.activeTasks[taskID] = activeTask
	w.taskIDsByJob[pte.JobID] = append(w.taskIDsByJob[pte.JobID], taskID)
	w.activeTasksMu.Unlock()
	return activeTask
}

// unregisterActiveTask is the deferred cleanup that mirrors the
// original closure: deletes the active task from both maps, removes
// the jobID entry when its task list drains to zero, then wakes the
// heartbeat so the next tick reports the updated state.
func (w *Worker) unregisterActiveTask(taskID string, pte *PendingTaskExecution) {
	w.activeTasksMu.Lock()
	delete(w.activeTasks, taskID)
	taskIDs := w.taskIDsByJob[pte.JobID]
	for i, tid := range taskIDs {
		if tid == taskID {
			w.taskIDsByJob[pte.JobID] = append(taskIDs[:i], taskIDs[i+1:]...)
			break
		}
	}
	if len(w.taskIDsByJob[pte.JobID]) == 0 {
		delete(w.taskIDsByJob, pte.JobID)
	}
	w.activeTasksMu.Unlock()
	w.wakeHeartbeat()
}

// logArtifactGraphProfiling emits the per-attempt intermediate-file
// profiling summary (Fase E2). Empty graphs (no records registered by the
// executor) are skipped entirely. Write-then-read candidates — the files a
// later optimization phase should consider eliminating — surface at INFO;
// the full ledger rides at DEBUG. This is the evidence base: nothing is
// removed a priori, candidates are only flagged for a decision.
func (w *Worker) logArtifactGraphProfiling(pte *PendingTaskExecution, g *artifactgraph.Graph) {
	if w == nil || g == nil || pte == nil {
		return
	}
	summary := g.Summary()
	if summary.FileCount == 0 {
		return
	}
	if len(summary.Candidates) > 0 {
		w.logger.Info("[ARTIFACT-GRAPH] attempt=%s files=%d candidates=%d reread_bytes=%d",
			pte.AttemptID, summary.FileCount, len(summary.Candidates), summary.TotalReReadBytes)
		for _, c := range summary.Candidates {
			w.logger.Info("[ARTIFACT-GRAPH]   reread_bytes=%d lifetime=%s producer=%s consumer=%s path=%s",
				c.ReReadBytes, c.Lifetime.Round(time.Millisecond), c.ProducerPhase, c.ConsumerPhase, c.Path)
		}
	}
	w.logger.Debug("[ARTIFACT-GRAPH] attempt=%s files=%d written_bytes=%d read_bytes=%d",
		pte.AttemptID, summary.FileCount, summary.TotalWrittenBytes, summary.TotalReadBytes)
}
