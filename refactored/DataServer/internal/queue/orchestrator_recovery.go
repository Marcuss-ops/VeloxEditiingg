package queue

import (
	"context"
	"fmt"
	"log"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// PR5b — Persistent worker assignments & heartbeat-based recovery
// ────────────────────────────────────────────────────────────────────────────

// syncStep is a pending assignment sync for a single orchestrator step.
type syncStep struct {
	msj        *MultiStepJob
	step       *JobStep
	queueJobID string
}

// syncWorkerAssignments reads FileQueue jobs for each PROCESSING orchestrator
// step and copies the worker assignment (AssignedTo) back into the orchestrator
// step's AssignedWorker/AssignedAt fields. This is a read-only cross-check that
// keeps the orchestrator aware of which worker is handling each step without
// modifying the FileQueue dispatch path.
//
// Survives restarts: orchestrator_jobs.raw_json persists AssignedWorker +
// AssignedAt, so after restart the assignments are immediately visible without
// waiting for the next sync cycle.
//
// PR5b fix: FileQueue I/O happens outside the write lock so other operations
// (SubmitMultiStepJob, GetJob, ReportStepComplete) are not blocked.
func (o *Orchestrator) syncWorkerAssignments(ctx context.Context) {
	// Phase 1: collect steps needing assignment sync under read lock
	o.mu.RLock()
	var pending []syncStep
	for _, msj := range o.jobs {
		for _, step := range msj.Steps {
			if step.Status != StepProcessing || step.AssignedWorker != "" {
				continue
			}
			pending = append(pending, syncStep{
				msj:        msj,
				step:       step,
				queueJobID: fmt.Sprintf("%s-%s", msj.JobID, step.StepID),
			})
		}
	}
	o.mu.RUnlock()

	if len(pending) == 0 {
		return
	}

	// Phase 2: query FileQueue jobs outside any lock
	type update struct {
		msj     *MultiStepJob
		step    *JobStep
		worker  string
	}
	var updates []update
	for _, p := range pending {
		fqJob, err := o.fileQ.GetJob(ctx, p.queueJobID)
		if err != nil {
			log.Printf("[ORCH] syncWorkerAssignments: FileQueue.GetJob(%s) failed: %v", p.queueJobID, err)
			continue
		}
		if fqJob == nil || fqJob.AssignedTo == "" {
			continue
		}
		updates = append(updates, update{
			msj:    p.msj,
			step:   p.step,
			worker: fqJob.AssignedTo,
		})
	}

	if len(updates) == 0 {
		return
	}

	// Phase 3: apply assignments under write lock, track dirty jobs
	now := time.Now().UTC().Format(time.RFC3339)
	dirty := make(map[string]*MultiStepJob)
	o.mu.Lock()
	for _, u := range updates {
		// Re-check status under write lock (may have changed since Phase 1)
		if u.step.Status != StepProcessing || u.step.AssignedWorker != "" {
			continue
		}
		u.step.AssignedWorker = u.worker
		u.step.AssignedAt = &now
		dirty[u.msj.JobID] = u.msj
		log.Printf("[ORCH] Step %s of job %s assigned to worker %s (synced from FileQueue)",
			u.step.StepName, u.msj.JobID[:min(8, len(u.msj.JobID))], u.worker)
	}
	o.mu.Unlock()

	// Phase 4: persist dirty jobs (outside lock, persist is safe for single job)
	for _, msj := range dirty {
		o.mu.Lock()
		o.persist(msj)
		o.mu.Unlock()
	}
}

// recoverStaleWorkerSteps finds steps assigned to workers whose heartbeats
// have timed out and resets those steps to READY for re-dispatch.
//
// PR5b: Uses the worker registry to detect stale workers. When a worker is
// stale, ALL PROCESSING steps assigned to that worker are reset to READY.
// The FileQueue's own RequeueZombieJobs will independently reset the
// underlying FileQueue jobs — this method ensures the orchestrator doesn't
// wait for checkJobTimeout to discover the orphaned step.
func (o *Orchestrator) recoverStaleWorkerSteps(ctx context.Context) {
	if o.workerRegistry == nil {
		return // No registry wired — skip recovery (e.g., in tests)
	}

	staleWorkers := o.workerRegistry.GetStaleWorkers(ctx, o.staleWorkerTimeout)
	if len(staleWorkers) == 0 {
		return
	}

	// Build set of stale worker IDs
	staleSet := make(map[string]struct{}, len(staleWorkers))
	for _, w := range staleWorkers {
		staleSet[w.WorkerID] = struct{}{}
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	totalRecovered := 0
	for _, msj := range o.jobs {
		if msj.Status == StepCompleted || msj.Status == StepFailed {
			continue
		}

		jobRecovered := 0 // reset per job (PR5b fix: no cross-job leak)
		for _, step := range msj.Steps {
			if step.Status != StepProcessing || step.AssignedWorker == "" {
				continue
			}
			if _, stale := staleSet[step.AssignedWorker]; !stale {
				continue
			}

			// Reset the step — it will be re-dispatched on the next poll cycle
			staleWorkerID := step.AssignedWorker
			step.Status = StepReady
			step.AssignedWorker = ""
			step.AssignedAt = nil

			msj.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			jobRecovered++

			log.Printf("[ORCH] Recovered step %s of job %s — worker %s is stale (heartbeat timeout > %v)",
				step.StepName, msj.JobID[:min(8, len(msj.JobID))], staleWorkerID, o.staleWorkerTimeout)
		}

		if jobRecovered > 0 {
			o.persist(msj)
			totalRecovered += jobRecovered
		}
	}

	if totalRecovered > 0 {
		log.Printf("[ORCH] Recovered %d steps from stale workers", totalRecovered)
	}
}
