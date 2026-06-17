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

// syncWorkerAssignments reads FileQueue jobs for each PROCESSING orchestrator
// step and copies the worker assignment (AssignedTo) back into the orchestrator
// step's AssignedWorker/AssignedAt fields. This is a read-only cross-check that
// keeps the orchestrator aware of which worker is handling each step without
// modifying the FileQueue dispatch path.
//
// Survives restarts: orchestrator_jobs.raw_json persists AssignedWorker +
// AssignedAt, so after restart the assignments are immediately visible without
// waiting for the next sync cycle.
func (o *Orchestrator) syncWorkerAssignments(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	changed := false

	for _, msj := range o.jobs {
		for _, step := range msj.Steps {
			if step.Status != StepProcessing {
				continue
			}
			// Already assigned — skip
			if step.AssignedWorker != "" {
				continue
			}

			// Build the FileQueue job ID for this step
			queueJobID := fmt.Sprintf("%s-%s", msj.JobID, step.StepID)

			fqJob, err := o.fileQ.GetJob(ctx, queueJobID)
			if err != nil || fqJob == nil {
				continue
			}

			// Only sync if the FileQueue job has been claimed by a worker
			if fqJob.AssignedTo == "" {
				continue
			}

			step.AssignedWorker = fqJob.AssignedTo
			step.AssignedAt = &now
			changed = true

			log.Printf("[ORCH] Step %s of job %s assigned to worker %s (synced from FileQueue)",
				step.StepName, msj.JobID[:min(8, len(msj.JobID))], fqJob.AssignedTo)
		}
	}

	if changed {
		// Persist updated assignments to SQLite (survives restarts)
		o.persistDirty()
	}
}

// persistDirty persists all dirty jobs (called after syncWorkerAssignments).
func (o *Orchestrator) persistDirty() {
	for _, msj := range o.jobs {
		o.persist(msj)
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

	recovered := 0
	for _, msj := range o.jobs {
		if msj.Status == StepCompleted || msj.Status == StepFailed {
			continue
		}
		for _, step := range msj.Steps {
			if step.Status != StepProcessing || step.AssignedWorker == "" {
				continue
			}
			if _, stale := staleSet[step.AssignedWorker]; !stale {
				continue
			}

			// Reset the step — it will be re-dispatched on the next poll cycle
			step.Status = StepReady
			step.AssignedWorker = ""
			step.AssignedAt = nil

			msj.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			recovered++

			log.Printf("[ORCH] Recovered step %s of job %s — worker %s is stale (heartbeat timeout > %v)",
				step.StepName, msj.JobID[:min(8, len(msj.JobID))], step.AssignedWorker, o.staleWorkerTimeout)
		}

		if recovered > 0 {
			o.persist(msj)
		}
	}

	if recovered > 0 {
		log.Printf("[ORCH] Recovered %d steps from stale workers", recovered)
	}
}
