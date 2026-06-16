package queue

import (
	"encoding/json"
	"log"
)

// ────────────────────────────────────────────────────────────────────────────
// Persistence (SQLite)
// ────────────────────────────────────────────────────────────────────────────

func (o *Orchestrator) load() error {
	rows, err := o.dbStore.ListOrchestratorJobs()
	if err != nil {
		return err
	}
	for _, raw := range rows {
		var job MultiStepJob
		data, _ := json.Marshal(raw)
		if err := json.Unmarshal(data, &job); err != nil {
			log.Printf("[ORCH] Warning: skipping malformed job: %v", err)
			continue
		}
		// Only load active jobs (not completed/failed)
		if job.Status == StepCompleted || job.Status == StepFailed {
			continue
		}
		o.jobs[job.JobID] = &job
	}
	if len(o.jobs) > 0 {
		log.Printf("[ORCH] Loaded %d active orchestrator jobs from SQLite", len(o.jobs))
	}
	return nil
}

func (o *Orchestrator) saveAll() {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, job := range o.jobs {
		o.persist(job)
	}
}

func (o *Orchestrator) persist(msj *MultiStepJob) {
	raw, err := json.Marshal(msj)
	if err != nil {
		log.Printf("[ORCH] Failed to marshal job %s: %v", msj.JobID[:8], err)
		return
	}
	started := ""
	if msj.StartedAt != nil {
		started = *msj.StartedAt
	}
	completed := ""
	if msj.CompletedAt != nil {
		completed = *msj.CompletedAt
	}
	if err := o.dbStore.UpsertOrchestratorJob(
		msj.JobID, string(msj.Status), msj.PipelineType,
		msj.TotalSteps, msj.CurrentStep, string(raw),
	); err != nil {
		log.Printf("[ORCH] Failed to persist job %s: %v", msj.JobID[:8], err)
	}
	_ = o.dbStore.SetOrchestratorJobTimestamps(msj.JobID, started, completed)
}
