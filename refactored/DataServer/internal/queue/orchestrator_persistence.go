package queue

import (
	"context"
	"encoding/json"
	"log"

	"velox-server/internal/store"
)

// ────────────────────────────────────────────────────────────────────────────
// Persistence (SQLite-authoritative, transactional outbox)
// ────────────────────────────────────────────────────────────────────────────

// load reads active orchestrator jobs from SQLite into the cache.
// Only loads non-terminal jobs (not COMPLETED or FAILED).
func (o *Orchestrator) load() error {
	rows, err := o.dbStore.ListOrchestratorJobs()
	if err != nil {
		return err
	}
	count := 0
	for _, raw := range rows {
		var job MultiStepJob
		data, _ := json.Marshal(raw)
		if err := json.Unmarshal(data, &job); err != nil {
			log.Printf("[ORCH] Warning: skipping malformed job: %v", err)
			continue
		}
		if job.Status == StepCompleted || job.Status == StepFailed {
			continue
		}
		o.jobs[job.JobID] = &job
		count++
	}
	if count > 0 {
		log.Printf("[ORCH] Loaded %d active orchestrator jobs from SQLite", count)
	}
	return nil
}

// persist writes a job to SQLite with optional outbox entries in a single transaction.
// PR5: SQLite is authoritative. Cache is updated after successful write.
func (o *Orchestrator) persist(msj *MultiStepJob, outboxEntries ...store.OutboxEntry) {
	raw := marshalJob(msj)
	started := ""
	if msj.StartedAt != nil {
		started = *msj.StartedAt
	}
	completed := ""
	if msj.CompletedAt != nil {
		completed = *msj.CompletedAt
	}

	// Write job + outbox in same transaction (PR5: no lost events, timestamps atomic with state)
	if err := o.dbStore.UpsertOrchestratorJobWithOutbox(
		msj.JobID, string(msj.Status), msj.PipelineType,
		msj.TotalSteps, msj.CurrentStep, raw,
		started, completed,
		outboxEntries,
	); err != nil {
		log.Printf("[ORCH] Failed to persist job %s: %v", msj.JobID[:min(8, len(msj.JobID))], err)
		return
	}

	// Signal poll loop to check outbox immediately
	o.notify()
}

// notify wakes the poll loop (non-blocking).
func (o *Orchestrator) notify() {
	select {
	case o.notifyCh <- struct{}{}:
	default:
	}
}

// submitOutboxEntry writes a single outbox entry directly (non-transactional,
// used for external events like ReportStepComplete). Does NOT touch the job.
func (o *Orchestrator) submitOutboxEntry(eventType, jobID, stepID string, payload map[string]interface{}) {
	payloadJSON := "{}"
	if payload != nil {
		if raw, err := json.Marshal(payload); err == nil {
			payloadJSON = string(raw)
		}
	}

	if err := o.dbStore.InsertOutboxEntry(eventType, jobID, stepID, payloadJSON); err != nil {
		log.Printf("[ORCH] Failed to write outbox entry (%s): %v", eventType, err)
		return
	}
	o.notify()
}

// pollOutbox reads and processes unprocessed outbox entries.
// PR5: replaces channels — events are never lost.
// Recovers from panics in individual entry processing to keep the loop alive.
func (o *Orchestrator) pollOutbox(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ORCH] PANIC in outbox processing: %v", r)
		}
	}()
	entries, err := o.dbStore.PollOrchestratorOutbox(o.outboxBatchSize)
	if err != nil {
		log.Printf("[ORCH] Outbox poll error: %v", err)
		return
	}
	for _, entry := range entries {
		id, _ := entry["id"].(int)
		if id == 0 {
			continue
		}
		o.processOutboxEntry(ctx, entry)
		_ = o.dbStore.MarkOutboxProcessed(id)
	}
}

// processOutboxEntry handles a single outbox entry by dispatching to the
// appropriate handler based on event_type.
func (o *Orchestrator) processOutboxEntry(ctx context.Context, entry map[string]any) {
	eventType, _ := entry["event_type"].(string)
	jobID, _ := entry["job_id"].(string)
	stepID, _ := entry["step_id"].(string)

	// Resolve job from cache or SQLite
	msj := o.resolveJob(jobID)
	if msj == nil {
		return
	}

	switch eventType {
	case "step_dispatch":
		o.handleDispatch(ctx, msj, stepID)
	case "step_complete":
		o.handleComplete(ctx, msj, stepID, entry["payload"])
	case "step_failed":
		o.handleStepFailed(ctx, msj, stepID, entry["payload"])
	case "job_complete":
		o.completeJob(msj)
	case "job_fail":
		o.failJob(msj, "orchestrator outbox")
	case "step_ready":
		o.advanceJob(ctx, msj)
	}
}

// resolveJob returns a job from cache, or loads from SQLite on cache miss.
// PR5: SQLite is authoritative; cache is read-through.
// Returns nil and logs a warning if the job is not found (shouldn't happen for active outbox entries).
func (o *Orchestrator) resolveJob(jobID string) *MultiStepJob {
	o.mu.RLock()
	msj, ok := o.jobs[jobID]
	o.mu.RUnlock()
	if ok {
		return msj
	}

	// Cache miss — load from SQLite (authoritative), then cache under write lock
	o.mu.Lock()
	// Double-check: another goroutine may have loaded it while we were waiting
	if msj, ok = o.jobs[jobID]; ok {
		o.mu.Unlock()
		return msj
	}

	raw, err := o.dbStore.GetOrchestratorJob(jobID)
	if err != nil || raw == "" {
		o.mu.Unlock()
		log.Printf("[ORCH] resolveJob: job %s not found in SQLite", jobID[:min(8, len(jobID))])
		return nil
	}
	var job MultiStepJob
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		o.mu.Unlock()
		log.Printf("[ORCH] resolveJob: failed to unmarshal job %s: %v", jobID[:min(8, len(jobID))], err)
		return nil
	}
	o.jobs[jobID] = &job
	o.mu.Unlock()
	return &job
}

// saveAll persists all cached jobs (called on shutdown).
func (o *Orchestrator) saveAll() {
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, job := range o.jobs {
		o.persist(job) // no outbox entries needed on shutdown
	}
}

// findStep locates a step by ID within a job.
func findStep(job *MultiStepJob, stepID string) *JobStep {
	for _, s := range job.Steps {
		if s.StepID == stepID {
			return s
		}
	}
	return nil
}

// Stats returns diagnostic statistics about the orchestrator.
func (o *Orchestrator) Stats() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	total := len(o.jobs)
	pending, running, completed, failed := 0, 0, 0, 0
	for _, j := range o.jobs {
		switch j.Status {
		case StepPending, StepReady:
			pending++
		case StepProcessing:
			running++
		case StepCompleted:
			completed++
		case StepFailed:
			failed++
		}
	}

	return map[string]interface{}{
		"total_jobs":     total,
		"pending_jobs":   pending,
		"running_jobs":   running,
		"completed_jobs": completed,
		"failed_jobs":    failed,
	}
}
