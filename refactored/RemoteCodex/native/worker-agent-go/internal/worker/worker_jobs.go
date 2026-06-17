// Package worker provides job processing logic for the worker agent.
// Job polling is now handled by the ControlTransport; the receiveLoop in
// worker.go routes JobOffer messages to executeJob. This file retains
// job validation helpers used by the receive loop and job executor.
package worker

import (
	"fmt"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/api/renderplan"
)

// validateJobOffer validates a job received via transport JobOffer message.
// Checks contract version, render plan validity, and concurrency capacity.
func (w *Worker) validateJobOffer(job *api.Job) error {
	if job == nil {
		return fmt.Errorf("nil job")
	}

	if job.ContractVersion != 0 && job.ContractVersion != api.ContractVersionV2 {
		w.logger.Error("[RENDERPLAN] Job contract version mismatch: got=%d want=%d", job.ContractVersion, api.ContractVersionV2)
		telemetry.GetPrometheusMetrics().RecordIdempotencyConflict("contract_version")
		return fmt.Errorf("unsupported contract version: %d", job.ContractVersion)
	}

	rp := renderplan.FromMap(map[string]interface{}{
		"version":    renderplan.RenderPlanVersion,
		"job_id":     job.JobID,
		"job_type":   job.JobType,
		"created_at": resolveJobCreatedAt(job),
		"priority":   job.Priority,
		"parameters": job.Parameters,
	})

	if err := renderplan.ValidateRenderPlan(rp); err != nil {
		w.logger.Error("[RENDERPLAN] Job validation failed: %v", err)
		if planErrs, ok := err.(renderplan.PlanErrors); ok {
			for _, planErr := range planErrs {
				w.logger.Error("[RENDERPLAN] error_code=%s field=%s message=%s", planErr.Code, planErr.Field, planErr.Message)
			}
		}
		telemetry.GetPrometheusMetrics().RecordIdempotencyConflict("validation_failed")
		return fmt.Errorf("job validation failed: %w", err)
	}

	rp.SetDefaults()
	if rp.Priority != job.Priority {
		w.logger.Debug("[RENDERPLAN] Applied default priority: %d -> %d", job.Priority, rp.Priority)
		job.Priority = rp.Priority
	}

	w.logger.Info("[RENDERPLAN] Job %s validated: render_plan_version=%s", job.JobID, rp.Version)

	if !w.concurrencyLimiter.CanAcceptJob(job.Priority) {
		w.logger.Warn("[CONCURRENCY] Cannot accept job %s: concurrency limit reached", job.JobID)
		telemetry.GetPrometheusMetrics().RecordIdempotencyConflict("concurrency_limit")
		return fmt.Errorf("concurrency limit reached for job %s", job.JobID)
	}

	telemetry.RecordJobReceived()
	return nil
}
