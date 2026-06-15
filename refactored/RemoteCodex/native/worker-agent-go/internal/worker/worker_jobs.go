// Package worker provides job processing logic for the worker agent.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/api/renderplan"
)

// jobLoop polls for jobs and executes them.
func (w *Worker) jobLoop(ctx context.Context) {
	defer w.wg.Done()

	pollInterval := 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	pollCount := 0
	lastSummaryLog := time.Now()
	summaryInterval := 10 * time.Minute // Log summary every 10 minutes

	w.logger.Info("[POLLING] Worker polling started — checking for jobs every %v", pollInterval)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[POLLING] Job loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Info("[POLLING] Job loop exiting (stop signal)")
			return
		case <-ticker.C:
			if w.Status() != StatusIdle || w.IsStopped() || w.drainMode.Load() {
				continue
			}

			pollCount++

			// Log every poll at DEBUG level for detailed tracing
			w.logger.Debug("[POLLING] Attempt %d — checking master for jobs", pollCount)

			job, err := w.pollJob(ctx)
			if err != nil {
				w.logger.Warn("[POLLING] Attempt %d failed: %v", pollCount, err)
				continue
			}
			if job != nil {
				w.logger.Info("[POLLING] Job acquired on attempt %d — executing", pollCount)
				w.executeJob(ctx, job)
				// Reset poll count after executing a job
				pollCount = 0
			}

			// Periodic summary log at DEBUG level (every ~10 minutes)
			if time.Since(lastSummaryLog) >= summaryInterval {
				w.logger.Debug("[POLLING] Status: alive — %d polls sent, no jobs available",
					pollCount)
				lastSummaryLog = time.Now()
			}
		}
	}
}

// pollJob checks for an available job from the master.
func (w *Worker) pollJob(ctx context.Context) (*api.Job, error) {
	w.logger.Debug("Polling for job...")
	job, err := w.apiClient.GetJob(ctx, w.config.WorkerID)
	if err != nil {
		return nil, err
	}

	if job != nil {
		w.logger.Info("Received job: %s (type: %s, priority: %d)", job.JobID, job.JobType, job.Priority)

		rp := renderplan.FromMap(map[string]interface{}{
			"version":    renderplan.RenderPlanVersion,
			"job_id":     job.JobID,
			"job_type":   job.JobType,
			"created_at": job.CreatedAt,
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
			return nil, fmt.Errorf("job validation failed: %w", err)
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
			return nil, fmt.Errorf("concurrency limit reached for job %s", job.JobID)
		}

		telemetry.RecordJobReceived()
	}

	return job, nil
}
