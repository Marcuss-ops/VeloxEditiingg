// Package pipeline: HTTP handlers for the remote pipeline-run API.
//
// File: cancel.go
// -----------------------------------------------------------------------------
// PR-DI-pipeline — Step 6 of the pipeline.go split.
//
// What lives here
//   - Cancel() — handler for DELETE /api/remote/pipeline/cancel/:trace_id.
//
// This is a verbatim move of the Cancel method that previously lived in
// pipeline_lifecycle.go. pipeline_lifecycle.go is removed in this same step
// (its entire contents have been redistributed: Status + isTerminalStatus
// already moved to status.go in Step 5, Cancel now moved here in Step 6).
//
// No signature changes, no body changes, no behaviour changes. The only
// intent is to keep each handler's lifecycle stage in its own file so that
// the routes/handlers/generate/error/response files all stay narrowly
// scoped to one responsibility.
// -----------------------------------------------------------------------------
package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// Cancel handles DELETE /api/remote/pipeline/cancel/:trace_id.
//
// PR-DI-pipeline: every dependency is now structural (h.client and
// h.jobs.{Reader,Writer,CmdMgr}). The previous top-level form took
// reader/writer/cmdMgr as bound parameters, but the global
// `remoteEngineClient` is gone: this method reads it from `h`. If the
// caller did not pass cancel-side deps to NewHandlersFull, that side
// of the response is silently skipped (the remote cancel still
// proceeds, which is the operator-correct behaviour).
func (h *Handlers) Cancel() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")
		if traceID == "" {
			traceID = c.Param("id")
		}
		pipelineLog("CANCEL: requested job_id=%s", traceID)

		localCancelled := []string{}
		workerCancelled := []string{}
		remoteCancel := false
		remoteErr := ""

		if h.client != nil && h.client.IsConfigured() {
			if err := h.client.CancelPipeline(c.Request.Context(), traceID); err != nil {
				pipelineLog("CANCEL: remote cancel FAILED job_id=%s: %v", traceID, err)
				remoteErr = err.Error()
			} else {
				pipelineLog("CANCEL: remote SUCCESS job_id=%s", traceID)
				remoteCancel = true
			}
		} else {
			pipelineLog("CANCEL: remote engine not configured — skipping remote cancel for job_id=%s", traceID)
		}

		if h.jobs.Reader != nil {
			toDelete := []string{traceID}
			workerIDs := map[string]bool{}
			workerFences := map[string][]map[string]interface{}{}

			// Resolve ownership from the canonical Task table. The old path
			// attempted to read AssignedTo from the legacy Job queue item,
			// but ownership now lives on tasks.worker_id.
			if h.jobs.TaskReader != nil {
				tasks, err := h.jobs.TaskReader.List(c.Request.Context(), taskgraph.Filter{
					JobIDs: []string{traceID},
					Limit:  10000,
				})
				if err != nil {
					pipelineLog("CANCEL: task ownership lookup failed job_id=%s: %v", traceID, err)
				} else {
					for _, task := range tasks {
						if task.WorkerID == "" || task.Status == taskgraph.StatusSucceeded ||
							task.Status == taskgraph.StatusFailed || task.Status == taskgraph.StatusCancelled {
							continue
						}
						workerIDs[task.WorkerID] = true
						workerFences[task.WorkerID] = append(workerFences[task.WorkerID], map[string]interface{}{
							"task_id": task.ID, "attempt_id": task.AttemptID,
							"lease_id": task.LeaseID, "attempt_number": task.AttemptNumber,
						})
					}
				}
			}

			allDomainJobs, _ := h.jobs.Reader.List(c.Request.Context(), jobs.Filter{Limit: 10000})
			for i := range allDomainJobs {
				j := jobs.ToQueueItem(&allDomainJobs[i])
				if j == nil || j.Payload == nil {
					continue
				}
				if t, ok := j.Payload["trace_id"].(string); ok && t == traceID {
					toDelete = append(toDelete, j.JobID)
					// PR #7: AssignedTo removed from QueueItem — tasks carry
					// worker ownership now. Pipeline cancel path retains
					// worker notification via separate task query.
				}
			}

			if h.jobs.CmdMgr != nil {
				for workerID := range workerIDs {
					payload := map[string]interface{}{"job_id": traceID, "reason": "operator_request"}
					if fences := workerFences[workerID]; len(fences) > 0 {
						payload["task_fences"] = fences
					}
					h.jobs.CmdMgr.PushCommand(workerID, "cancel_job", payload)
					workerCancelled = append(workerCancelled, workerID)
					pipelineLog("CANCEL: pushed cancel_job to worker %s for job_id=%s", workerID, traceID)
				}
			}

			if h.jobs.Writer != nil {
				for _, id := range toDelete {
					// Preserve the canonical Job row so the worker's cancelled
					// TaskResult can be ingested and fenced. Hard-deleting here
					// turns a real remote abort into a late upload/FAILED result.
					job, err := h.jobs.Reader.Get(c.Request.Context(), id)
					if err != nil {
						pipelineLog("CANCEL: failed to load job %s: %v", id, err)
						continue
					}
					if job == nil {
						continue
					}
					if job.Status == jobs.StatusCancelled {
						localCancelled = append(localCancelled, id)
					} else if job.Status != jobs.StatusSucceeded && job.Status != jobs.StatusFailed {
						if err := h.jobs.Writer.Cancel(c.Request.Context(), id, "operator_request", -1); err != nil {
							pipelineLog("CANCEL: failed to mark job %s CANCELLED: %v", id, err)
							continue
						}
						localCancelled = append(localCancelled, id)
					}
				}
			}
		}

		status := "cancelled"
		if len(localCancelled) == 0 && len(workerCancelled) == 0 {
			pipelineLog("CANCEL: no local jobs or workers found for job_id=%s, remote_cancel=%v", traceID, remoteCancel)
			if !remoteCancel {
				status = "not_found"
			}
		} else {
			pipelineLog("CANCEL: SUCCESS job_id=%s cancelled %d local job(s), notified %d worker(s), remote_cancel=%v",
				traceID, len(localCancelled), len(workerCancelled), remoteCancel)
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":               len(localCancelled) > 0 || len(workerCancelled) > 0 || remoteCancel,
			"trace_id":         traceID,
			"status":           status,
			"remote_cancel":    remoteCancel,
			"local_cancelled":  localCancelled,
			"workers_notified": workerCancelled,
			"remote_error":     remoteErr,
		})
	}
}
