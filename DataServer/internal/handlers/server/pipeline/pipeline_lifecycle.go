package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/jobs"
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
					h.jobs.CmdMgr.PushCommand(workerID, "cancel_job", map[string]interface{}{
						"job_id": traceID,
					})
					workerCancelled = append(workerCancelled, workerID)
					pipelineLog("CANCEL: pushed cancel_job to worker %s for job_id=%s", workerID, traceID)
				}
			}

			if h.jobs.Writer != nil {
				for _, id := range toDelete {
					if err := h.jobs.Writer.Delete(c.Request.Context(), id); err == nil {
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
