package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"net/http"

	"velox-server/internal/config"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/workers"
)

// PR15.7a: pipelineEnqueuer (wired by InitPipelineEnqueuer) lives in
// pipeline.go and is the shared Enqueuer used by both the sync forward
// path and this async poll path. Cancellation uses jobs.Reader + jobs.Writer
// directly to iterate queued jobs and delete by ID.

func isTerminalStatus(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	return s == "completed" || s == "succeeded" || s == "done" || s == "failed" || s == "error"
}

// startPipelinePolling polls a pipeline job status every intervalSec seconds
// in a background goroutine until completion, then forwards to workers.
//
// PR15.7a: the forwarding path now
// reads pipelineEnqueuer (wired via InitPipelineEnqueuer at boot) instead.
func startPipelinePolling(client *remoteengine.Client, jobID string, intervalSec int) {
	if client == nil || !client.IsConfigured() {
		pipelineLog("POLL: client not configured — cannot poll job %s", jobID)
		return
	}
	if pipelineEnqueuer == nil {
		pipelineLog("POLL: enqueuer not wired — cannot forward job %s", jobID)
		return
	}

	go func() {
		if intervalSec < 5 {
			intervalSec = 30
		}

		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()

		startTime := time.Now()
		maxPolls := (1800 + intervalSec - 1) / intervalSec
		if maxPolls < 10 {
			maxPolls = 10
		}
		if maxPolls > 120 {
			maxPolls = 120
		}

		pipelineLog("POLL: started goroutine job_id=%s interval=%ds max_polls=%d timeout=%ds",
			jobID, intervalSec, maxPolls, intervalSec*maxPolls)

		for i := 0; i < maxPolls; i++ {
			<-ticker.C
			elapsed := time.Since(startTime).Round(time.Second)

			pipelineLog("POLL: checking job_id=%s attempt=%d/%d elapsed=%s", jobID, i+1, maxPolls, elapsed)
			status, err := client.GetPipelineStatus(context.Background(), jobID)
			if err != nil {
				pipelineLog("POLL: ERROR job_id=%s attempt=%d/%d: %v", jobID, i+1, maxPolls, err)
				continue
			}

			pipelineLog("POLL: STATUS job_id=%s attempt=%d/%d status=%s progress=%.0f%% elapsed=%s",
				jobID, i+1, maxPolls, status.Status, status.Progress, elapsed)

			switch {
			case status.Status == "completed" || status.Status == "succeeded" || status.Status == "done":
				totalElapsed := time.Since(startTime).Round(time.Second)
				pipelineLog("COMPLETE: job_id=%s progress=100%% total_elapsed=%s", jobID, totalElapsed)

				if status.Result != nil {
					if docURL, ok := status.Result["doc_url"].(string); ok {
						pipelineLog("COMPLETE: job_id=%s doc_url=%s", jobID, docURL)
					}
					if jsonPath, ok := status.Result["json_path"].(string); ok {
						pipelineLog("COMPLETE: job_id=%s json_path=%s", jobID, jsonPath)
					}
					if voiceoverURL, ok := status.Result["voiceover_url"].(string); ok {
						pipelineLog("COMPLETE: job_id=%s voiceover_url=%s", jobID, voiceoverURL)
					}
					if scenesCount, ok := status.Result["scenes_count"]; ok {
						pipelineLog("COMPLETE: job_id=%s scenes=%v", jobID, scenesCount)
					}
				}

				pipelineLog("FORWARD: attempting worker handoff job_id=%s", jobID)
				forwardResult := map[string]interface{}{
					"ok":       true,
					"status":   "completed",
					"trace_id": jobID,
				}
				if status.Result != nil {
					forwardResult["result"] = status.Result
					for k, v := range status.Result {
						forwardResult[k] = v
					}
				}

				if enqueue.ShouldForwardPipelineResult(forwardResult) {
					if forwarded, fwdErr := forwardPipelineResultToWorker(context.Background(), pipelineEnqueuer, forwardResult); fwdErr != nil {
						pipelineLog("FORWARD: FAILED job_id=%s: %v", jobID, fwdErr)
					} else {
						workerJobID, _ := forwarded["job_id"].(string)
						pipelineLog("FORWARD: SUCCESS job_id=%s -> worker_job_id=%s total_elapsed=%s",
							jobID, workerJobID, totalElapsed)
					}
				} else {
					missing := []string{}
					if status.Result != nil {
						if _, ok := status.Result["scenes_json"]; !ok {
							if _, ok2 := status.Result["json_path"]; !ok2 {
								if _, ok3 := status.Result["scenes"]; !ok3 {
									missing = append(missing, "scenes_json")
								}
							}
						}
						if _, ok := status.Result["voiceover_path"]; !ok {
							if _, ok2 := status.Result["voiceover_url"]; !ok2 {
								if _, ok3 := status.Result["voiceover"]; !ok3 {
									missing = append(missing, "voiceover")
								}
							}
						}
					} else {
						missing = append(missing, "result (nil)")
					}
					pipelineLog("FORWARD: SKIPPED job_id=%s — missing fields: %s", jobID, strings.Join(missing, ", "))
				}
				return

			case status.Status == "failed" || status.Status == "error":
				pipelineLog("FAILED: job_id=%s progress=%.0f%% elapsed=%s", jobID, status.Progress, time.Since(startTime).Round(time.Second))
				return

			default:
				// Continue polling for running/queued/pending
			}
		}

		pipelineLog("TIMEOUT: job_id=%s exceeded %d polls (%d min)", jobID, maxPolls, intervalSec*maxPolls/60)
	}()
}

// PipelineStatus handles GET /api/remote/pipeline/status/<trace_id>
func PipelineStatus(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")
		pipelineLog("STATUS: requested job_id=%s", traceID)

		if remoteEngineClient == nil || !remoteEngineClient.IsConfigured() {
			pipelineLog("STATUS: remote engine NOT configured")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":       false,
				"trace_id": traceID,
				"error":    "remote engine not configured",
				"hint":     "set VELOX_REMOTE_ENGINE_URL",
			})
			return
		}

		status, err := remoteEngineClient.GetPipelineStatus(c.Request.Context(), traceID)
		if err != nil {
			pipelineLog("STATUS: ERROR job_id=%s: %v", traceID, err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		pipelineLog("STATUS: job_id=%s status=%s progress=%.0f%%", traceID, status.Status, status.Progress)
		c.JSON(http.StatusOK, status)
	}
}

// PipelineCancel handles DELETE /api/remote/pipeline/cancel/<trace_id>
func PipelineCancel(cfg *config.Config, reader jobs.Reader, writer jobs.Writer, cmdMgr *workers.CommandManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")
		pipelineLog("CANCEL: requested job_id=%s", traceID)

		localCancelled := []string{}
		workerCancelled := []string{}
		remoteCancel := false
		remoteErr := ""

		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			if err := remoteEngineClient.CancelPipeline(c.Request.Context(), traceID); err != nil {
				pipelineLog("CANCEL: remote cancel FAILED job_id=%s: %v", traceID, err)
				remoteErr = err.Error()
			} else {
				pipelineLog("CANCEL: remote SUCCESS job_id=%s", traceID)
				remoteCancel = true
			}
		} else {
			pipelineLog("CANCEL: remote engine not configured — skipping remote cancel for job_id=%s", traceID)
		}

		if reader != nil {
			toDelete := []string{traceID}
			workerIDs := map[string]bool{}

			allDomainJobs, _ := reader.List(c.Request.Context(), jobs.Filter{Limit: 10000})
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

			if cmdMgr != nil {
				for workerID := range workerIDs {
					cmdMgr.PushCommand(workerID, "cancel_job", map[string]interface{}{
						"job_id": traceID,
					})
					workerCancelled = append(workerCancelled, workerID)
					pipelineLog("CANCEL: pushed cancel_job to worker %s for job_id=%s", workerID, traceID)
				}
			}

			if writer != nil {
				for _, id := range toDelete {
					if err := writer.Delete(c.Request.Context(), id); err == nil {
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
