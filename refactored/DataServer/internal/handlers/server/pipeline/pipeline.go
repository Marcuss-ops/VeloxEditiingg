package pipeline

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/queue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/workers"
	"velox-shared/payload"
)

var remoteEngineClient *remoteengine.Client

// structured log helper
func pipelineLog(format string, args ...interface{}) {
	log.Printf("[PIPELINE] "+format, args...)
}

// InitRemoteEngine initializes the remote engine client
func InitRemoteEngine(cfg *config.Config) {
	if cfg.RemoteEngineURL == "" {
		pipelineLog("INIT: remote engine NOT configured (VELOX_REMOTE_ENGINE_URL empty)")
		return
	}
	remoteEngineClient = remoteengine.NewClient(remoteengine.Config{
		URL:       cfg.RemoteEngineURL,
		Token:     cfg.RemoteEngineToken,
		TimeoutMS: cfg.RemoteEngineTimeoutMS,
		Retries:   cfg.RemoteEngineRetries,
	})
	pipelineLog("INIT: remote engine configured url=%s timeout_ms=%d retries=%d poll_interval=%ds",
		cfg.RemoteEngineURL, cfg.RemoteEngineTimeoutMS, cfg.RemoteEngineRetries, cfg.RemoteEnginePollInterval)
}

// PipelineGenerate handles POST /api/remote/pipeline/generate
func PipelineGenerate(cfg *config.Config, q *queue.FileQueue) gin.HandlerFunc {	return func(c *gin.Context) {
		var reqPayload map[string]interface{}
		if err := c.ShouldBindJSON(&reqPayload); err != nil {
			pipelineLog("REQUEST: invalid JSON from %s: %v", c.ClientIP(), err)
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		topic := payload.FirstString(reqPayload, "topic", "title", "source_text")
		language := payload.FirstString(reqPayload, "language")
		style := payload.FirstString(reqPayload, "style")
		sceneCount := reqPayload["scene_count"]
		pipelineLog("REQUEST: received topic=%q language=%s style=%s scenes=%v", topic, language, style, sceneCount)

		if remoteEngineClient == nil || !remoteEngineClient.IsConfigured() {
			pipelineLog("REQUEST: remote engine NOT configured — returning 503")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "remote engine not configured",
				"hint":  "set VELOX_REMOTE_ENGINE_URL",
			})
			return
		}

		pipelineLog("REMOTE: forwarding to %s/api/script/generate-with-images", cfg.RemoteEngineURL)
		result, err := remoteEngineClient.StartPipeline(c.Request.Context(), reqPayload)
		if err != nil {
			pipelineLog("REMOTE: request FAILED: %v", err)
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
			return
		}

		jobID, _ := result["job_id"].(string)
		status, _ := result["status"].(string)
		if jobID != "" {
			pipelineLog("REMOTE: response job_id=%s status=%s", jobID, status)
		} else {
			pipelineLog("REMOTE: response ok=%v status=%s", result["ok"], status)
		}

		response := gin.H{}
		for k, v := range result {
			response[k] = v
		}

		// Try synchronous forward if result is already complete
		if enqueue.ShouldForwardPipelineResult(result) {
			pipelineLog("FORWARD: result complete — forwarding to Velox workers (sync)")
			if forwarded, forwardErr := forwardPipelineResultToWorker(c.Request.Context(), q, result); forwardErr != nil {
				pipelineLog("FORWARD: FAILED: %v", forwardErr)
				response["worker_forwarded"] = false
				response["worker_forward_error"] = forwardErr.Error()
			} else {
				workerJobID, _ := forwarded["job_id"].(string)
				pipelineLog("FORWARD: SUCCESS job_id=%s", workerJobID)
				response["worker_forwarded"] = true
				response["worker_forward_result"] = forwarded
			}
		} else if jobID != "" && !isTerminalStatus(status) {
			// Async: start background polling
			pollInterval := cfg.RemoteEnginePollInterval
			if pollInterval <= 0 {
				pollInterval = 30
			}
			maxPolls := (1800 + pollInterval - 1) / pollInterval
			if maxPolls > 120 {
				maxPolls = 120
			}
			pipelineLog("POLL: starting background polling job_id=%s status=%s interval=%ds max_polls=%d (~%d min timeout)",
				jobID, status, pollInterval, maxPolls, pollInterval*maxPolls/60)
			startPipelinePolling(remoteEngineClient, q, jobID, pollInterval)
			response["polling_enabled"] = true
			response["poll_interval_sec"] = pollInterval
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete yet — background polling started"
		} else if jobID != "" {
			pipelineLog("FORWARD: result NOT complete for job %s (status=%s) — missing scenes/voiceover", jobID, status)
			response["worker_forwarded"] = false
			response["worker_forward_error"] = "pipeline result is not complete enough for worker handoff — missing scenes/voiceover"
		}

		c.JSON(http.StatusOK, response)
	}
}

func isTerminalStatus(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	return s == "completed" || s == "succeeded" || s == "done" || s == "failed" || s == "error"
}

// startPipelinePolling polls a pipeline job status every intervalSec seconds
// in a background goroutine until completion, then forwards to workers.
func startPipelinePolling(client *remoteengine.Client, q *queue.FileQueue, jobID string, intervalSec int) {
	if client == nil || !client.IsConfigured() {
		pipelineLog("POLL: client not configured — cannot poll job %s", jobID)
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

				// Log result details if available
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
					if forwarded, fwdErr := forwardPipelineResultToWorker(context.Background(), q, forwardResult); fwdErr != nil {
						pipelineLog("FORWARD: FAILED job_id=%s: %v", jobID, fwdErr)
					} else {
						workerJobID, _ := forwarded["job_id"].(string)
						pipelineLog("FORWARD: SUCCESS job_id=%s -> worker_job_id=%s total_elapsed=%s",
							jobID, workerJobID, totalElapsed)
					}
				} else {
					// Log which fields are missing
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

func forwardPipelineResultToWorker(ctx context.Context, q *queue.FileQueue, result map[string]interface{}) (map[string]interface{}, error) {
	pipelineLog("FORWARD: building worker payload...")
	jobPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		pipelineLog("FORWARD: BuildPipelinePayload FAILED: %v", err)
		return nil, err
	}

	payloadJSON, _ := json.Marshal(jobPayload)
	if len(payloadJSON) > 500 {
		pipelineLog("FORWARD: payload built size=%d bytes title=%s scenes=%v",
			len(payloadJSON), jobPayload["title"], jobPayload["scene_count"])
	} else {
		pipelineLog("FORWARD: payload built: %s", string(payloadJSON))
	}

	enqueued, err := enqueue.EnqueueSceneVideoJob(ctx, q, jobPayload)
	if err != nil {
		pipelineLog("FORWARD: EnqueueSceneVideoJob FAILED: %v", err)
		return nil, err
	}
	pipelineLog("FORWARD: enqueued to Velox queue job_id=%v", enqueued["job_id"])
	return enqueued, nil
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
// Cancels a running pipeline job:
// 1. Tries remote engine cancel (script generation)
// 2. Finds and removes local queue jobs
// 3. Pushes cancel_job command to the worker that claimed the job
func PipelineCancel(cfg *config.Config, q *queue.FileQueue, cmdMgr *workers.CommandManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.Param("trace_id")
		pipelineLog("CANCEL: requested job_id=%s", traceID)

		localCancelled := []string{}
		workerCancelled := []string{}
		remoteCancel := false
		remoteErr := ""

		// 1) Try to cancel on the remote engine (script generation on 77.93.152.122)
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

		// 2) Find local jobs, push cancel_job to workers, then delete from queue
		if q != nil {
			// Collect job IDs to delete (avoid modifying map during iteration)
			toDelete := []string{traceID} // exact match first
			workerIDs := map[string]bool{}

			allJobs, _ := q.GetAllJobs(c.Request.Context())
			for _, job := range allJobs {
				if job == nil || job.Payload == nil {
					continue
				}
				if t, ok := job.Payload["trace_id"].(string); ok && t == traceID {
					toDelete = append(toDelete, job.JobID)
					if job.AssignedTo != "" {
						workerIDs[job.AssignedTo] = true
					}
				}
			}

			// Push cancel_job command to workers BEFORE deleting
			if cmdMgr != nil {
				for workerID := range workerIDs {
					cmdMgr.PushCommand(workerID, "cancel_job", map[string]interface{}{
						"job_id": traceID,
					})
					workerCancelled = append(workerCancelled, workerID)
					pipelineLog("CANCEL: pushed cancel_job to worker %s for job_id=%s", workerID, traceID)
				}
			}

			// Delete all collected job IDs from queue
			for _, id := range toDelete {
				if err := q.DeleteJob(c.Request.Context(), id); err == nil {
					localCancelled = append(localCancelled, id)
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

// ScriptSimple handles POST /api/script-simple
func ScriptSimple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.SimpleScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		pipelineLog("SCRIPT_SIMPLE: topic=%q language=%s", req.Topic, req.Language)

		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateSimpleScript(c.Request.Context(), req)
			if err != nil {
				pipelineLog("SCRIPT_SIMPLE: FAILED: %v", err)
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			pipelineLog("SCRIPT_SIMPLE: ok=%v trace_id=%s", result.OK, result.TraceID)
			c.JSON(http.StatusOK, result)
			return
		}

		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}

// ScriptMultiple handles POST /api/script-multiple
func ScriptMultiple(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req remoteengine.BatchScriptRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid JSON"})
			return
		}

		pipelineLog("SCRIPT_MULTIPLE: topics=%d language=%s", len(req.Topics), req.Language)

		if remoteEngineClient != nil && remoteEngineClient.IsConfigured() {
			result, err := remoteEngineClient.GenerateBatchScripts(c.Request.Context(), req)
			if err != nil {
				pipelineLog("SCRIPT_MULTIPLE: FAILED: %v", err)
				c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": err.Error()})
				return
			}
			pipelineLog("SCRIPT_MULTIPLE: ok=%v scripts=%d", result.OK, len(result.Scripts))
			c.JSON(http.StatusOK, result)
			return
		}

		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "remote engine not configured",
			"hint":  "set VELOX_REMOTE_ENGINE_URL",
		})
	}
}


