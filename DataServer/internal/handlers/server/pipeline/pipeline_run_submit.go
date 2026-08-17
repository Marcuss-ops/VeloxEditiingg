// Package pipeline / pipeline_run_submit.go — pipeline run submit phase.
// Extracted from pipeline_run_actions.go: the FAILED-run retry flow that
// re-issues the remote engine call with the original payload.
package pipeline

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"velox-server/internal/creatorflow"
	"velox-server/internal/pipelineruns"
	"velox-server/internal/remoteengine"

	"github.com/gin-gonic/gin"
)

// RetryPipelineRun handles POST /api/v1/pipeline-runs/:id/retry.
//
// Retries a FAILED pipeline run by:
//  1. Resetting the pipeline_run status to ACCEPTED.
//  2. Clearing error fields.
//  3. Re-issuing the remote call with the original requested payload.
//
// Only runs in a FAILED state can be retried. Non-failed runs return 409.
func (h *Handlers) RetryPipelineRun() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "pipeline store not wired"})
			return
		}
		idParam := strings.TrimSpace(c.Param("id"))
		if idParam == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "id is required"})
			return
		}

		ctx := c.Request.Context()
		clientID := strings.TrimSpace(ClientIDFromContext(c))
		pr, _, err := h.lookupPipelineRun(ctx, idParam, clientID)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				if clientID != "" {
					writeM2MJobNotFound(c)
				} else {
					c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				}
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		markRunError := func(code, message, failedStage string) error {
			if clientID != "" {
				return h.store.UpdatePipelineRunErrorForClient(ctx, pr.ID, clientID, code, message, failedStage)
			}
			return h.store.UpdatePipelineRunError(ctx, pr.ID, code, message, failedStage)
		}

		// Only FAILED runs can be retried.
		if pr.Status != pipelineruns.StatusFailed {
			c.JSON(http.StatusConflict, gin.H{
				"ok":     false,
				"error":  "only FAILED pipeline runs can be retried",
				"status": string(pr.Status),
			})
			return
		}

		// Only runs in the pipeline_runs table (not legacy synthesised)
		// can be retried — we need the original requested_payload_json.
		if pr.RequestedPayloadJSON == "" {
			c.JSON(http.StatusConflict, gin.H{
				"ok":    false,
				"error": "cannot retry: original request payload not available (legacy run)",
			})
			return
		}

		// Reset the run to ACCEPTED + clear errors.
		var updateErr error
		if clientID != "" {
			updateErr = h.store.UpdatePipelineRunStatusForClient(ctx, pr.ID, clientID,
				pipelineruns.StatusAccepted, "retry requested")
		} else {
			updateErr = h.store.UpdatePipelineRunStatus(ctx, pr.ID,
				pipelineruns.StatusAccepted, "retry requested")
		}
		if updateErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": updateErr.Error()})
			return
		}
		// Clear error fields by stamping empty values via a direct update.
		if clientID != "" {
			if err := h.store.ClearPipelineRunErrorForClient(ctx, pr.ID, clientID); err != nil {
				pipelineLog("RETRY: failed to clear error fields run=%s: %v", pr.ID, err)
			}
		} else if err := h.store.ClearPipelineRunError(ctx, pr.ID); err != nil {
			pipelineLog("RETRY: failed to clear error fields run=%s: %v", pr.ID, err)
		}

		pipelineLog("RETRY: reset run=%s to ACCEPTED, re-issuing remote call", pr.ID)

		// Check if the remote engine is configured.
		if h.client == nil || !h.client.IsConfigured() {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"status":          string(pipelineruns.StatusAccepted),
				"error":           "remote engine not configured — run reset but cannot re-submit",
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// Re-dispatch the remote call with the original payload.
		var remotePayload map[string]interface{}
		if err := json.Unmarshal([]byte(pr.RequestedPayloadJSON), &remotePayload); err != nil {
			pipelineLog("RETRY: failed to unmarshal original payload run=%s: %v", pr.ID, err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"error":           "failed to parse original request payload",
			})
			return
		}

		// Transition to REMOTE_SUBMITTING.
		if clientID != "" {
			if err := h.store.UpdatePipelineRunStatusForClient(ctx, pr.ID, clientID,
				pipelineruns.StatusRemoteSubmitting, "retry: submitting to remote engine"); err != nil {
				pipelineLog("RETRY: failed to transition to REMOTE_SUBMITTING run=%s: %v", pr.ID, err)
			}
		} else if err := h.store.UpdatePipelineRunStatus(ctx, pr.ID,
			pipelineruns.StatusRemoteSubmitting, "retry: submitting to remote engine"); err != nil {
			pipelineLog("RETRY: failed to transition to REMOTE_SUBMITTING run=%s: %v", pr.ID, err)
		}

		result, remoteErr := h.client.StartPipeline(ctx, remotePayload, pr.ID)
		if remoteErr != nil {
			pipelineLog("RETRY: remote call FAILED run=%s: %v", pr.ID, remoteErr)
			if markErr := markRunError(
				"RETRY_REMOTE_FAILED", remoteErr.Error(), "REMOTE_SUBMITTING"); markErr != nil {
				pipelineLog("RETRY: failed to mark error run=%s: %v", pr.ID, markErr)
			}
			c.JSON(http.StatusBadGateway, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           remoteErr.Error(),
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// Area 2: Parse the raw result into the typed DTO and derive
		// the worker payload. The remote result must NOT be passed
		// raw to the worker.
		dto, parseErr := remoteengine.ParseRemotePipelineResult(result)
		if parseErr != nil {
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": parseErr.Error()})
			return
		}
		workerPayload, projectionErr := dto.ToWorkerPayloadChecked()
		if projectionErr != nil {
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": projectionErr.Error()})
			return
		}

		jobID := firstStringResolver(workerPayload, "job_id", "trace_id", "id")
		if jobID != "" {
			pr.RemoteJobID = jobID
			var stampErr error
			if clientID != "" {
				stampErr = h.store.UpdatePipelineRunRemoteJobForClient(ctx, pr.ID, clientID, "remote_engine", jobID)
			} else {
				stampErr = h.store.UpdatePipelineRunRemoteJob(ctx, pr.ID, "remote_engine", jobID)
			}
			if stampErr != nil {
				pipelineLog("RETRY: failed to stamp remote_job_id run=%s: %v", pr.ID, stampErr)
			}
		}

		// Async forward always (the legacy sync-forward on early completion branch was removed
		// when /api/remote/pipeline was deleted from main; RetryPipelineRun now matches
		// generate.go’s always-async forwarding behavior).
		// Persist forwarding for async result.
		if jobID == "" {
			// Remote response missing job_id — contract violation.
			pipelineLog("RETRY: remote response missing job_id run=%s", pr.ID)
			if markErr := markRunError(
				"RETRY_REMOTE_CONTRACT", "remote response missing job_id", "REMOTE_SUBMITTING"); markErr != nil {
				pipelineLog("RETRY: failed to mark error run=%s: %v", pr.ID, markErr)
			}
			c.JSON(http.StatusBadGateway, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           "remote response missing job_id",
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		// jobID is set — persist forwarding if resolver is available.
		if h.resolver == nil || !h.resolver.HasDBAccess() {
			pipelineLog("RETRY: durable resolver unavailable run=%s job=%s", pr.ID, jobID)
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"status":          string(pipelineruns.StatusRemoteSubmitting),
				"error":           "durable forwarding is not configured",
				"remote_job_id":   jobID,
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		targetExecutor := firstStringResolver(workerPayload, "executor_id", "pipeline_id")
		forwarding, persistErr := h.resolver.PersistPendingRemoteForwarding(
			ctx, "remote_engine", jobID, targetExecutor, ClientIDFromContext(c), creatorflow.IntakeSourcePipelineRun,
		)
		if persistErr != nil {
			pipelineLog("RETRY: failed to persist forwarding run=%s: %v", pr.ID, persistErr)
			if markErr := markRunError(
				"RETRY_FORWARDING_FAILED", persistErr.Error(), "FORWARDING"); markErr != nil {
				pipelineLog("RETRY: failed to mark error run=%s: %v", pr.ID, markErr)
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":              false,
				"pipeline_run_id": pr.ID,
				"status":          string(pipelineruns.StatusFailed),
				"error":           persistErr.Error(),
				"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
			})
			return
		}

		pr.ForwardingID = forwarding.ForwardingID
		var forwardingStampErr error
		if clientID != "" {
			forwardingStampErr = h.store.UpdatePipelineRunForwardingForClient(ctx, pr.ID, clientID,
				forwarding.ForwardingID, pipelineruns.StatusRemoteQueued)
		} else {
			forwardingStampErr = h.store.UpdatePipelineRunForwarding(ctx, pr.ID,
				forwarding.ForwardingID, pipelineruns.StatusRemoteQueued)
		}
		if forwardingStampErr != nil {
			pipelineLog("RETRY: failed to stamp forwarding_id run=%s: %v", pr.ID, forwardingStampErr)
		}

		// Update the run with the result JSON for audit.
		if resultJSON, mErr := json.Marshal(result); mErr == nil {
			var resultErr error
			if clientID != "" {
				resultErr = h.store.UpdatePipelineRunResultForClient(ctx, pr.ID, clientID, string(resultJSON))
			} else {
				resultErr = h.store.UpdatePipelineRunResult(ctx, pr.ID, string(resultJSON))
			}
			if resultErr != nil {
				pipelineLog("RETRY: failed to stamp result_json run=%s: %v", pr.ID, resultErr)
			}
		}

		c.JSON(http.StatusAccepted, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"status":          string(pipelineruns.StatusRemoteQueued),
			"remote_job_id":   jobID,
			"forwarding_id":   pr.ForwardingID,
			"message":         "pipeline run retried — remote job submitted",
			"status_url":      "/api/v1/pipeline-runs/" + pr.ID,
		})
	}
}
