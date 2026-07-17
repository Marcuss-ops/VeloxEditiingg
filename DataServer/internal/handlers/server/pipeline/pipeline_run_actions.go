package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/pipelineruns"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// lookupPipelineRun is the shared helper that resolves :id into a
// *pipelineruns.PipelineRun. It tries pipeline_runs by primary key,
// then by request_id, then falls back to creator_forwardings (legacy).
// When found via the legacy path, a minimal PipelineRun is synthesised
// so the caller has a consistent struct.
//
// Returns (run, forwarding, nil) where forwarding is non-nil only when
// the row was found via the legacy creator_forwardings path. When
// neither path finds a row, returns (nil, nil, errNotFound).
func (h *Handlers) lookupPipelineRun(ctx context.Context, idParam string) (*pipelineruns.PipelineRun, *store.CreatorForwarding, error) {
	// 1. pipeline_runs by PK
	if pr, err := h.store.GetPipelineRun(ctx, idParam); err == nil && pr != nil {
		return pr, nil, nil
	}
	// 2. pipeline_runs by request_id
	if pr, err := h.store.GetPipelineRunByRequestID(ctx, idParam); err == nil && pr != nil {
		return pr, nil, nil
	}
	// 3-4. Legacy: creator_forwardings
	forwarding, err := h.store.GetCreatorForwarding(ctx, idParam)
	if errors.Is(err, store.ErrCreatorForwardingNoRow) {
		forwarding, err = h.store.GetCreatorForwardingByRemoteJob(ctx, "remote_engine", idParam)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, store.ErrCreatorForwardingNoRow) {
			return nil, nil, errPipelineRunNotFound
		}
		return nil, nil, err
	}
	// Synthesise a minimal PipelineRun from the forwarding row.
	pr := &pipelineruns.PipelineRun{
		ID:             idParam,
		RequestID:      idParam,
		RemoteProvider: forwarding.SourceProvider,
		RemoteJobID:    forwarding.SourceJobID,
		ForwardingID:   forwarding.ForwardingID,
		VeloxJobID:     forwarding.TargetJobID,
		Status:         pipelineruns.Status(forwardingStatus(forwarding)),
	}
	return pr, forwarding, nil
}

// CancelPipelineRun handles POST /api/v1/pipeline-runs/:id/cancel.
//
// Cancels the pipeline run by:
//  1. Cancelling the remote engine job (if a remote_job_id is set).
//  2. Deleting the Velox job (if a velox_job_id is set).
//  3. Notifying workers with cancel_job commands.
//  4. Marking the pipeline_run as CANCELLED.
//
// Idempotent: cancelling an already-terminal run returns 200 with the
// current status instead of an error.
func (h *Handlers) CancelPipelineRun() gin.HandlerFunc {
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
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Already terminal — return idempotent success.
		if pr.Status.Terminal() {
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"status":          string(pr.Status),
				"message":         "pipeline run is already in a terminal state",
			})
			return
		}

		remoteCancelled := false
		remoteErr := ""
		localCancelled := []string{}

		// 1. Cancel remote engine job.
		if pr.RemoteJobID != "" && h.client != nil && h.client.IsConfigured() {
			if err := h.client.CancelPipeline(ctx, pr.RemoteJobID); err != nil {
				pipelineLog("CANCEL: remote cancel FAILED run=%s job=%s: %v", pr.ID, pr.RemoteJobID, err)
				remoteErr = err.Error()
			} else {
				pipelineLog("CANCEL: remote SUCCESS run=%s job=%s", pr.ID, pr.RemoteJobID)
				remoteCancelled = true
			}
		}

		// 2. Cancel Velox job + notify workers.
		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID != "" && h.jobs.Writer != nil {
			if err := h.jobs.Writer.Delete(ctx, veloxJobID); err != nil {
				pipelineLog("CANCEL: local delete FAILED run=%s job=%s: %v", pr.ID, veloxJobID, err)
			} else {
				localCancelled = append(localCancelled, veloxJobID)
				pipelineLog("CANCEL: local SUCCESS run=%s job=%s", pr.ID, veloxJobID)
			}
		}

		// 3. Mark the pipeline_run as CANCELLED (only when found in
		// pipeline_runs table, not the legacy synthesised row).
		if forwarding == nil {
			if err := h.store.UpdatePipelineRunStatus(ctx, pr.ID,
				pipelineruns.StatusCancelled, "cancelled by user"); err != nil {
				pipelineLog("CANCEL: failed to mark CANCELLED run=%s: %v", pr.ID, err)
			}
		} else {
			// Legacy path: mark the creator_forwarding row as CANCELLED
			// so the runner does not pick it up again.
			if err := h.store.MarkCreatorForwardingCancelled(ctx,
				forwarding.ForwardingID, "", "",
				"CANCELLED_BY_USER", "cancelled by user"); err != nil {
				pipelineLog("CANCEL: failed to mark forwarding CANCELLED run=%s fwd=%s: %v",
					pr.ID, forwarding.ForwardingID, err)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"status":          string(pipelineruns.StatusCancelled),
			"remote_cancel":   remoteCancelled,
			"local_cancelled": localCancelled,
			"remote_error":    remoteErr,
		})
	}
}

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
		pr, _, err := h.lookupPipelineRun(ctx, idParam)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
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
		if err := h.store.UpdatePipelineRunStatus(ctx, pr.ID,
			pipelineruns.StatusAccepted, "retry requested"); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		// Clear error fields by stamping empty values via a direct update.
		if err := h.store.ClearPipelineRunError(ctx, pr.ID); err != nil {
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
		if err := h.store.UpdatePipelineRunStatus(ctx, pr.ID,
			pipelineruns.StatusRemoteSubmitting, "retry: submitting to remote engine"); err != nil {
			pipelineLog("RETRY: failed to transition to REMOTE_SUBMITTING run=%s: %v", pr.ID, err)
		}

		result, remoteErr := h.client.StartPipeline(ctx, remotePayload, pr.ID)
		if remoteErr != nil {
			pipelineLog("RETRY: remote call FAILED run=%s: %v", pr.ID, remoteErr)
			if markErr := h.store.UpdatePipelineRunError(ctx, pr.ID,
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
		dto, _ := remoteengine.ParseRemotePipelineResult(result)
		workerPayload := dto.ToWorkerPayload()

		jobID := firstStringResolver(workerPayload, "job_id", "trace_id", "id")
		if jobID != "" {
			pr.RemoteJobID = jobID
			if err := h.store.UpdatePipelineRunRemoteJob(ctx, pr.ID, "remote_engine", jobID); err != nil {
				pipelineLog("RETRY: failed to stamp remote_job_id run=%s: %v", pr.ID, err)
			}
		}		// Persist forwarding for async result, or handle sync forward.
		if jobID == "" {
			// Remote response missing job_id — contract violation.
			pipelineLog("RETRY: remote response missing job_id run=%s", pr.ID)
			if markErr := h.store.UpdatePipelineRunError(ctx, pr.ID,
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
			ctx, "remote_engine", jobID, targetExecutor,
		)
		if persistErr != nil {
			pipelineLog("RETRY: failed to persist forwarding run=%s: %v", pr.ID, persistErr)
			if markErr := h.store.UpdatePipelineRunError(ctx, pr.ID,
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
		if err := h.store.UpdatePipelineRunForwarding(ctx, pr.ID,
			forwarding.ForwardingID, pipelineruns.StatusRemoteQueued); err != nil {
			pipelineLog("RETRY: failed to stamp forwarding_id run=%s: %v", pr.ID, err)
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

// PipelineRunTimeline handles GET /api/v1/pipeline-runs/:id/timeline.
//
// Returns a chronological list of events for the pipeline run. Events
// come from:
//  1. The pipeline_run's own state transitions (created_at, updated_at,
//     completed_at).
//  2. job_events for the Velox job (when velox_job_id is set).
//  3. job_attempts for the Velox job.
//
// Events are sorted by timestamp ascending.
func (h *Handlers) PipelineRunTimeline() gin.HandlerFunc {
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
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		events := []gin.H{}

		// 1. Pipeline run lifecycle events.
		events = append(events, gin.H{
			"timestamp": pr.CreatedAt,
			"stage":     string(pipelineruns.StageRemote),
			"event":     "pipeline_run_created",
			"status":    string(pipelineruns.StatusAccepted),
		})
		if pr.Status != pipelineruns.StatusAccepted {
			events = append(events, gin.H{
				"timestamp": pr.UpdatedAt,
				"stage":     string(pr.Status.StageOf()),
				"event":     "status_changed",
				"status":    string(pr.Status),
			})
		}
		if pr.ErrorCode != "" {
			events = append(events, gin.H{
				"timestamp":   pr.UpdatedAt,
				"stage":       pr.FailedStage,
				"event":       "error",
				"error_code":  pr.ErrorCode,
				"error_message": pr.ErrorMessage,
			})
		}
		if !pr.CompletedAt.IsZero() {
			events = append(events, gin.H{
				"timestamp": pr.CompletedAt,
				"stage":     string(pipelineruns.StageTerminal),
				"event":     "pipeline_run_completed",
				"status":    string(pr.Status),
			})
		}

		// 2. Job events for the Velox job.
		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID != "" {
			jobEvents, _ := h.store.ListJobEvents(veloxJobID, 100)
			for _, e := range jobEvents {
				events = append(events, gin.H{
					"timestamp": e.Timestamp,
					"stage":     string(pipelineruns.StageWorker),
					"event":     e.Event,
					"job_id":    veloxJobID,
					"raw":       e.RawJSON,
				})
			}

			// 3. Job attempts.
			attempts, _ := h.store.GetJobAttempts(veloxJobID, 50)
			for _, a := range attempts {
				events = append(events, gin.H{
					"timestamp":   a.StartedAt,
					"stage":       string(pipelineruns.StageWorker),
					"event":       "job_attempt",
					"job_id":      veloxJobID,
					"attempt":     a.AttemptNumber,
					"worker":      a.WorkerID,
					"status":      a.Status,
					"error":       a.ErrorCode,
				})
			}
		}

		// Sort events by timestamp ascending.
		sort.SliceStable(events, func(i, j int) bool {
			return eventTimestamp(events[i]) < eventTimestamp(events[j])
		})

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"events":          events,
			"count":           len(events),
		})
	}
}

// eventTimestamp extracts a comparable string from a timeline event.
// time.Time values format as RFC3339 (lexically sortable); string
// timestamps from the DB are also RFC3339. Empty strings sort first.
func eventTimestamp(e gin.H) string {
	switch v := e["timestamp"].(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case string:
		return v
	default:
		return ""
	}
}

// PipelineRunArtifacts handles GET /api/v1/pipeline-runs/:id/artifacts.
//
// Returns the artifacts produced by the Velox job associated with the
// pipeline run. When no velox_job_id is set, returns an empty list.
func (h *Handlers) PipelineRunArtifacts() gin.HandlerFunc {
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
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID == "" {
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"artifacts":       []interface{}{},
				"count":           0,
			})
			return
		}

		artifacts, _ := h.store.GetArtifactsByJob(veloxJobID, 50)
		result := make([]gin.H, 0, len(artifacts))
		for _, a := range artifacts {
			result = append(result, gin.H{
				"artifact_id":   a.ID,
				"job_id":        a.JobID,
				"type":          a.Type,
				"status":        a.Status,
				"sha256":        a.SHA256,
				"size_bytes":    a.SizeBytes,
				"storage_url":   a.StorageURL,
				"mime_type":     a.MimeType,
				"verified_at":   a.VerifiedAt,
				"created_at":    a.CreatedAt,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"artifacts":       result,
			"count":           len(result),
		})
	}
}

// PipelineRunDeliveries handles GET /api/v1/pipeline-runs/:id/deliveries.
//
// Returns the delivery rows associated with the Velox job's artifacts.
// When no velox_job_id is set, returns an empty list.
func (h *Handlers) PipelineRunDeliveries() gin.HandlerFunc {
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
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam)
		if err != nil {
			if errors.Is(err, errPipelineRunNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "pipeline run not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		veloxJobID := pr.VeloxJobID
		if veloxJobID == "" && forwarding != nil {
			veloxJobID = forwarding.TargetJobID
		}
		if veloxJobID == "" {
			c.JSON(http.StatusOK, gin.H{
				"ok":              true,
				"pipeline_run_id": pr.ID,
				"deliveries":      []interface{}{},
				"count":           0,
			})
			return
		}

		deliveries, _ := h.store.ListJobDeliveriesByJob(veloxJobID)
		result := make([]gin.H, 0, len(deliveries))
		for _, d := range deliveries {
			item := gin.H{
				"delivery_id":     d.DeliveryID,
				"artifact_id":     d.ArtifactID,
				"destination_id":  d.DestinationID,
				"status":          d.Status,
				"remote_id":       d.RemoteID,
				"remote_url":      d.RemoteURL,
				"created_at":      d.CreatedAt,
				"updated_at":      d.UpdatedAt,
			}
			if d.IdempotencyKey != "" {
				item["idempotency_key"] = d.IdempotencyKey
			}
			result = append(result, item)
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"pipeline_run_id": pr.ID,
			"deliveries":      result,
			"count":           len(result),
		})
	}
}

// errPipelineRunNotFound is the sentinel for lookup misses.
var errPipelineRunNotFound = errors.New("pipeline run not found")
