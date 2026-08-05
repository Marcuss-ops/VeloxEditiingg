package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/pipelineruns"
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
func (h *Handlers) lookupPipelineRun(ctx context.Context, idParam, externalClientID string) (*pipelineruns.PipelineRun, *store.CreatorForwarding, error) {
	clientID := strings.TrimSpace(externalClientID)

	// A pipeline_runs row has no client column of its own. For an M2M
	// request, ownership is therefore established through its durable
	// forwarding_id. A missing link or a client mismatch is deliberately
	// indistinguishable from a missing run.
	ownershipCheck := func(pr *pipelineruns.PipelineRun) (*store.CreatorForwarding, error) {
		if clientID == "" {
			return nil, nil
		}
		if pr.ForwardingID == "" {
			return nil, errPipelineRunNotFound
		}
		forwarding, err := h.store.GetCreatorForwardingByIDForClient(ctx, pr.ForwardingID, clientID)
		if err != nil {
			if errors.Is(err, store.ErrCreatorForwardingNoRow) {
				return nil, errPipelineRunNotFound
			}
			return nil, err
		}
		return forwarding, nil
	}

	// 1. pipeline_runs by PK
	if pr, err := h.store.GetPipelineRun(ctx, idParam); err == nil && pr != nil {
		if _, ownershipErr := ownershipCheck(pr); ownershipErr != nil {
			return nil, nil, ownershipErr
		}
		return pr, nil, nil
	}
	// 2. pipeline_runs by request_id
	if pr, err := h.store.GetPipelineRunByRequestID(ctx, idParam); err == nil && pr != nil {
		if _, ownershipErr := ownershipCheck(pr); ownershipErr != nil {
			return nil, nil, ownershipErr
		}
		return pr, nil, nil
	}
	// 3-4. Legacy: creator_forwardings. The M2M path uses only the
	// ownership-scoped repository methods; admin callers retain the
	// existing unscoped legacy lookup.
	var forwarding *store.CreatorForwarding
	var err error
	if clientID != "" {
		forwarding, err = h.store.GetCreatorForwardingByIDForClient(ctx, idParam, clientID)
		if errors.Is(err, store.ErrCreatorForwardingNoRow) {
			forwarding, err = h.store.GetCreatorForwardingByRemoteJobForClient(ctx, "remote_engine", idParam, clientID)
		}
	} else {
		forwarding, err = h.store.GetCreatorForwarding(ctx, idParam)
		if errors.Is(err, store.ErrCreatorForwardingNoRow) {
			forwarding, err = h.store.GetCreatorForwardingByRemoteJob(ctx, "remote_engine", idParam)
		}
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
		pr, forwarding, err := h.lookupPipelineRun(ctx, idParam, ClientIDFromContext(c))
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

// errPipelineRunNotFound is the sentinel for lookup misses.
var errPipelineRunNotFound = errors.New("pipeline run not found")
