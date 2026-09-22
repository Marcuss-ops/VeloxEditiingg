// Package pipeline — enqueue_persistence.go owns:
//   - GetSubmittedJob: GET /api/v1/jobs/{job_id} polling
//     entrypoint that the resolver forwards to once a job is
//     accepted. Prefers jobs.Status; falls back to
//     forwarding.Status when jobs.Reader hasn't materialized
//     yet (pre-FORWARDING race). A miss → 404 job_not_found;
//     a store error → 500 store_failure.
//
// SINGLE-WRITER INVARIANT: this file MUST NOT introduce NEW
// INSERTs into jobs / tasks / task_specs. Persistence writes
// flow through h.resolveCompletedPayload → enqueue → store
// (see scripts/ci/check-single-writer.sh). GetSubmittedJob is
// read-only.
package pipeline

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/forwardingstore"
	"velox-server/internal/store"
)

// The endpoint is M2M-only and fails closed: a missing middleware client
// identity is indistinguishable from an unknown job.
//
// Edge cases:
//   - Unknown :id → 404 job_not_found.
//   - Pre-FORWARDED target_job_id not populated → primary lookup
//     misses → 404 (callers should retry with exponential backoff).
func (h *Handlers) GetSubmittedJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "job_id_required",
				"message": "URL path /api/v1/jobs/:id requires non-empty :id",
			})
			return
		}
		ctx := c.Request.Context()
		clientID := strings.TrimSpace(ClientIDFromContext(c))
		if clientID == "" {
			// /api/v1/jobs/:id is M2M-only. A missing middleware identity
			// must fail closed rather than silently reverting to an unscoped
			// lookup.
			c.JSON(http.StatusNotFound, gin.H{
				"ok":      false,
				"error":   "job_not_found",
				"message": "job_id does not match any known creator forwarding",
			})
			return
		}
		forwarding, err := h.store.Forwarding().GetCreatorForwardingByTargetJobID(ctx, jobID, clientID)
		if err != nil {
			if errors.Is(err, forwardingstore.ErrCreatorForwardingNoRow) {
				c.JSON(http.StatusNotFound, gin.H{
					"ok":      false,
					"error":   "job_not_found",
					"message": "job_id does not match any known creator forwarding",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"error":   "store_failure",
				"message": err.Error(),
			})
			return
		}
		// Prefer jobs.Status (canonical render state). Fall back to
		// forwarding.Status when the jobs row has not materialized
		// (resolver race in pre-FORWARDING). Tripping the fallback
		// produces PENDING / POLLING / etc. — these can be more
		// granular than the public jobs.Status enum, but the 4-field
		// contract here passes them through verbatim; clients
		// implementing strict status matching should consult the
		// openapi.yaml schema enum, not the raw forwarding status.
		// Enrichment is strictly client-scoped: clientID is guaranteed
		// non-empty here (the handler fails closed above), so every read
		// goes through the ownership-scoped repository surface. There is
		// deliberately no unscoped fallback path.
		status := string(forwarding.Status)
		var startedAt, completedAt string
		if h.store != nil {
			job, gErr := h.store.GetJobForClient(ctx, jobID, clientID)
			if gErr == nil && job != nil {
				if s, ok := job["status"].(string); ok {
					status = s
				}
				if s, ok := job["started_at"].(string); ok {
					startedAt = s
				}
				if s, ok := job["completed_at"].(string); ok {
					completedAt = s
				}
			}
		}

		// Enrich with artifact info (best-effort; nil store tolerated).
		var artifactURL string
		var artifactSizeBytes int64
		if h.store != nil {
			artifacts, aErr := h.store.GetArtifactsByJobForClient(ctx, jobID, clientID, 50)
			if aErr == nil {
				if a := selectPrimaryReadyArtifact(artifacts); a != nil {
					if h.cfg != nil {
						base := strings.TrimRight(string(h.cfg.ControlPlane.RESTPublic), "/")
						if base != "" {
							artifactURL = base + "/api/internal/artifacts/" + a.ID + "/download"
						}
					}
					if artifactURL == "" {
						artifactURL = a.StorageURL
					}
					artifactSizeBytes = a.SizeBytes
				}
			}
		}

		// Enrich with task-attempt identity (worker_id, task_id,
		// attempt_id, lease_id). Best-effort — the attempt row may
		// not exist yet when the job is PENDING.
		var workerID, taskID, attemptID, leaseID string
		if h.store != nil {
			snap, sErr := h.store.GetLatestTaskAttemptForJobForClient(ctx, jobID, clientID)
			if sErr == nil && snap != nil {
				workerID = snap.WorkerID
				taskID = snap.TaskID
				attemptID = snap.AttemptID
				leaseID = snap.LeaseID
			}
		}

		prefetchDispatchStatus, futureAssetPlan := h.prefetchReadModel(ctx, jobID, clientID)

		created := forwarding.SourceProvider == ExternalAPISourceProvider
		statusURL := "/api/v1/jobs/" + jobID

		resp := gin.H{
			"ok":         true,
			"job_id":     jobID,
			"status":     status,
			"created":    created,
			"status_url": statusURL,
		}
		if startedAt != "" {
			resp["started_at"] = startedAt
		}
		if completedAt != "" {
			resp["completed_at"] = completedAt
		}
		if artifactURL != "" {
			resp["artifact_url"] = artifactURL
			resp["artifact_size_bytes"] = artifactSizeBytes
		}
		if workerID != "" {
			resp["worker_id"] = workerID
		}
		if taskID != "" {
			resp["task_id"] = taskID
		}
		if attemptID != "" {
			resp["attempt_id"] = attemptID
		}
		if leaseID != "" {
			resp["lease_id"] = leaseID
		}
		if prefetchDispatchStatus != "" {
			resp["dispatch_status"] = prefetchDispatchStatus
		}
		if futureAssetPlan != "" {
			resp["future_asset_plan"] = futureAssetPlan
		}
		c.JSON(http.StatusOK, resp)
	}
}

// prefetchReadModel projects the latest durable prefetch lifecycle events into
// the small polling response. The event journal remains the detailed source of
// truth; this projection deliberately exposes only stable state labels and no
// asset/job identifiers beyond the already-authorized job URL.
func (h *Handlers) prefetchReadModel(ctx context.Context, jobID, clientID string) (dispatchStatus, futureAssetPlan string) {
	if h == nil || h.store == nil || jobID == "" || clientID == "" {
		return "", ""
	}
	events, err := h.store.ListJobEventsForClient(ctx, jobID, clientID, 100)
	if err != nil {
		return "", ""
	}
	return prefetchStateFromEvents(events)
}

func prefetchStateFromEvents(events []store.JobEvent) (dispatchStatus, futureAssetPlan string) {
	for _, event := range events {
		switch event.Event {
		case "prefetch.prefetch_prepared":
			if dispatchStatus == "" {
				dispatchStatus = "prefetch_ready"
			}
			if futureAssetPlan == "" {
				futureAssetPlan = "prepared"
			}
		case "prefetch.future_plan_applied":
			if dispatchStatus == "" {
				dispatchStatus = "prefetch_preparing"
			}
			if futureAssetPlan == "" {
				futureAssetPlan = "applied"
			}
		case "prefetch.future_plan_received":
			if dispatchStatus == "" {
				dispatchStatus = "prefetch_planning"
			}
			if futureAssetPlan == "" {
				futureAssetPlan = "received"
			}
		case "prefetch.future_plan_sent":
			if dispatchStatus == "" {
				dispatchStatus = "prefetch_queued"
			}
			if futureAssetPlan == "" {
				futureAssetPlan = "sent"
			}
		case "prefetch.prejob_prepare_failed", "prefetch.prefetch_failed", "prefetch.prefetch_error":
			if dispatchStatus == "" {
				dispatchStatus = "prefetch_failed"
			}
			if futureAssetPlan == "" {
				futureAssetPlan = "failed"
			}
		}
		if dispatchStatus != "" && futureAssetPlan != "" {
			return dispatchStatus, futureAssetPlan
		}
	}
	return dispatchStatus, futureAssetPlan
}
