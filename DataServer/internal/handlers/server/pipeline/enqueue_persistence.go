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
	"errors"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
	"velox-server/internal/store"
)

// context) is a documented followup.
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
		forwarding, err := h.store.GetCreatorForwardingByTargetJobID(ctx, jobID)
		if err != nil {
			if errors.Is(err, store.ErrCreatorForwardingNoRow) {
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
		status := string(forwarding.Status)
		var startedAt, completedAt string
		if h.jobs.Reader != nil {
			if job, gErr := h.jobs.Reader.Get(ctx, jobID); gErr == nil && job != nil {
				status = string(job.Status)
				if !job.StartedAt.IsZero() {
					startedAt = job.StartedAt.UTC().Format("2006-01-02T15:04:05Z")
				}
				if !job.CompletedAt.IsZero() {
					completedAt = job.CompletedAt.UTC().Format("2006-01-02T15:04:05Z")
				}
			}
		}

		// Enrich with artifact info (best-effort; nil store tolerated).
		var artifactURL string
		var artifactSizeBytes int64
		if h.store != nil {
			artifacts, aErr := h.store.GetArtifactsByJob(jobID, 1)
			if aErr == nil && len(artifacts) > 0 {
				a := artifacts[0]
				if a.StorageURL != "" {
					artifactURL = a.StorageURL
				} else {
					artifactURL = a.LocalPath
				}
				artifactSizeBytes = a.SizeBytes
			}
		}

		// Enrich with task-attempt identity (worker_id, task_id,
		// attempt_id, lease_id). Best-effort — the attempt row may
		// not exist yet when the job is PENDING.
		var workerID, taskID, attemptID, leaseID string
		if h.store != nil {
			if snap, sErr := h.store.GetLatestTaskAttemptForJob(ctx, jobID); sErr == nil && snap != nil {
				workerID = snap.WorkerID
				taskID = snap.TaskID
				attemptID = snap.AttemptID
				leaseID = snap.LeaseID
			}
		}

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
		c.JSON(http.StatusOK, resp)
	}
}
