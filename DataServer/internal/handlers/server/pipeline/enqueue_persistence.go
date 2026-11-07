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
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/forwardingstore"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
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
		// M2M polling has a strict sub-second budget. Bound all DB projections
		// under one shared deadline so a busy SQLite reader cannot consume the
		// caller's entire 45-second polling timeout.
		ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
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
		var createdAt, completedAt, jobErrorMessage string
		if h.store != nil {
			job, gErr := h.store.GetJobForClient(ctx, jobID, clientID)
			if gErr == nil && job != nil {
				if s, ok := job["status"].(string); ok {
					status = s
				}
				if s, ok := job["created_at"].(string); ok {
					createdAt = s
				}
				if s, ok := job["completed_at"].(string); ok {
					completedAt = s
				}
				if s, ok := job["error_message"].(string); ok {
					jobErrorMessage = s
				}
			}
		}

		// Enrich with artifact info (best-effort; nil store tolerated).
		var artifactURL, artifactSHA256 string
		var artifactSizeBytes int64
		if h.store != nil {
			artifacts, aErr := h.store.GetArtifactsByJobForClient(ctx, jobID, clientID, 50)
			if aErr == nil {
				if a := selectPrimaryReadyArtifact(artifacts); a != nil {
					artifactSHA256 = a.SHA256
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
		// attempt_id, lease_id) and, when terminal, the failure identity
		// (attempt status + worker-reported error_code/error_message).
		// Best-effort — the attempt row may not exist yet when the job is
		// PENDING. Surfacing the failure reason here lets M2M producers
		// diagnose a FAILED job without operator shell access to the
		// master (the error is already persisted on task_attempts by
		// CompleteFinal; this only exposes the same fact to the owner).
		var workerID, taskID, attemptID, leaseID string
		var leaseAt string
		var attemptStatus, attemptErrorCode, attemptErrorMessage string
		var workerTiming gin.H
		if h.store != nil {
			snap, sErr := h.store.GetLatestTaskAttemptForJobForClient(ctx, jobID, clientID)
			if sErr == nil && snap != nil {
				workerID = snap.WorkerID
				taskID = snap.TaskID
				attemptID = snap.AttemptID
				leaseID = snap.LeaseID
				attemptStatus = snap.Status
				attemptErrorCode = snap.ErrorCode
				attemptErrorMessage = snap.ErrorMessage
				if jobErrorMessage == "" {
					jobErrorMessage = snap.ErrorMessage
				}
				leaseAt = snap.LeasedAt
				workerTiming = gin.H{
					"leased_at":   nullableJobTimestamp(snap.LeasedAt),
					"started_at":  nullableJobTimestamp(snap.StartedAt),
					"finished_at": nullableJobTimestamp(snap.FinishedAt),
					"download_ms": snap.DownloadMS,
					"decode_ms":   snap.DecodeMS,
					"compose_ms":  snap.ComposeMS,
					"encode_ms":   snap.EncodeMS,
					"mux_ms":      snap.MuxMS,
					"upload_ms":   snap.UploadMS,
					"frames":      snap.Frames,
					"encode_fps":  encodeFPS(snap.Frames, snap.EncodeMS),
				}
			}
		}

		prefetchDispatchStatus, futureAssetPlan, prefetchIssue := h.prefetchReadModel(ctx, jobID, clientID)

		created := forwarding.SourceProvider == ExternalAPISourceProvider
		statusURL := "/api/v1/jobs/" + jobID

		resp := gin.H{
			"ok":           true,
			"job_id":       jobID,
			"status":       status,
			"created":      created,
			"status_url":   statusURL,
			"worker_id":    workerID,
			"artifact_url": artifactURL,
			"sha256":       artifactSHA256,
			"error":        jobErrorMessage,
			"created_at":   nullableJobTimestamp(createdAt),
			// Public job.started_at is the lease acquisition time. The worker's
			// own execution start is separately reported in worker_timing.
			"started_at":   nullableJobTimestamp(leaseAt),
			"completed_at": nullableJobTimestamp(completedAt),
		}
		if workerTiming != nil {
			resp["worker_timing"] = workerTiming
		}
		if artifactURL != "" {
			resp["artifact_size_bytes"] = artifactSizeBytes
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
		// Failure surfacing (new, additive): only when the job is FAILED and
		// the latest attempt carries a worker-reported error. Additive-only
		// keeps the existing 4-field contract and every current consumer
		// (run-flow.sh polling, verify smoke tests) byte-compatible on
		// success paths.
		if status == "FAILED" && attemptStatus != "" {
			resp["attempt_status"] = attemptStatus
			resp["failure_code"] = attemptErrorCode
			resp["failure_message"] = attemptErrorMessage
		} else if status == "FAILED" {
			// No attempt row (or error not yet persisted): still tell the
			// client the failure came from the job lifecycle, falling back to
			// the jobs.error_message written by Fail.
			if jobErrorMessage != "" {
				resp["failure_code"] = "JOB_FAILED"
				resp["failure_message"] = jobErrorMessage
			}
		}
		if prefetchDispatchStatus != "" {
			resp["dispatch_status"] = prefetchDispatchStatus
		}
		if futureAssetPlan != "" {
			resp["future_asset_plan"] = futureAssetPlan
		}
		if prefetchIssue != nil {
			resp["prefetch_issue"] = prefetchIssue
		}
		if h.jobs.TaskReader != nil {
			if task, taskErr := h.jobs.TaskReader.GetByJobID(ctx, jobID); taskErr == nil && task != nil {
				if specs, ok := h.jobs.TaskReader.(taskgraph.TaskSpecStore); ok {
					if payload, payloadErr := specs.GetTaskSpecPayload(ctx, task.ID); payloadErr == nil {
						if pending, ok := payload["runtime_assets_pending"].(bool); ok && pending {
							resp["runtime_assets_pending"] = true
							resp["future_asset_plan"] = "awaiting_runtime_assets"
							switch state, _ := payload[runtimeFinalizeStatusKey].(string); state {
							case runtimeFinalizeQueued:
								resp["dispatch_status"] = "stock_preparation_queued"
							case runtimeFinalizeRunning:
								resp["dispatch_status"] = "stock_preparation_running"
							case runtimeFinalizeFailed:
								resp["dispatch_status"] = "stock_preparation_failed"
							default:
								resp["dispatch_status"] = "waiting_runtime_assets"
							}
							if state, ok := payload[runtimeFinalizeStatusKey].(string); ok && state != "" {
								resp["finalization_status"] = state
							}
							if message, ok := payload[runtimeFinalizeErrorKey].(string); ok && message != "" {
								resp["finalization_error"] = message
							}
						}
					}
				}
			}
		}
		c.JSON(http.StatusOK, resp)
	}
}

func nullableJobTimestamp(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return parsed.UTC().Format(time.RFC3339Nano)
		}
	}
	return raw
}

func encodeFPS(frames int64, encodeMS float64) float64 {
	if frames <= 0 || encodeMS <= 0 {
		return 0
	}
	return float64(frames) * 1000 / encodeMS
}

// prefetchReadModel projects the latest durable prefetch lifecycle events into
// the small polling response. The event journal remains the detailed source of
// truth; this projection deliberately exposes only stable state labels and no
// asset/job identifiers beyond the already-authorized job URL.
func (h *Handlers) prefetchReadModel(ctx context.Context, jobID, clientID string) (dispatchStatus, futureAssetPlan string, issue map[string]interface{}) {
	if h == nil || h.store == nil || jobID == "" || clientID == "" {
		return "", "", nil
	}
	events, err := h.store.ListJobEventsForClient(ctx, jobID, clientID, 100)
	if err != nil {
		return "", "", nil
	}
	// The M2M read returns newest first; the state projection consumes the
	// journal in chronological order so later events replace earlier state.
	chronological := make([]store.JobEvent, len(events))
	for i := range events {
		chronological[len(events)-1-i] = events[i]
	}
	dispatchStatus, futureAssetPlan = prefetchStateFromEvents(chronological)
	issue = prefetchIssueFromEvents(events)
	return dispatchStatus, futureAssetPlan, issue
}

func prefetchStateFromEvents(events []store.JobEvent) (dispatchStatus, futureAssetPlan string) {
	for _, event := range events {
		switch event.Event {
		case "prefetch.prefetch_prepared":
			dispatchStatus = "prefetch_ready"
			futureAssetPlan = "prepared"
		case "prefetch.prefetch_preparation_waiting":
			dispatchStatus = "prefetch_waiting_assets"
			futureAssetPlan = "applied"
		case "prefetch.future_plan_applied":
			dispatchStatus = "prefetch_preparing"
			futureAssetPlan = "applied"
		case "prefetch.future_plan_received":
			dispatchStatus = "prefetch_planning"
			futureAssetPlan = "received"
		case "prefetch.future_plan_sent":
			dispatchStatus = "prefetch_queued"
			futureAssetPlan = "sent"
		case "prefetch.prejob_prepare_failed", "prefetch.prefetch_failed", "prefetch.prefetch_error":
			dispatchStatus = "prefetch_failed"
			futureAssetPlan = "failed"
		}
	}
	return dispatchStatus, futureAssetPlan
}

// prefetchIssueFromEvents exposes the latest worker-reported blocker to the
// owning M2M client. The event list is already ownership-scoped and newest
// first, so this returns the first actionable prefetch event without exposing
// unrelated jobs or unbounded event history.
func prefetchIssueFromEvents(events []store.JobEvent) map[string]interface{} {
	var issue map[string]interface{}
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if !strings.HasPrefix(event.Event, "prefetch.") {
			continue
		}
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(event.RawJSON), &raw); err != nil {
			continue
		}
		if event.Event == "prefetch.prefetch_prepared" {
			issue = nil
			continue
		}
		reason, _ := raw["error_reason"].(string)
		if reason == "" {
			// Successful asset resolution clears a prior per-asset failure;
			// preparation_waiting remains useful progress until PREPARED.
			if event.Event == "prefetch.asset_ready" && issue != nil && issue["event"] != "prefetch.prefetch_preparation_waiting" {
				issue = nil
			}
			continue
		}
		issue = map[string]interface{}{
			"event":       event.Event,
			"message":     reason,
			"occurred_at": event.Timestamp,
		}
		for _, key := range []string{"worker_id", "task_id", "asset_key"} {
			if value, ok := raw[key].(string); ok && value != "" {
				issue[key] = value
			}
		}
	}
	return issue
}
