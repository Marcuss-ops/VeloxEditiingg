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
	"net/http"
	"strings"
	"github.com/gin-gonic/gin"
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
		if h.jobs.Reader != nil {
			if job, gErr := h.jobs.Reader.Get(ctx, jobID); gErr == nil && job != nil {
				status = string(job.Status)
			}
		}
		created := forwarding.SourceProvider == ExternalAPISourceProvider
		statusURL := "/api/v1/jobs/" + jobID
		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"job_id":     jobID,
			"status":     status,
			"created":    created,
			"status_url": statusURL,
		})
	}
}

// NormalizeExternalJobSubmission is the canonical typed-DTO adapter
// for POST /api/v1/jobs. It walks the SAME path that creator_push
// walks (creator_push.go::normalizeCreatorPushRequest):
//
//  1. Build a flat raw map mirroring the wire shape that
//     remoteengine.ParseRemotePipelineResult consumes (status,
//     job_id, video_name, script_text, voiceover_paths, scenes[],
//     delivery_plan[]).
//
//  2. Pass it through remoteengine.ParseRemotePipelineResult to
//     produce the typed RemotePipelineResult DTO. This is the single
//     point where typed validation happens — there is no hand-rolled
//     string-key lookup anymore.
//
//  3. Call (*RemotePipelineResult).ToWorkerPayload() which:
//     - base-copies fields from the flat raw map (delivery_plan,
//     output_path, non-DTO passthroughs),
//     - overlays typed DTO fields (job_id from RemoteJobID,
//     video_name from Script.Title, scenes_json from Scenes,
//     voiceover_paths from Voiceover.Paths).
//
//  4. Stamp the stable identity tuple:
//     - source_provider    = ExternalAPISourceProvider (constant,
//     low-cardinality — see [P0 #4] audit for the rationale).
//     - source_job_id      = the (already-validated) IdempotencyKey.
//     - target_executor_id = JobSubmitTargetExecutorID (constant).
//
// Returns *CanonicalCompletedPayload (alias for normalizedCreatorPush,
// the type CreatorPush's path also returns), so a future third
// producer (e.g., webhook intake) only has to return the same shape.
//
// submitRequestToRawPayload's retry_budget handling is the canonical
// boundary for the *int round-trip contract: nil → DefaultRetryBudget
// (mirrors OpenAPI default), pointer-to-0 → 0 (preserves explicit
// client choice). Anything else is just a value dereference.
//
// Trim policy in submitRequestToRawPayload: trim SPACE around
// identity-bearing fields (IdempotencyKey, VideoName, scene
// clip_link / image_link, delivery destination_id) because these
// participate in dedup / URL parsing downstream. Do NOT trim
// ScriptText or scene `text` — these are CONTENT fields where
// legitimate whitespace might be present.
