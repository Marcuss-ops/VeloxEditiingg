// Package pipeline — job_submit.go is the HTTP adapter for
// POST /api/v1/jobs. It owns exactly two concerns: decode the strict JSON body,
// and write the canonical intake outcome to this transport. The intake logic
// itself lives in job_submit_core.go (submitJobCore), which POST
// /api/v1/jobs/batch also calls per item — see that file for why the batch
// surface no longer replays this handler.
//
// Domain logic lives in:
//   - job_submit_core.go (submitJobCore: the transport-neutral intake)
//   - intake_validation.go (DTO types, limit consts, regexes,
//     SubmitJobValidationError, ValidateSubmitJobRequest)
//   - canonical_request_projection.go (NormalizeExternalJobSubmission)
//   - projection/ (neutral nested render mapping and worker projection)
//   - worker_payload_projection.go (SubmitJobRequest → projection adapter)
//   - enqueue_persistence.go (GetSubmittedJob polling)
//
// Decoding strategy: json.NewDecoder(...).Decode with
// DisallowUnknownFields — NOT c.ShouldBindJSON. The strict
// decoder rejects any field name not on the struct, so a
// typo'd json blob fails with 400 invalid_json before
// downstream code runs.
package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
)

// SubmitJob handles POST /api/v1/jobs.
//
// Strict decoding: json.NewDecoder(...).Decode with DisallowUnknownFields
// rejects any field name not on the struct, so a typo'd json blob fails with
// 400 invalid_json BEFORE we touch downstream code. Gin's binding tag
// machinery is permissive for cross-field validation and silently accepts
// unknown fields, which is the wrong default for an external-API surface.
//
// After decoding, the handler is a thin transport adapter: it extracts the
// request-scoped identity the M2M middleware resolved, runs the canonical
// intake core, and writes the outcome (status + envelope + Location /
// Retry-After headers). It performs no validation and no resolution of its own,
// so the batch envelope can share the identical logic without an HTTP replay.
func (h *Handlers) SubmitJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "invalid_json",
				"message": "request body must be valid JSON without unknown fields: " + err.Error(),
			})
			return
		}

		out := h.submitJobCore(c.Request.Context(), req, intakeIdentity{
			ClientID: ClientIDFromContext(c),
			// This IS the canonical submit surface; the batch envelope stamps
			// its own label (see batchIntakeIdentity).
			IntakeSource: creatorflow.IntakeSourceCanonical,
			Quota:        KeyFromContext(c),
		})

		// Headers first: a 429 from the publishing-target resolver carries
		// Retry-After, and the accepted path carries Location. Header() must be
		// called before c.JSON writes the status line.
		for key, values := range out.Header {
			for _, value := range values {
				c.Header(key, value)
			}
		}
		if out.Location != "" {
			c.Header("Location", out.Location)
		}
		// Stash scene count + total duration so the M2M audit middleware (or the
		// response writer wrapper) records the ACTUAL request shape in
		// m2m_audit_log. Only an admitted request carries usage: the core leaves
		// Usage nil on every rejection.
		if out.Usage != nil {
			SetUsageStats(c, out.Usage.Scenes, out.Usage.TotalDurationSeconds)
		}
		c.JSON(out.Status, out.Body)
	}
}

// GetSubmittedJob handles GET /api/v1/jobs/:id.
//
// Polling endpoint for jobs that came in via POST /api/v1/jobs.
// Reuses the canonical lookup surface: creator_forwardings.target_job_id
// + jobs.Reader.Get — the same data path the resolver committed to
// when the job was created. No new SQL is introduced at the lookup
// layer; the helper at
// forwardingstore.GetCreatorForwardingByTargetJobID (reached via
// SQLiteStore.Forwarding()) is the only new addition, and it is
// exercised by the migration-102 B-tree index for O(log N) polling
// under M2M load.
//
// Response shape (4 fields, per user P2 spec + status_url canonical
// chain):
//
//	job_id      canonical id (the same string POST returned).
//	status      jobs.Status (canonical render state) — falls back to
//	            forwarding.Status when the jobs row has not materialized
//	            yet (resolver race in pre-FORWARDING state: the row
//	            exists but target_job_id was not yet committed).
//	created     bool — true if the row was produced by POST /api/v1/jobs
//	            (source_provider == ExternalAPISourceProvider). False
//	            if it came in via POST /api/v1/creator/jobs. The
//	            indicator lets clients distinguish the two intake
//	            paths without a separate lookup.
//	status_url  env-relative path "/api/v1/jobs/{job_id}" so clients
//	            can chain the canonical into their next request
//	            without re-deriving the URL.
//
// 404 envelope mirrors the m2m_token_rejected shape (ok:false,
// error:job_not_found, message:...) so a single error dispatcher
// handles auth + lookup misses.
//
// Auth scope: jobs.submit. The M2M middleware runs before the
// handler and rejects requests lacking a valid token. Cross-client
// authorization is strict: a token for client A can poll only jobs
// whose creator_forwardings.external_client_id is client A. A missing
// job and a job owned by another client intentionally share the same
// 404 job_not_found envelope. The ownership boundary is enforced by
// forwardingstore.GetCreatorForwardingByTargetJobID (via
// SQLiteStore.Forwarding()).
