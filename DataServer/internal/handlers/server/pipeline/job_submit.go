// Package pipeline — job_submit.go is the thin composer
// for POST /api/v1/jobs. Orchestrates decode → validate
// (IdempotencyKey byte-level + ValidateSubmitJobRequest) →
// NormalizeExternalJobSubmission (canonical_request_projection.go) →
// enqueue (jobs/enqueue) → 202 Accepted response
// (response_shaping.go status + Location helpers).
//
// Domain logic lives in:
//   - intake_validation.go (DTO types, limit consts, regexes,
//     SubmitJobValidationError, ValidateSubmitJobRequest)
//   - canonical_request_projection.go (NormalizeExternalJobSubmission)
//   - asset_projection.go (nested asset map builders)
//   - worker_payload_projection.go (submitRequestToRawPayload and worker projection)
//   - enqueue_persistence.go (GetSubmittedJob polling)
//   - telemetry.go (isLegacyCompatShape,
//     countScenesWithClipLink, legacyBodySinkOrNoop)
//
// Decoding strategy: json.NewDecoder(...).Decode with
// DisallowUnknownFields — NOT c.ShouldBindJSON. The strict
// decoder rejects any field name not on the struct, so a
// typo'd json blob fails with 400 invalid_json before
// downstream code runs.
package pipeline

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/creatorflow"
)

// invalid_json BEFORE we touch downstream code. Gin's binding tag
// machinery is permissive for cross-field validation and silently
// accepts unknown fields, which is the wrong default for an
// external-API surface.
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

		// Idempotency-key validation: 1..128 valid UTF-8 bytes with no
		// control chars or forbidden separators (':' or '%'). The
		// helper trims whitespace before validating, so the canonical
		// (post-trim) form is what reaches the resolver as source_job_id.
		// A typed *IdempotencyKeyError carries machine-readable reason
		// + diagnostics so the API envelope is actionable. 400 because
		// idempotency_key is a request-level byte-shape issue, distinct
		// from the 422 semantic issues that follow.
		if vErr, bad := ValidateIdempotencyKey(req.IdempotencyKey); bad {
			writeIdempotencyKeyError(c, vErr)
			return
		}
		// ValidateIdempotencyKey intentionally trims only for validation;
		// carry the same canonical value into the resolver, response, and
		// logs so retries with surrounding whitespace cannot diverge.
		req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)

		// SubmitJob-level validation: video_name byte-length, scenes
		// count + each scene (text, duration bounds), each delivery
		// entry destination_id. Aggregates ALL violations into the
		// returned details so the client can fix them in one round
		// trip. Maps to 422 invalid_payload per OpenAPI's ErrorEnvelope.
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"ok":      false,
				"error":   vErr.Code,
				"message": vErr.Message,
				"details": vErr.Details,
			})
			return
		}

		if req.ManifestRef != nil {
			resolvedReq, resolveErr := h.ResolveRenderManifestRef(c.Request.Context(), req)
			if resolveErr != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":      false,
					"error":   resolveErr.Code,
					"message": resolveErr.Message,
					"details": resolveErr.Details,
				})
				return
			}
			req = resolvedReq
			if vErr, bad := ValidateSubmitJobRequest(req); bad {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":      false,
					"error":   vErr.Code,
					"message": vErr.Message,
					"details": vErr.Details,
				})
				return
			}
		}

		// Resolve the optional channel/group selector before any destination
		// pre-flight or quota check. Group expansion is server-side and
		// all-or-nothing: once this returns, DeliveryPlan contains the
		// deterministic concrete snapshot that every downstream validator and
		// enqueue step must observe.
		if req.PublishingTarget != nil {
			resolvedReq, targetErr := h.resolvePublishingTarget(c.Request.Context(), req)
			if targetErr != nil {
				writePublishingTargetError(c, targetErr)
				return
			}
			req = resolvedReq
			// Re-run the canonical validator over the concrete plan so any
			// delivery-plan constraints also apply to expanded group members.
			if vErr, bad := ValidateSubmitJobRequest(req); bad {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":      false,
					"error":   vErr.Code,
					"message": vErr.Message,
					"details": vErr.Details,
				})
				return
			}
		}

		// Delivery-destination existence pre-flight (P0 #2 closure).
		// Extracted into checkDeliveryPlanDestinations (job_submit_preflight.go);
		// returns true when a 500 store_failure or 422 invalid_payload
		// response was already written.
		if checkDeliveryPlanDestinations(c, h, req) {
			return
		}

		// SSRF URL validator (P1 admin-audit trail step #2): every
		// voiceover_path, scene.clip_link, scene.image_link MUST
		// satisfy the hybrid blocklist+allowlist policy. Runs AFTER
		// byte-level + cross-field validators so attackers can't
		// probe private IP classification on bodies that fail
		// earlier checks (which would leak validation gaps).
		if ssrfErrs := ValidateAllExternalURLs(req, h.cfg); len(ssrfErrs) > 0 {
			details := make([]gin.H, 0, len(ssrfErrs))
			for _, e := range ssrfErrs {
				details = append(details, gin.H{
					"path":   e.Path,
					"url":    e.URL,
					"reason": e.Reason,
				})
			}
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"ok":      false,
				"error":   "ssrf_rejected",
				"message": "one or more external URLs failed the egress policy",
				"details": details,
			})
			return
		}

		// Per-request quota (rate limit / scenes / total duration).
		// Runs AFTER validation+SSRF so the rejection paths are
		// stable: a body that violates the cross-field rules gets
		// 422 first, and only well-formed shapes hit the quota
		// gate. The M2MContext must be populated by the route
		// middleware; if it isn't this returns 500 with a hint
		// (a misconfigured production deployment where /api/v1/jobs
		// was wired without M2M auth).
		if qerr := EnforcePerRequestQuota(c, req, h.cfg); qerr != nil {
			if qe, ok := qerr.(*QuotaError); ok {
				c.JSON(http.StatusTooManyRequests, gin.H{
					"ok":      false,
					"error":   "m2m_quota_exceeded",
					"message": qe.Error(),
					"details": gin.H{
						"reason":   qe.Reason,
						"observed": qe.Observed,
						"cap":      qe.Cap,
					},
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"error":   "m2m_quota_failure",
				"message": qerr.Error(),
			})
			return
		}

		// Derive Creator-compatible identity via the canonical
		// pipeline path: SubmitJobRequest → ParseRemotePipelineResult
		// (typed DTO) → ToWorkerPayload → CanonicalCompletedPayload.
		// This is the SAME path creator_push's normalizeCreatorPushRequest
		// takes, so the resolver sees one canonical shape regardless of
		// the producer (creator workstation vs external /api/v1/jobs).
		canonical := h.NormalizeExternalJobSubmission(req)

		// Delegate to the same resolver used by CreatorPush.
		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			canonical.SourceProvider,
			canonical.SourceJobID,
			canonical.TargetExecutorID,
			canonical.WorkerPayload,
			canonical.DeliveryPlan,
			canonical.PublicationSpecs,
		)
		if err != nil {
			// P0 contract: every resolver-layer error is mapped
			// to the canonical HTTP envelope by the shared helper
			// in package creatorflow. Previously this branch was
			// missing the enqueue.ValidationErrorField mapping
			// entirely, so any enqueue-layer validation error
			// (missing delivery_plan entry, malformed destination_id,
			// …) silently downgraded to a 500. The helper owns the
			// full mapping — the third arg dropped in [P0 #2]
			// because the typed validationError carries the field
			// path internally and the helper falls back to
			// "idempotency_key" only when no typed path is available.
			creatorflow.WriteResolverError(c, err)
			return
		}
		if forwarded == nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"error":   "resolver_failure",
				"message": "job resolved without an enqueue response",
			})
			return
		}

		response := gin.H{}
		for key, value := range forwarded {
			response[key] = value
		}
		response["ok"] = true
		response["accepted_from"] = "api_v1_jobs"
		response["idempotency_key"] = req.IdempotencyKey
		if _, owned := response["dispatch_status"]; !owned {
			response["dispatch_status"] = "queued_for_workers"
		}
		// Surface the resolved client_id in the response envelope so
		// clients can correlate locally-logged requests (no DB join
		// needed). Null-string when M2M middleware did not run
		// (admin-auth fallback mount).
		if cid := ClientIDFromContext(c); cid != "" {
			response["client_id"] = cid
		}

		h.intakeSinkOrNoop().IncAccepted("api_v1_jobs")
		jobID, _ := response["job_id"].(string)
		pipelineLog(
			"API_V1_JOBS_ACCEPTED idem_hash=%s job_id=%s client_id=%s",
			logHashShort(req.IdempotencyKey),
			jobID,
			ClientIDFromContext(c),
		)

		// Status URL + Location header: canonical polling endpoint
		// address for this job_id. The 202 response carries BOTH the
		// JSON field (status_url) AND the Location header (per HTTP
		// RFC 7231 location-of-resource) so automation clients can
		// pick whichever fits their language — curl --include
		// surfaces the header; jq .status_url surfaces the field.
		// Env-relative (no host:port / scheme) so the helper works
		// across dev / staging / production environments unchanged
		// and matches the openapi.yaml documented shape.
		if jobID != "" {
			statusURL := "/api/v1/jobs/" + jobID
			c.Header("Location", statusURL)
			response["status_url"] = statusURL
		}

		// Stash scene count + total duration so the M2M audit
		// middleware (or the response writer wrapper) records the
		// ACTUAL request shape in m2m_audit_log. Best effort; if
		// the keys are missing the audit row simply logs 0/0.
		var totalDur float64
		for _, s := range req.Scenes {
			totalDur += s.DurationSeconds
		}
		SetUsageStats(c, len(req.Scenes), totalDur)

		c.JSON(http.StatusAccepted, response)
	}
}

// GetSubmittedJob handles GET /api/v1/jobs/:id.
//
// Polling endpoint for jobs that came in via POST /api/v1/jobs.
// Reuses the canonical lookup surface: creator_forwardings.target_job_id
// + jobs.Reader.Get — the same data path the resolver committed to
// when the job was created. No new SQL is introduced at the lookup
// layer; the helper at store.GetCreatorForwardingByTargetJobID is the
// only new addition, and it is exercised by the migration-102 B-tree
// index for O(log N) polling under M2M load.
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
// authorization (a token for client A polling a job created by
// client B) is intentionally SOFT for v1 — any valid M2M token
// can poll any job_id. The strict boundary tightening
// (creator_forwardings.external_client_id == m2m_client_id from
