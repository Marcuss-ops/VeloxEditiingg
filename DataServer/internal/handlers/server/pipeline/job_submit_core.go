// Package pipeline — job_submit_core.go is the transport-neutral core of the
// canonical single-job intake.
//
// # Why this file exists
//
// POST /api/v1/jobs/batch used to reach the intake logic by REBUILDING an HTTP
// exchange per item: it re-marshalled each SubmitJobRequest to JSON, cloned the
// gin.Context, pointed a synthetic gin.ResponseWriter at the clone, invoked the
// single-job handler, and then parsed the JSON response body back into a batch
// item result. The batch surface therefore depended on the HTTP serialization
// round-trip of its own handler, and the application seam it actually wanted
// (creatorflow.CanonicalJobSubmitter) was reachable only by simulating HTTP.
//
// The core below owns the whole canonical intake — byte-level validation →
// canonical recipe/assembly normalization → request validation → manifest-ref
// and publishing-target resolution → delivery/asset/SSRF/quota pre-flight →
// canonical projection → CanonicalJobSubmitter → response envelope — and
// returns a transport-neutral outcome. POST /api/v1/jobs is a thin adapter over
// it, and POST /api/v1/jobs/batch calls it directly per item, so both surfaces
// share ONE implementation and neither simulates the other.
//
// Semantics are intentionally unchanged: identical validation order, identical
// status codes, identical error codes/messages/details (including the
// `"details":null` keys the historical envelope emitted), identical
// intake-source telemetry and identical accept log lines.
package pipeline

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/m2mkeys"
	"velox-shared/compatibility"
)

// intakeSurfaceAPIv1Jobs is the legacy CreatorIntakeSink / accept-log
// attribution for the canonical submit surface.
//
// Batch items inherit this label because the batch envelope historically
// dispatched through the single-job handler, so accepted batch items were
// counted on the `api_v1_jobs` series. Preserving the label keeps the
// operator-facing intake counts continuous across this refactor; moving batch
// items onto their own series is a telemetry change that must be made
// deliberately (the submitter's own `pipeline.intake_source_accepted_total`
// already carries `intake_source=batch` for per-surface measurement).
const intakeSurfaceAPIv1Jobs = "api_v1_jobs"

// intakeIdentity is the request-scoped identity resolved at the HTTP boundary.
// Extracting it once keeps the core free of any gin.Context dependency, so the
// batch intake can supply the same values for an item that never had its own
// request.
type intakeIdentity struct {
	// ClientID is the M2M client id resolved by the auth middleware ("" when
	// the middleware did not run, e.g. the admin-auth fallback mount).
	ClientID string
	// IntakeSource is the bounded intake-source label recorded by the
	// canonical submitter (`canonical` for POST /api/v1/jobs, `batch` for
	// POST /api/v1/jobs/batch items).
	IntakeSource string
	// Quota is the typed M2M key the per-request quota is enforced against,
	// or nil when M2M auth did not run.
	Quota *m2mkeys.M2MAPIKey
}

// intakeUsage is the request shape the M2M audit log records for an ADMITTED
// request. It mirrors what SetUsageStats stashes on the gin.Context.
type intakeUsage struct {
	Scenes               int
	TotalDurationSeconds float64
}

// intakeResponse is the transport-neutral outcome of submitJobCore.
type intakeResponse struct {
	// Status is the canonical HTTP status the single-job surface returns for
	// this outcome (202 accept, or the 4xx/5xx rejection status).
	Status int
	// Body is the canonical JSON envelope. It is the SAME map the single-job
	// surface serializes, so the batch envelope classifies a failure from the
	// typed values instead of parsing serialized JSON back.
	Body gin.H
	// Location is the canonical status_url, set only on an accepted intake.
	Location string
	// Header carries response headers the outcome requires (e.g. Retry-After
	// on a provider rate limit). The batch envelope intentionally drops them,
	// matching the previous per-item dispatch whose capture discarded headers.
	Header http.Header
	// Usage is non-nil only on an ACCEPTED intake: the audit middleware
	// records the request shape for admitted work, never for rejections.
	Usage *intakeUsage
}

// submitJobCore runs the canonical single-job intake for one already-decoded
// request and returns the transport-neutral outcome. It never writes to a
// transport: the caller owns status/body/headers.
func (h *Handlers) submitJobCore(ctx context.Context, req SubmitJobRequest, identity intakeIdentity) intakeResponse {
	source := identity.IntakeSource
	if source == "" {
		source = creatorflow.IntakeSourceCanonical
	}

	// Idempotency-key validation: 1..128 valid UTF-8 bytes with no control
	// chars or forbidden separators (':' or '%'). The helper trims whitespace
	// before validating, so the canonical (post-trim) form is what reaches the
	// resolver as source_job_id. A typed *IdempotencyKeyError carries a
	// machine-readable reason + diagnostics so the API envelope is actionable.
	// 400 because idempotency_key is a request-level byte-shape issue, distinct
	// from the 422 semantic issues that follow.
	if vErr, bad := ValidateIdempotencyKey(req.IdempotencyKey); bad {
		status, body := idempotencyKeyErrorEnvelope(vErr)
		return intakeResponse{Status: status, Body: body}
	}
	// ValidateIdempotencyKey intentionally trims only for validation; carry the
	// same canonical value into the resolver, response, and logs so retries
	// with surrounding whitespace cannot diverge.
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)

	if err := compatibility.ValidateNoLegacyAliases(req.Spec); err != nil {
		return reject(http.StatusUnprocessableEntity, "legacy_alias_rejected", err.Error())
	}
	if err := NormalizeCanonicalRecipe(&req); err != nil {
		return rejectWithDetails(http.StatusUnprocessableEntity, "unsupported_recipe", err.Error(),
			[]gin.H{{"path": "job_type/spec", "issue": "invalid_recipe"}})
	}
	if req.Assembly != nil {
		if _, err := req.Assembly.Normalize(req.IdempotencyKey); err != nil {
			return reject(http.StatusUnprocessableEntity, "invalid_assembly", err.Error())
		}
	}

	// SubmitJob-level validation: video_name byte-length, scenes count + each
	// scene (text, duration bounds), each delivery entry destination_id.
	// Aggregates ALL violations into the returned details so the client can fix
	// them in one round trip. Maps to 422 invalid_payload per OpenAPI's
	// ErrorEnvelope.
	if vErr, bad := ValidateSubmitJobRequest(req); bad {
		return validationRejection(vErr)
	}

	if req.ManifestRef != nil {
		resolvedReq, resolveErr := h.ResolveRenderManifestRef(ctx, req)
		if resolveErr != nil {
			return rejectWithDetails(http.StatusUnprocessableEntity, resolveErr.Code, resolveErr.Message, resolveErr.Details)
		}
		req = resolvedReq
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			return validationRejection(vErr)
		}
	}

	// Resolve the optional channel/group selector before any destination
	// pre-flight or quota check. Group expansion is server-side and
	// all-or-nothing: once this returns, DeliveryPlan contains the
	// deterministic concrete snapshot that every downstream validator and
	// enqueue step must observe.
	if req.PublishingTarget != nil {
		resolvedReq, targetErr := h.resolvePublishingTarget(ctx, req)
		if targetErr != nil {
			status, body, header := publishingTargetErrorEnvelope(targetErr)
			return intakeResponse{Status: status, Body: body, Header: header}
		}
		req = resolvedReq
		// Re-run the canonical validator over the concrete plan so any
		// delivery-plan constraints also apply to expanded group members.
		if vErr, bad := ValidateSubmitJobRequest(req); bad {
			return validationRejection(vErr)
		}
	}

	// Delivery-destination existence pre-flight (P0 #2 closure): yields a
	// 500 store_failure or 422 invalid_payload envelope when a destination is
	// missing/disabled.
	if status, body, bad := deliveryPlanDestinationEnvelope(ctx, h, &req); bad {
		return intakeResponse{Status: status, Body: body}
	}

	// Asset registry/blob pre-flight: validate every local velox-asset
	// reference before enqueue. This is a Master-local read-only integrity
	// check; it does not fetch or prefetch anything onto workers. Deferred
	// velox-drive references remain deferred to the authenticated worker
	// bridge.
	if status, body, bad := assetPreflightEnvelope(ctx, h, req); bad {
		return intakeResponse{Status: status, Body: body}
	}

	// SSRF URL validator (P1 admin-audit trail step #2): every nested scene
	// asset URL MUST satisfy the hybrid blocklist+allowlist policy. Runs AFTER
	// byte-level + cross-field validators so attackers can't probe private IP
	// classification on bodies that fail earlier checks (which would leak
	// validation gaps).
	if ssrfErrs := ValidateAllExternalURLs(req, h.cfg); len(ssrfErrs) > 0 {
		status, body := ssrfValidationErrorEnvelope(ssrfErrs)
		return intakeResponse{Status: status, Body: body}
	}

	// Per-request quota (rate limit / scenes / total duration). Runs AFTER
	// validation+SSRF so the rejection paths are stable: a body that violates
	// the cross-field rules gets 422 first, and only well-formed shapes hit the
	// quota gate.
	if qerr := enforcePerRequestQuota(identity.Quota, req, h.cfg); qerr != nil {
		status, body := quotaErrorEnvelope(qerr)
		return intakeResponse{Status: status, Body: body}
	}

	// Derive Creator-compatible identity via the canonical pipeline path:
	// SubmitJobRequest → ParseRemotePipelineResult (typed DTO) → ToWorkerPayload
	// → CanonicalCompletedPayload. This is the SAME path creator_push's
	// normalizeCreatorPushRequest takes, so the resolver sees one canonical
	// shape regardless of the producer (creator workstation vs external
	// /api/v1/jobs).
	canonical := h.NormalizeExternalJobSubmission(req)
	if canonical == nil {
		return reject(http.StatusBadGateway, "canonical_projection_failed",
			"unable to build the renderer-only payload")
	}

	// Delegate to the same resolver used by CreatorPush (and, through it, to
	// creatorflow.CanonicalJobSubmitter — the single production Job+Task path).
	forwarded, err := h.resolveCompletedPayload(
		ctx,
		canonical.SourceProvider,
		canonical.SourceJobID,
		canonical.TargetExecutorID,
		canonical.WorkerPayload,
		canonical.DeliveryPlan,
		canonical.PublicationSpecs,
		canonical.Assembly,
		identity.ClientID,
		source,
	)
	if err != nil {
		// P0 contract: every resolver-layer error is mapped to the canonical
		// envelope by the shared mapper in package creatorflow, so a surface
		// that owns no HTTP response classifies identically to this one.
		status, body := creatorflow.ResolverErrorEnvelope(err)
		return intakeResponse{Status: status, Body: body}
	}
	if forwarded == nil {
		return reject(http.StatusInternalServerError, "resolver_failure",
			"job resolved without an enqueue response")
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
	// Surface the resolved client_id in the response envelope so clients can
	// correlate locally-logged requests (no DB join needed).
	if identity.ClientID != "" {
		response["client_id"] = identity.ClientID
	}

	h.intakeSinkOrNoop().IncAccepted(intakeSurfaceAPIv1Jobs)
	jobID, _ := response["job_id"].(string)
	pipelineLog(
		"API_V1_JOBS_ACCEPTED idem_hash=%s job_id=%s client_id=%s",
		logHashShort(req.IdempotencyKey),
		jobID,
		identity.ClientID,
	)

	// Status URL + Location header: canonical polling endpoint address for
	// this job_id. The 202 response carries BOTH the JSON field (status_url)
	// AND the Location header (per HTTP RFC 7231 location-of-resource) so
	// automation clients can pick whichever fits their language — curl
	// --include surfaces the header; jq .status_url surfaces the field.
	// Env-relative (no host:port / scheme) so the helper works across dev /
	// staging / production environments unchanged and matches the
	// openapi.yaml documented shape.
	out := intakeResponse{Status: http.StatusAccepted, Body: response}
	if jobID != "" {
		out.Location = "/api/v1/jobs/" + jobID
		response["status_url"] = out.Location
	}

	var totalDuration float64
	for _, scene := range req.Scenes {
		totalDuration += scene.DurationSeconds
	}
	out.Usage = &intakeUsage{Scenes: len(req.Scenes), TotalDurationSeconds: totalDuration}
	return out
}

// reject builds a rejection envelope WITHOUT a `details` key. Use it where the
// historical envelope omitted details entirely.
func reject(status int, code, message string) intakeResponse {
	return intakeResponse{Status: status, Body: gin.H{
		"ok":      false,
		"error":   code,
		"message": message,
	}}
}

// rejectWithDetails builds a rejection envelope that ALWAYS carries the
// `details` key — including `"details":null` when the source slice is nil —
// so the wire shape stays byte-identical to the historical handler envelope.
func rejectWithDetails(status int, code, message string, details any) intakeResponse {
	return intakeResponse{Status: status, Body: gin.H{
		"ok":      false,
		"error":   code,
		"message": message,
		"details": details,
	}}
}

// validationRejection maps a typed cross-field validation failure to its 422
// envelope (details key always present, matching the historical wire shape).
func validationRejection(vErr *SubmitJobValidationError) intakeResponse {
	return rejectWithDetails(http.StatusUnprocessableEntity, vErr.Code, vErr.Message, vErr.Details)
}

// ssrfValidationErrorEnvelope maps the SSRF policy rejections to their 422
// envelope (P1 admin-audit trail step #2).
func ssrfValidationErrorEnvelope(errs []SSRFValidationError) (int, gin.H) {
	details := make([]gin.H, 0, len(errs))
	for _, e := range errs {
		details = append(details, gin.H{"path": e.Path, "url": e.URL, "reason": e.Reason})
	}
	return http.StatusUnprocessableEntity, gin.H{
		"ok":      false,
		"error":   "ssrf_rejected",
		"message": "one or more external URLs failed the egress policy",
		"details": details,
	}
}

// quotaErrorEnvelope maps a per-request quota failure to its HTTP envelope.
// A *QuotaError is a 429 with machine-readable reason/observed/cap; anything
// else is an internal failure resolving the quota itself.
func quotaErrorEnvelope(qerr error) (int, gin.H) {
	var qe *QuotaError
	if errors.As(qerr, &qe) {
		return http.StatusTooManyRequests, gin.H{
			"ok":      false,
			"error":   "m2m_quota_exceeded",
			"message": qe.Error(),
			"details": gin.H{
				"reason":   qe.Reason,
				"observed": qe.Observed,
				"cap":      qe.Cap,
			},
		}
	}
	return http.StatusInternalServerError, gin.H{
		"ok":      false,
		"error":   "m2m_quota_failure",
		"message": qerr.Error(),
	}
}
