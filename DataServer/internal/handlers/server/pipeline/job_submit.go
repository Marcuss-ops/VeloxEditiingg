package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
)

// ExternalAPISourceProvider is the canonical SourceProvider stamped on
// forwardings produced by POST /api/v1/jobs. A constant here guarantees
// the provider dimension stays low-cardinality: every external job
// aggregates under one provider label, so dashboards can group them
// and security audits can detect cross-job correlation attempts
// without scanning for high-cardinality values.
//
// The earlier implementation synthesised the provider from each
// incoming idempotency_key ("api_" + key-prefix), which produced a
// new provider per job, broke aggregation, and risked Unicode
// truncation mid-rune at the 32-byte boundary. A fixed provider also
// lets the runner honour a future "client_id" extension without
// touching the schema contract again.
const ExternalAPISourceProvider = "external_api"

// JobSubmitTargetExecutorID is the canonical executor that POST /api/v1/jobs
// requests dispatch to. It is the same as Creator-push's default so the
// same worker pool services both intake paths.
const JobSubmitTargetExecutorID = "scene.composite.v1"

// MaxVideoNameBytes is the byte-length cap on SubmitJobRequest.video_name.
// The cap protects log-line printers (some wire up to 4096-byte lines)
// and prevents accidental "paste the entire script in the title" classes
// of misuse. Picked to comfortably fit real video titles; raise only
// with operator approval.
//
// MUST mirror openapi.yaml:SubmitJobRequest.video_name.maxLength.
// Drift here silently breaks the contract: clients receive 422 for
// names the spec claimed were valid.
const MaxVideoNameBytes = 300

// MaxScenes is the upper bound on SubmitJobRequest.scenes count. The
// number is a defensive ceiling — a real-world video composition is
// rarely more than a few hundred scenes, so any larger count is
// almost certainly a misconfigured client. Anything above this bound
// is rejected with 422 invalid_payload to make the resource cost
// explicit (instead of letting it silently O(N) the resolver).
//
// MUST mirror openapi.yaml:SubmitJobRequest.scenes.maxItems.
// Drift here causes a silent acceptance/rejection asymmetry: clients
// sending a number of scenes spec-permitted but Go-rejected (or vice
// versa) get a confusing 422 on a structurally valid request.
const MaxScenes = 10000

// MinSceneDurationSeconds is the lower bound on SubmitScene.duration_seconds.
// Per the OpenAPI contract (and the validation helper below) sub-second
// durations are supported; zero or negative is rejected.
//
// MUST mirror openapi.yaml:SubmitScene.duration_seconds.minimum.
// Drift breaks sub-second scene cuts, an explicit feature for
// fine-grained montage work the Worker supports.
const MinSceneDurationSeconds = 0.1

// MaxSceneDurationSeconds is the upper bound, expressed in seconds.
// 86,400 s == 24 h, a generous ceiling that catches runaway defects
// (a client accidentally setting DurationSeconds = 1e10) without
// blocking legitimate full-day timelapses.
//
// MUST mirror openapi.yaml:SubmitScene.duration_seconds.maximum.
// Drift lets buggy clients silently inflate server-side resource
// budgets (a 1e10-second scene never finishes a paint cycle).
const MaxSceneDurationSeconds = 86400.0

// DefaultRetryBudget is the value stamped on a delivery_plan entry
// whose pointer is nil — i.e., the client did not declare a
// retry_budget and accepts the OpenAPI default. Mirrors
// openapi.yaml:SubmitDeliveryPlanEntry.retry_budget.default.
//
// MUST change in lockstep with the spec — if openapi.yaml's default
// changes, this constant changes. A divergence silently breaks
// the omitted-RetryBudget round-trip contract (clients that omit
// retry_budget get a different value than the spec advertises).
const DefaultRetryBudget = 3

// SubmitJobRequest is the simplified, versioned API contract for
// POST /api/v1/jobs. It allows external systems to submit complete
// video jobs without going through the Creator intermediary.
//
// The format is intentionally flat and intuitive:
//   - No nested "payload" envelope
//   - No source_provider/source_job_id/target_executor_id ceremony
//   - idempotency_key is the single dedup handle
//
// The system derives the Creator-compatible identity tuple
// (source_provider, source_job_id, target_executor_id) automatically.
//
// Field validation rules — every contract on this struct mirrors
// fields on openapi.yaml's SubmitJobRequest schema. The handler uses
// strict JSON decoding (DisallowUnknownFields) + ValidateSubmitJobRequest
// (the canonical programmatic validator) so the schema and the Go
// code can stay in lockstep without relying on Gin binding tags,
// which silently default to permissive behaviour for missing
// cross-field constraints.
type SubmitJobRequest struct {
	// IdempotencyKey is required. 1..128 bytes after UTF-8 trim, valid
	// UTF-8, no control bytes, no ':' or '%' separators. See
	// ValidateIdempotencyKey in idempotency_validation.go for the
	// byte-level rules and rejection envelopes.
	IdempotencyKey string `json:"idempotency_key"`

	// VideoName is the display name for the resulting video. Capped
	// at MaxVideoNameBytes (300); empty allowed.
	VideoName string `json:"video_name,omitempty"`

	// ScriptText is the plain-text script used for TTS / overlay.
	// Content field — NOT trimmed. Empty allowed. No byte-length cap
	// here; matches the creator path's tolerance.
	ScriptText string `json:"script_text,omitempty"`

	// VoiceoverPaths are voiceover audio references. Each entry MUST
	// be a velox-asset:// URI or a fully-qualified reachable URL.
	VoiceoverPaths []string `json:"voiceover_paths,omitempty"`

	// Scenes is the scene list. Each scene drives one composited
	// segment. At least one scene is required; max MaxScenes (10k).
	Scenes []SubmitScene `json:"scenes"`

	// Layers are independent overlays: title, name, important phrase or
	// additional media. They are not folded into Scenes, so callers can
	// submit a video, images and any combination of overlays together.
	Layers []SubmitLayer `json:"layers,omitempty"`

	// SubtitleTracks are independent from visual layers and media.
	SubtitleTracks []SubmitSubtitleTrack `json:"subtitle_tracks,omitempty"`

	// DeliveryPlan is the ordered list of delivery targets. Empty
	// allowed (defaults to scene.composite.v1's default resolver).
	DeliveryPlan []SubmitDeliveryPlanEntry `json:"delivery_plan,omitempty"`
}

// SubmitScene is a single scene in the simplified job submission format.
//
// Field validation rules — submitted scenes MUST satisfy:
//   - Text non-empty (string length > 0 after trim).
//   - DurationSeconds in [MinSceneDurationSeconds,
//     MaxSceneDurationSeconds] (i.e. [0.1, 86400] seconds).
//
// ValidateSubmitJobRequest (in this file) runs the per-scene check
// and aggregates failures into a single 422 with details pointing at
// the offending index.
type SubmitScene struct {
	// Text is the narration / overlay text for this scene. Must be
	// non-empty after trim.
	Text string `json:"text"`

	// ClipLink is a velox-asset:// clip URI or reachable URL.
	ClipLink string `json:"clip_link,omitempty"`

	// ImageLink is an optional image fallback.
	ImageLink string `json:"image_link,omitempty"`

	// DurationSeconds is the intended duration of the scene. Must be
	// in [MinSceneDurationSeconds, MaxSceneDurationSeconds].
	DurationSeconds float64 `json:"duration_seconds"`
}

// SubmitLayer is the API representation of one independent Chronon layer.
// Type is one of text, image, video or color; Role can distinguish title,
// name and important_phrase without creating separate renderer paths.
type SubmitLayer struct {
	ID              string    `json:"id"`
	Type            string    `json:"type"`
	Role            string    `json:"role,omitempty"`
	Text            string    `json:"text,omitempty"`
	Asset           string    `json:"asset,omitempty"`
	Source          string    `json:"source,omitempty"`
	Font            string    `json:"font,omitempty"`
	FontSize        float64   `json:"font_size,omitempty"`
	Position        []float64 `json:"position,omitempty"`
	StartSeconds    float64   `json:"start_seconds,omitempty"`
	DurationSeconds float64   `json:"duration_seconds,omitempty"`
	Preset          string    `json:"preset,omitempty"`
	Animation       string    `json:"animation,omitempty"`
}

// SubmitSubtitleTrack is a separate subtitle API payload. SRT, VTT and
// Chronon-compatible JSON sources are supported by the renderer.
type SubmitSubtitleTrack struct {
	Source string `json:"source"`
	Preset string `json:"preset,omitempty"`
	Font   string `json:"font,omitempty"`
}

// SubmitDeliveryPlanEntry is a single destination in the delivery plan.
//
// Field validation rules — submitted entries MUST satisfy:
//   - DestinationID non-empty (string length > 0 after trim).
//   - RetryBudget is a POINTER so that an explicit client-supplied value
//     of 0 round-trips distinctly from "field omitted". A nil pointer
//     means "client did not specify" — submitRequestToRawPayload
//     substitutes the OpenAPI default (DefaultRetryBudget = 3) at
//     normalization time. A pointer-to-0 means "client explicitly
//     wants 0 retries" — preserved verbatim into the worker payload.
//     Without the *int pointer, the Go default for int (0) would
//     silently merge with the omitted-field default and clients could
//     not distinguish "0 explicitly" from "omitted".
type SubmitDeliveryPlanEntry struct {
	DestinationID string `json:"destination_id"`
	Priority      int    `json:"priority,omitempty"`
	// RetryBudget is *int so that an explicit JSON value 0 round-trips
	// distinctly from the omitted case (nil). See the type doc for
	// the contract.
	RetryBudget *int `json:"retry_budget,omitempty"`
	Metadata    any  `json:"metadata,omitempty"`
}

// SubmitJobValidationError is the structured 4xx envelope returned by
// ValidateSubmitJobRequest when one (or more) of the cross-field
// constraints on SubmitJobRequest / SubmitScene / SubmitDeliveryPlanEntry
// fail. Designed for human-readable error reporting AND machine-
// readable code dispatch:
//
//   - Code is "invalid_payload" (always; the cross-field validators
//     are semantic, not byte-level).
//   - Reason is a short singular noun (e.g. "video_name_too_long",
//     "scenes_too_many", "scene_text_empty", "scene_duration_range",
//     "delivery_destination_empty").
//   - Details is the canonical {path, issue, ...} shape: path is the
//     JSON-pointer style dotted path to the offending field, issue
//     is a machine-readable short token.
//
// The handler maps every validation error to 422 with the error envelope
// (per openapi.yaml's ErrorEnvelope shape). One invalid scene out of
// 100 produces ONE error with details.path = "scenes.47.duration_seconds"
// — the handler emits 422 with all violations baked into details[] so
// the client sees the full picture in one round trip.
//
// Idempotency-key byte-level errors (e.g. too long, contains ":") are
// reported by ValidateIdempotencyKey separately (see
// idempotency_validation.go and SubmitJob() handler).
type SubmitJobValidationError struct {
	Code    string
	Reason  string
	Message string
	Details []gin.H
}

// Error implements the error interface so the helper is composable
// with `return err`.
func (e *SubmitJobValidationError) Error() string {
	return fmt.Sprintf("submit_job_validation: %s: %s (details=%d)", e.Code, e.Reason, len(e.Details))
}

// ValidateSubmitJobRequest is the canonical programmatically-invoked
// validator for SubmitJobRequest. It runs AFTER the strict JSON decoder
// has populated the struct, so every field is present (or zero-value)
// by the time this runs. It is the single source of truth for the
// [P1] OpenAPI-aligned cross-field constraints; the OpenAPI Python
// validator script confirms the same set of constraints at the spec
// level.
//
// The function accumulates ALL field-level violations into a single
// SubmitJobValidationError so the client can correct them in one round
// trip ("fix these 4 fields and resubmit") rather than learning about
// them one at a time across multiple requests.
//
// All bool-returned-from-validation paths return false on the happy
// path so the handler can write `if verr, bad := …; bad { ... }`.
//
// ValidateIdempotencyKey runs separately at the byte level (it is
// invoked BEFORE this helper at the handler level so a cheap byte-level
// rejected request never has its body fully traversed here).
func ValidateSubmitJobRequest(req SubmitJobRequest) (*SubmitJobValidationError, bool) {
	var details []gin.H

	// VideoName byte-length cap. Trimmed to match canonical identity-
	// field trim policy in submitRequestToRawPayload (TrimSpace).
	if v := strings.TrimSpace(req.VideoName); len(v) > MaxVideoNameBytes {
		details = append(details, gin.H{
			"path":     "video_name",
			"issue":    "max_length",
			"max":      MaxVideoNameBytes,
			"observed": len(v),
		})
	}

	// Scenes count: at least 1, at most MaxScenes. Empty or oversized
	// is a hard fail.
	if len(req.Scenes) == 0 {
		details = append(details, gin.H{
			"path":  "scenes",
			"issue": "empty",
		})
	} else if len(req.Scenes) > MaxScenes {
		details = append(details, gin.H{
			"path":     "scenes",
			"issue":    "max_items",
			"max":      MaxScenes,
			"observed": len(req.Scenes),
		})
	}

	// Per-scene validation: text non-empty, duration in [0.1, 86400].
	for i, s := range req.Scenes {
		pathPrefix := fmt.Sprintf("scenes.%d", i)
		if strings.TrimSpace(s.Text) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".text",
				"issue": "empty",
			})
		}
		if s.DurationSeconds < MinSceneDurationSeconds || s.DurationSeconds > MaxSceneDurationSeconds {
			details = append(details, gin.H{
				"path":     pathPrefix + ".duration_seconds",
				"issue":    "out_of_range",
				"min":      MinSceneDurationSeconds,
				"max":      MaxSceneDurationSeconds,
				"observed": s.DurationSeconds,
			})
		}
	}

	// Per-delivery-plan-entry validation: destination_id non-empty
	// after trim. RetryBudget has NO upper bound at the OpenAPI layer
	// (only "minimum: 0"); allowing 0 is the whole point of the *int
	// change so the explicit zero-round-trip contract holds.
	for i, d := range req.DeliveryPlan {
		pathPrefix := fmt.Sprintf("delivery_plan.%d", i)
		if strings.TrimSpace(d.DestinationID) == "" {
			details = append(details, gin.H{
				"path":  pathPrefix + ".destination_id",
				"issue": "empty",
			})
		}
	}

	if len(details) == 0 {
		return nil, false
	}

	return &SubmitJobValidationError{
		Code:    "invalid_payload",
		Reason:  "validation_failed",
		Message: fmt.Sprintf("request body has %d validation failure(s) (see details)", len(details)),
		Details: details,
	}, true
}

// SubmitJob handles POST /api/v1/jobs.
//
// This is the simplified, external-friendly entry point for job
// submission. It converts the flat request into the Creator-push
// format and delegates to the same resolver machinery.
//
// The identity tuple is derived from the idempotency_key with stable
// cardinality:
//   - source_provider:    ExternalAPISourceProvider ("external_api")
//   - source_job_id:      the (validated) idempotency_key
//   - target_executor_id: JobSubmitTargetExecutorID ("scene.composite.v1")
//
// Stable provider cardinality is a deliberate invariant: it lets
// dashboards aggregate "all external_api jobs" without scanning
// per-key labels, and keeps future M2M-auth client_ids additive
// rather than redefining the provider dimension.
//
// Decoding strategy: the handler uses json.NewDecoder(...).Decode
// with DisallowUnknownFields — NOT c.ShouldBindJSON. The strict
// decoder rejects any field name not on the struct, so a typo'd
// json blob (e.g. {"ideliverency_key": "...", ...}) fails with 400
// invalid_json BEFORE we touch downstream code. Gin's binding tag
// machinery is permissive for cross-field validation and silently
// accepts unknown fields, which is the wrong default for an
// external-API surface.
func (h *Handlers) SubmitJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		dec := json.NewDecoder(c.Request.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			// Drain the request body so the unread tail does not
			// corrupt subsequent requests on the same HTTP/1.1
			// keepalive connection. A small body in practice, but
			// a long-lived automation client can fold many
			// requests onto one TCP stream where leftover bytes
			// from a 400 would otherwise mix with the next request.
			_, _ = io.Copy(io.Discard, c.Request.Body)
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
			details := gin.H{"path": "idempotency_key"}
			if vErr.Reason != "" {
				details["reason"] = vErr.Reason
			}
			if vErr.FieldLength != nil {
				details["length"] = *vErr.FieldLength
			}
			if vErr.FieldByteOff != nil {
				details["byte_offset"] = *vErr.FieldByteOff
			}
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   vErr.Code,
				"message": vErr.Message,
				"details": details,
			})
			return
		}

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

		// Delivery-destination existence pre-flight (P0 #2 closure):
		// every unique non-empty delivery_plan[N].destination_id
		// MUST resolve to an enabled row in delivery_destinations.
		// Runs AFTER ValidateSubmitJobRequest so we don't query the
		// store for bodies that already failed shape rules, and
		// BEFORE SSRF / quota so the rejection paths are order-stable:
		// a missing destination is a 422 invalid_payload (this
		// gate), not a 500 from inside AtomicForwardAndEnqueue
		// (where validateDeliveryDestinationTx currently emits a
		// plaintext "destination_id %q does not exist" error that
		// WriteResolverError cannot map). Mirrors the policy of
		// store.validateDeliveryDestinationTx so the handler-side
		// and tx-side checks agree on existence + enabled=1.
		//
		// aggregates ALL destination-existence violations into a
		// single 422 with details[].path = "delivery_plan.N.destination_id"
		// (same path shape used by enqueue's *validationError).
		// Fail-closed on store failure (500 store_failure).
		if len(req.DeliveryPlan) > 0 {
			ids := make([]string, 0, len(req.DeliveryPlan))
			for _, d := range req.DeliveryPlan {
				if tid := strings.TrimSpace(d.DestinationID); tid != "" {
					ids = append(ids, tid)
				}
			}
			if len(ids) > 0 {
				exist, qerr := h.store.BatchDeliveryDestinationsExistAndEnabled(
					c.Request.Context(), ids,
				)
				if qerr != nil {
					c.JSON(http.StatusInternalServerError, gin.H{
						"ok":      false,
						"error":   "store_failure",
						"message": "failed to resolve delivery destinations: " + qerr.Error(),
					})
					return
				}
				var destDetails []gin.H
				for i, d := range req.DeliveryPlan {
					tid := strings.TrimSpace(d.DestinationID)
					if tid == "" {
						// empty destination_id is caught upstream by
						// ValidateSubmitJobRequest; skip here so we
						// don't emit a misleading duplicate entry.
						continue
					}
					if ok, present := exist[tid]; !present || !ok {
						destDetails = append(destDetails, gin.H{
							"path":  fmt.Sprintf("delivery_plan.%d.destination_id", i),
							"issue": "invalid",
						})
					}
				}
				if len(destDetails) > 0 {
					c.JSON(http.StatusUnprocessableEntity, gin.H{
						"ok":      false,
						"error":   "invalid_payload",
						"message": fmt.Sprintf("request body has %d validation failure(s) (see details)", len(destDetails)),
						"details": destDetails,
					})
					return
				}
			}
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
		canonical := NormalizeExternalJobSubmission(req)

		// Delegate to the same resolver used by CreatorPush.
		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			canonical.SourceProvider,
			canonical.SourceJobID,
			canonical.TargetExecutorID,
			canonical.WorkerPayload,
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
//   job_id      canonical id (the same string POST returned).
//   status      jobs.Status (canonical render state) — falls back to
//               forwarding.Status when the jobs row has not materialized
//               yet (resolver race in pre-FORWARDING state: the row
//               exists but target_job_id was not yet committed).
//   created     bool — true if the row was produced by POST /api/v1/jobs
//               (source_provider == ExternalAPISourceProvider). False
//               if it came in via POST /api/v1/creator/jobs. The
//               indicator lets clients distinguish the two intake
//               paths without a separate lookup.
//   status_url  env-relative path "/api/v1/jobs/{job_id}" so clients
//               can chain the canonical into their next request
//               without re-deriving the URL.
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
func NormalizeExternalJobSubmission(req SubmitJobRequest) *CanonicalCompletedPayload {
	rawPayload := submitRequestToRawPayload(&req)

	dto, _ := remoteengine.ParseRemotePipelineResult(rawPayload)
	workerPayload := dto.ToWorkerPayload()

	return &CanonicalCompletedPayload{
		SourceProvider:   ExternalAPISourceProvider,
		SourceJobID:      req.IdempotencyKey,
		TargetExecutorID: JobSubmitTargetExecutorID,
		WorkerPayload:    workerPayload,
	}
}

// submitRequestToRawPayload builds the canonical flat-map shape that
// remoteengine.ParseRemotePipelineResult consumes. Mirrors the wire
// shape documented at DataServer/api/openapi.yaml under
// `CreatorPushPayload` — same key names (snake_case, alias paths
// collapsed to canonical) the typed DTO expects.
//
// The map is a one-shot invariant: it is the boundary between the
// Submit-scoped typed structs (SubmitScene, SubmitDeliveryPlanEntry)
// and the remoteengine-typed DTO (RemotePipelineResult). Everything
// downstream of this point sees only the canonical envelope.
//
// RetryBudget boundary contract:
//   - d.RetryBudget == nil              → entry["retry_budget"] = DefaultRetryBudget
//     (the OpenAPI default of 3; mirrors what an omitted int field
//     would have meant historically).
//   - d.RetryBudget != nil, *d == 0    → entry["retry_budget"] = 0
//     (client explicitly chose zero retries; preserved verbatim so
//     downstream enqueue-layer validation can distinguish "0
//     explicitly" from "omitted").
//   - d.RetryBudget != nil, *d > 0     → entry["retry_budget"] = *d
//     (clamp unchanged; a future enrichment could enforce an upper
//     bound here without touching the boundary contract).
//
// Trim policy (matching NormalizeExternalJobSubmission's contract):
// identity-bearing fields are trimmed (IdempotencyKey, VideoName,
// URL-shaped scene clip_link / image_link, delivery destination_id).
// CONTENT fields (ScriptText, scene text) are passed through verbatim
// because legitimate whitespace might appear.
func submitRequestToRawPayload(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		"status": "completed",
		"job_id": strings.TrimSpace(req.IdempotencyKey),
	}

	if req.VideoName != "" {
		m["video_name"] = strings.TrimSpace(req.VideoName)
	}
	if req.ScriptText != "" {
		m["script_text"] = req.ScriptText
	}
	if len(req.VoiceoverPaths) > 0 {
		// NormalizeToStrings shape matches what
		// extractVoiceoverPathsDTO scans for.
		m["voiceover_paths"] = req.VoiceoverPaths
	}

	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for _, s := range req.Scenes {
			scene := map[string]interface{}{
				"text":             s.Text,
				"duration_seconds": s.DurationSeconds,
			}
			if s.ClipLink != "" {
				scene["clip_link"] = strings.TrimSpace(s.ClipLink)
			}
			if s.ImageLink != "" {
				scene["image_link"] = strings.TrimSpace(s.ImageLink)
			}
			scenes = append(scenes, scene)
		}
		m["scenes"] = scenes
	}
	if len(req.Layers) > 0 {
		layers := make([]interface{}, 0, len(req.Layers))
		for _, layer := range req.Layers {
			entry := map[string]interface{}{"id": strings.TrimSpace(layer.ID), "type": strings.TrimSpace(layer.Type)}
			for key, value := range map[string]interface{}{
				"role": layer.Role, "text": layer.Text, "asset": layer.Asset, "source": layer.Source,
				"font": layer.Font, "font_size": layer.FontSize, "position": layer.Position,
				"start_seconds": layer.StartSeconds, "duration_seconds": layer.DurationSeconds,
				"preset": layer.Preset, "animation": layer.Animation,
			} {
				if value != nil && value != "" && !(key == "position" && len(layer.Position) == 0) && !(key != "position" && value == float64(0)) {
					entry[key] = value
				}
			}
			layers = append(layers, entry)
		}
		m["layers"] = layers
	}
	if len(req.SubtitleTracks) > 0 {
		subtitles := make([]interface{}, 0, len(req.SubtitleTracks))
		for _, track := range req.SubtitleTracks {
			subtitles = append(subtitles, map[string]interface{}{"source": strings.TrimSpace(track.Source), "preset": track.Preset, "font": track.Font})
		}
		m["subtitle_tracks"] = subtitles
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": strings.TrimSpace(d.DestinationID),
			}
			if d.Priority > 0 {
				entry["priority"] = d.Priority
			}
			if d.RetryBudget == nil {
				entry["retry_budget"] = DefaultRetryBudget
			} else {
				entry["retry_budget"] = *d.RetryBudget
			}
			if d.Metadata != nil {
				entry["metadata"] = d.Metadata
			}
			plan = append(plan, entry)
		}
		m["delivery_plan"] = plan
	}

	return m
}