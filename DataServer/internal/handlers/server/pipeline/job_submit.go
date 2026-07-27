package pipeline

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/remoteengine"
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
type SubmitJobRequest struct {
	// IdempotencyKey is required. Two requests with the same key
	// converge on the same Velox job (idempotent).
	//
	// Validated below in ValidateIdempotencyKey: 1..128 bytes, valid
	// UTF-8, no control bytes or forbidden separators (':' or '%').
	IdempotencyKey string `json:"idempotency_key" binding:"required"`

	// VideoName is the display name for the resulting video.
	VideoName string `json:"video_name"`

	// ScriptText is the plain-text script used for TTS / overlay.
	ScriptText string `json:"script_text"`

	// VoiceoverPaths are voiceover audio references. Each entry MUST
	// be a velox-asset:// URI or a fully-qualified reachable URL.
	VoiceoverPaths []string `json:"voiceover_paths"`

	// Scenes is the scene list. Each scene drives one composited segment.
	Scenes []SubmitScene `json:"scenes"`

	// DeliveryPlan is the ordered list of delivery targets.
	DeliveryPlan []SubmitDeliveryPlanEntry `json:"delivery_plan"`
}

// SubmitScene is a single scene in the simplified job submission format.
type SubmitScene struct {
	// Text is the narration / overlay text for this scene.
	Text string `json:"text"`

	// ClipLink is a velox-asset:// clip URI or reachable URL.
	ClipLink string `json:"clip_link,omitempty"`

	// ImageLink is an optional image fallback.
	ImageLink string `json:"image_link,omitempty"`

	// DurationSeconds is the intended duration of the scene.
	DurationSeconds float64 `json:"duration_seconds"`
}

// SubmitDeliveryPlanEntry is a single destination in the delivery plan.
type SubmitDeliveryPlanEntry struct {
	DestinationID string `json:"destination_id"`
	Priority      int    `json:"priority,omitempty"`
	RetryBudget   int    `json:"retry_budget,omitempty"`
	Metadata      any    `json:"metadata,omitempty"`
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
func (h *Handlers) SubmitJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "invalid_json",
				"message": "request body must be valid JSON",
			})
			return
		}

		// Idempotency-key validation: 1..128 valid UTF-8 bytes with no
		// control chars or forbidden separators (':' or '%'). The
		// helper trims whitespace before validating, so the canonical
		// (post-trim) form is what reaches the resolver as source_job_id.
		// A typed *IdempotencyKeyError carries machine-readable reason
		// + diagnostics so the API envelope is actionable.
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

		if len(req.Scenes) == 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"ok":      false,
				"error":   "invalid_payload",
				"message": "at least one scene is required",
			})
			return
		}

		for i, scene := range req.Scenes {
			if scene.DurationSeconds <= 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{
					"ok":      false,
					"error":   "invalid_payload",
					"message": fmt.Sprintf("scenes[%d].duration_seconds must be > 0", i),
				})
				return
			}
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
			// P0 #2 contract: every resolver-layer error is mapped
			// to the canonical HTTP envelope by the shared helper
			// in package creatorflow. Previously this branch was
			// missing the enqueue.ValidationErrorField mapping
			// entirely, so any enqueue-layer validation error
			// (missing delivery_plan entry, missing retry_budget,
			// malformed destination_id, …) silently downgraded
			// to a 500.
			creatorflow.WriteResolverError(c, err, "idempotency_key")
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

		h.intakeSinkOrNoop().IncAccepted("api_v1_jobs")
		jobID, _ := response["job_id"].(string)
		pipelineLog(
			"API_V1_JOBS_ACCEPTED idem_hash=%s job_id=%s",
			logHashShort(req.IdempotencyKey),
			jobID,
		)

		c.JSON(http.StatusAccepted, response)
	}
}

// NormalizeExternalJobSubmission is the canonical typed-DTO adapter
// for POST /api/v1/jobs. It walks the SAME path that creator_push
// walks (creator_push.go's normalizeCreatorPushRequest):
//
//  1. Build a flat raw map mirroring the wire shape that
//     remoteengine.ParseRemotePipelineResult consumes (status,
//     job_id, video_name, script_text, voiceover_paths, scenes[],
//     delivery_plan[]).
//
//  2. Pass it through remoteengine.ParseRemotePipelineResult to
//     produce the typed RemotePipelineResult DTO. This is the single
//     point where validation (status, scene shape, voiceover shape,
//     metadata) happens — there is no hand-rolled string-key lookup
//     anymore.
//
//  3. Call (*RemotePipelineResult).ToWorkerPayload() which:
//     - base-copies fields from the flat raw map (delivery_plan,
//       output_path, non-DTO passthroughs),
//     - overlays typed DTO fields (job_id from RemoteJobID,
//       video_name from Script.Title, scenes_json from Scenes,
//       voiceover_paths from Voiceover.Paths).
//
//  4. Stamp the stable identity tuple:
//     - source_provider    = ExternalAPISourceProvider (constant,
//       low-cardinality — see #P0 #4 audit for the rationale).
//     - source_job_id      = req.IdempotencyKey (the only client-
//       supplied identity element on this path).
//     - target_executor_id = JobSubmitTargetExecutorID (constant).
//
// Returns *CanonicalCompletedPayload (alias for normalizedCreatorPush,
// the type CreatorPush's path also returns) so a future third
// producer (e.g., webhook intake) only has to return the same shape.
//
// Validation errors are surfaced as typed errors that the handler maps
// to 422 invalid_payload. Notably this consolidates a previous
// duplicate inline-completeness check (scenes > 0, duration > 0) into
// the typed DTO's NormalizeExternalJobSubmission caller — the
// SubmitJob() handler still does its OWN belt-and-braces inline check
// because ParseRemotePipelineResult's incomplete-payload handling
// runs INSIDE the resolver entry point, not at the normalization
// boundary.
//
// NormalizeExternalJobSubmission does NOT return an error in the
// current implementation: submitRequestToRawPayload always produces
// a non-nil raw map (the only failure mode in ParseRemotePipelineResult
// is "raw == nil", which we control). Semantic validation (scenes > 0,
// durations > 0, idempotency_key valid) is the SubmitJob() handler's
// job — it runs BEFORE NormalizeExternalJobSubmission as the early-
// rejection boundary so a malformed request never reaches the typed
// DTO path. Normalize is the canonical SHAPE transformation; the
// handler is the canonical VALIDATION boundary.
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

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": strings.TrimSpace(d.DestinationID),
			}
			if d.Priority > 0 {
				entry["priority"] = d.Priority
			}
			if d.RetryBudget > 0 {
				entry["retry_budget"] = d.RetryBudget
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
