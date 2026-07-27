package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
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

		// Derive Creator-compatible identity. The provider is a
		// LOW-CARDINALITY constant instead of a per-key synthesis
		// ("api_" + key-prefix) so dashboards and security audits
		// can aggregate jobs by their real source rather than
		// stumble over a new label per request.
		sourceProvider := ExternalAPISourceProvider
		sourceJobID := req.IdempotencyKey
		targetExecutorID := JobSubmitTargetExecutorID

		// Build the worker payload from the simplified request.
		workerPayload := buildWorkerPayloadFromSubmit(&req)

		// Delegate to the same resolver used by CreatorPush.
		forwarded, err := h.resolveCompletedPayload(
			c.Request.Context(),
			sourceProvider,
			sourceJobID,
			targetExecutorID,
			workerPayload,
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
			logHashIdempotencyKey(req.IdempotencyKey),
			jobID,
		)

		c.JSON(http.StatusAccepted, response)
	}
}

// buildWorkerPayloadFromSubmit converts a SubmitJobRequest into the
// map[string]interface{} shape that the resolver expects.
func buildWorkerPayloadFromSubmit(req *SubmitJobRequest) map[string]interface{} {
	m := map[string]interface{}{
		"status": "completed",
		"job_id": req.IdempotencyKey,
	}

	if req.VideoName != "" {
		m["video_name"] = req.VideoName
	}
	if req.ScriptText != "" {
		m["script_text"] = req.ScriptText
	}
	if len(req.VoiceoverPaths) > 0 {
		m["voiceover_paths"] = req.VoiceoverPaths
	}

	if len(req.Scenes) > 0 {
		scenes := make([]interface{}, 0, len(req.Scenes))
		for _, s := range req.Scenes {
			scene := map[string]interface{}{
				"text":            s.Text,
				"duration_seconds": s.DurationSeconds,
			}
			if s.ClipLink != "" {
				scene["clip_link"] = s.ClipLink
			}
			if s.ImageLink != "" {
				scene["image_link"] = s.ImageLink
			}
			scenes = append(scenes, scene)
		}
		m["scenes"] = scenes
	}

	if len(req.DeliveryPlan) > 0 {
		plan := make([]interface{}, 0, len(req.DeliveryPlan))
		for _, d := range req.DeliveryPlan {
			entry := map[string]interface{}{
				"destination_id": d.DestinationID,
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

// logHashIdempotencyKey returns the first 12 hex characters of the
// SHA-256 digest of an idempotency_key. We use this hash in
// operator-facing log lines (API_V1_JOBS_ACCEPTED, ERROR, audit trail)
// instead of the raw key because:
//
//   - The raw key can carry user-supplied identifiers (email addresses,
//     customer IDs, accidental bearer tokens) that we DO NOT want in
//     journald / Loki / CloudWatch output.
//
//   - The key can carry unusual Unicode that breaks terminal log
//     printers. A 12-hex-char ASCII hash is always safe to print.
//
//   - The full key remains the source-of-truth dedup identifier in the
//     forwarding row (cf.source_job_id) and in the database. Operators
//     who need to look up a specific request can run
//     `SELECT * FROM creator_forwardings WHERE source_job_id = ?` —
//     they do not need the key in stdout.
//
// The 12-hex-char prefix is the same compact form used by
// creatorflow.shaShort in the [P0 #3] payload-hash work, so log
// operators see a consistent format across both log sources.
//
// Truncating a SHA-256 to 12 hex chars leaves 48 bits of entropy —
// ample to distinguish concurrent jobs in practice but NOT a security
// guarantee against brute-force. It is purely an operator-UX choice.
func logHashIdempotencyKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}
