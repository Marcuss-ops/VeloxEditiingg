package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/taskgraph"
)

// FinalizeJobRequest is the runtime half of the same job created by
// POST /api/v1/jobs/pre. runtime_payload is intentionally extensible so 77
// can add overlay, TTS, BGM and SFX fields without introducing parallel
// resolver/cache infrastructure. overlays stays typed because its exact
// [start_frame,end_frame) contract is part of the public API.
type FinalizeJobRequest struct {
	IdempotencyKey string                   `json:"idempotency_key"`
	Overlays       []SubmitOverlay          `json:"overlays,omitempty"`
	RuntimeAssets  []map[string]interface{} `json:"runtime_assets,omitempty"`
	RuntimePayload map[string]interface{}   `json:"runtime_payload,omitempty"`
}

// PrepareJob handles the first stage of the two-stage intake. It uses the
// canonical submission core, so delivery validation, asset normalization and
// job idempotency remain identical to POST /api/v1/jobs.
func (h *Handlers) PrepareJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_json", "message": "request body must be valid JSON without unknown fields: " + err.Error()})
			return
		}
		req.RuntimeAssetsPending = !runtimeAssetsCompleteForPrepare(req)
		out := h.submitJobCore(c.Request.Context(), req, intakeIdentity{
			ClientID:     ClientIDFromContext(c),
			IntakeSource: creatorflow.IntakeSourceCanonical,
			Quota:        KeyFromContext(c),
		})
		if out.Status < http.StatusMultipleChoices {
			out.Body["phase"] = "PREPARE"
			if req.RuntimeAssetsPending {
				out.Body["dispatch_status"] = "waiting_runtime_assets"
			} else {
				out.Body["dispatch_status"] = "prefetch_queued"
			}
		}
		writeIntakeResponse(c, out)
	}
}

func runtimeAssetsCompleteForPrepare(req SubmitJobRequest) bool {
	complete := len(req.RuntimeAssets) > 0
	if runtimePayloadHasNonEmptyAssets(req.RuntimePayload) {
		complete = true
	}
	if req.RuntimeAssetsComplete != nil {
		complete = *req.RuntimeAssetsComplete
	}
	return complete
}

func runtimePayloadHasNonEmptyAssets(payload map[string]interface{}) bool {
	value, ok := payload["runtime_assets"]
	if !ok || value == nil {
		return false
	}
	switch assets := value.(type) {
	case []interface{}:
		return len(assets) > 0
	case []map[string]interface{}:
		return len(assets) > 0
	default:
		return false
	}
}

// FinalizeJob applies runtime assets to the already persisted task and then
// re-drives FutureAssetPlan on the reservation owner. It never calls Enqueue.
func (h *Handlers) FinalizeJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req FinalizeJobRequest
		if err := decodeStrictJSON(c.Request.Body, &req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_json", "message": "request body must be valid JSON without unknown fields: " + err.Error()})
			return
		}
		req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
		if vErr, bad := ValidateIdempotencyKey(req.IdempotencyKey); bad {
			status, body := idempotencyKeyErrorEnvelope(vErr)
			c.JSON(status, body)
			return
		}
		jobID := strings.TrimSpace(c.Param("id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required", "message": "URL path /api/v1/jobs/:id/finalize requires non-empty :id"})
			return
		}
		if len(req.Overlays) > 0 {
			if details := validateSubmitOverlays(req.Overlays); len(details) > 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": "overlay windows are invalid", "details": details})
				return
			}
		}

		taskReader := h.jobs.TaskReader
		if taskReader == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "task_store_unavailable", "message": "two-stage task updates are not wired"})
			return
		}
		task, err := taskReader.GetByJobID(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "store_failure", "message": err.Error()})
			return
		}
		if task == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job_not_found", "message": "job_id does not match any task"})
			return
		}
		if task.JobID != jobID {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job_not_found", "message": "job_id does not match any task"})
			return
		}
		if task.Status != taskgraph.StatusPending && task.Status != taskgraph.StatusReady {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "job_already_leased", "message": "runtime assets must be finalized before the task is leased"})
			return
		}
		specStore, ok := taskReader.(taskgraph.TaskSpecStore)
		if !ok {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "task_store_unavailable", "message": "task spec runtime updates are not wired"})
			return
		}
		current, err := specStore.GetTaskSpecPayload(c.Request.Context(), task.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "store_failure", "message": err.Error()})
			return
		}
		if previous, _ := current["runtime_finalize_idempotency_key"].(string); previous != "" && previous != req.IdempotencyKey {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "finalize_idempotency_conflict", "message": "job was already finalized with a different idempotency_key"})
			return
		}
		if previous, _ := current["runtime_finalize_idempotency_key"].(string); previous == req.IdempotencyKey && current["runtime_assets_pending"] == false {
			c.JSON(http.StatusAccepted, gin.H{"ok": true, "job_id": jobID, "phase": "FINALIZE", "dispatch_status": "prefetch_refresh_queued", "idempotent": true})
			return
		}

		patch := make(map[string]interface{}, len(req.RuntimePayload)+4)
		for key, value := range req.RuntimePayload {
			if key == "runtime_assets_pending" || key == "runtime_finalize_idempotency_key" {
				continue
			}
			patch[key] = value
		}
		if len(req.Overlays) > 0 {
			value, err := jsonValue(req.Overlays)
			if err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": err.Error()})
				return
			}
			patch["overlays"] = value
		}
		if len(req.RuntimeAssets) > 0 {
			patch["runtime_assets"] = req.RuntimeAssets
		}
		patch["runtime_assets_pending"] = false
		patch["runtime_finalize_idempotency_key"] = req.IdempotencyKey
		if _, err := specStore.MergeTaskSpecPayload(c.Request.Context(), task.ID, patch); err != nil {
			status := http.StatusConflict
			if strings.Contains(err.Error(), "not initialized") || strings.Contains(err.Error(), "not found") {
				status = http.StatusInternalServerError
			}
			c.JSON(status, gin.H{"ok": false, "error": "runtime_payload_update_failed", "message": err.Error()})
			return
		}
		if h.enqueuer != nil {
			h.enqueuer.NotifyTaskUpdated(c.Request.Context(), jobID)
		}
		c.JSON(http.StatusAccepted, gin.H{
			"ok": true, "job_id": jobID, "phase": "FINALIZE",
			"dispatch_status":   "prefetch_refresh_queued",
			"future_asset_plan": "refresh",
			"idempotency_key":   req.IdempotencyKey,
		})
	}
}

func jsonValue(value interface{}) (interface{}, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func writeIntakeResponse(c *gin.Context, out intakeResponse) {
	for key, values := range out.Header {
		for _, value := range values {
			c.Header(key, value)
		}
	}
	if out.Location != "" {
		c.Header("Location", out.Location)
	}
	if out.Usage != nil {
		SetUsageStats(c, out.Usage.Scenes, out.Usage.TotalDurationSeconds)
	}
	c.JSON(out.Status, out.Body)
}
