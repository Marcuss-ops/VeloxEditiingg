package pipeline

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

// FinalizeJobRequest carries runtime asset bindings for a producer-compiled
// V2 plan. Timeline compilation belongs to the producer.
type FinalizeJobRequest struct {
	IdempotencyKey string                   `json:"idempotency_key"`
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
			Phase:        "PREPARE",
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
	// For a compiled V2 plan, a non-empty asset list is not proof that the
	// plan is ready to dispatch. Producers commonly know overlays/audio before
	// stock bindings arrive; treating that partial list as complete pins the
	// immutable task executor to copy before FINALIZE can reveal Drive stock.
	// Require every plan asset and final audio binding (or at least one
	// resolved member for a StockPool plan) before allowing single-stage PRE.
	if strings.TrimSpace(req.CompiledRenderPlanJSON) != "" {
		if req.RuntimeAssetsComplete != nil && !*req.RuntimeAssetsComplete {
			return false
		}
		return compiledPlanRuntimeAssetsComplete(req)
	}
	return complete
}

func compiledPlanRuntimeAssetsComplete(req SubmitJobRequest) bool {
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(req.CompiledRenderPlanJSON), &plan); err != nil {
		return false
	}
	assets := append([]map[string]interface{}(nil), req.RuntimeAssets...)
	if raw, ok := req.RuntimePayload["runtime_assets"]; ok {
		encoded, marshalErr := json.Marshal(raw)
		if marshalErr != nil {
			return false
		}
		var payloadAssets []map[string]interface{}
		if json.Unmarshal(encoded, &payloadAssets) != nil {
			return false
		}
		assets = append(assets, payloadAssets...)
	}
	bound := make(map[string]struct{}, len(assets)*2)
	stockPoolMembers := 0
	for _, asset := range assets {
		for _, key := range []string{"asset_id", "asset_key", "drive_file_id"} {
			if value, ok := asset[key].(string); ok && strings.TrimSpace(value) != "" {
				bound[strings.TrimSpace(value)] = struct{}{}
			}
		}
		driveFileID, _ := asset["drive_file_id"].(string)
		if member, _ := asset["stock_pool_member"].(bool); member && strings.TrimSpace(driveFileID) != "" {
			stockPoolMembers++
		}
	}
	if plan.StockPool != nil && stockPoolMembers == 0 {
		return false
	}
	for _, asset := range plan.Assets {
		_, assetIDBound := bound[asset.AssetID]
		_, assetKeyBound := bound[asset.AssetKey]
		_, driveIDBound := bound[asset.DriveFileID]
		if !assetIDBound && !assetKeyBound && !driveIDBound {
			return false
		}
	}
	if plan.FinalAudio.AssetID != "" {
		if _, ok := bound[plan.FinalAudio.AssetID]; !ok {
			return false
		}
	}
	return len(assets) > 0
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
		if errs := ValidateRuntimeAssetSourceURLs(req.RuntimeAssets, h.cfg); len(errs) > 0 {
			status, body := ssrfValidationErrorEnvelope(errs)
			c.JSON(status, body)
			return
		}
		jobID := strings.TrimSpace(c.Param("id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required", "message": "URL path /api/v1/jobs/:id/finalize requires non-empty :id"})
			return
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
		h.finalizeMu.Lock()
		defer h.finalizeMu.Unlock()
		current, err = specStore.GetTaskSpecPayload(c.Request.Context(), task.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "store_failure", "message": err.Error()})
			return
		}
		planJSON, _ := current[contract.PayloadKeyCompiledRenderPlanJSON].(string)
		if previous, _ := current["runtime_finalize_idempotency_key"].(string); previous != "" && previous != req.IdempotencyKey {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "finalize_idempotency_conflict", "message": "job was already finalized with a different idempotency_key"})
			return
		}
		if previous, _ := current["runtime_finalize_idempotency_key"].(string); previous == req.IdempotencyKey && current["runtime_assets_pending"] == false {
			// The payload merge may have committed just before a process crash
			// prevented the original planner notification. Replays must re-drive
			// the refresh so the durable prefetch plan cannot remain stale.
			if h.enqueuer != nil {
				h.enqueuer.NotifyTaskUpdated(c.Request.Context(), jobID)
			}
			logFinalizeAccepted(jobID, req, current, true)
			c.JSON(http.StatusAccepted, gin.H{"ok": true, "job_id": jobID, "phase": "FINALIZE", "dispatch_status": "prefetch_refresh_queued", "idempotent": true})
			return
		}
		if previous, _ := current["runtime_finalize_idempotency_key"].(string); previous == req.IdempotencyKey {
			state, _ := current[runtimeFinalizeStatusKey].(string)
			if state == runtimeFinalizeQueued || state == runtimeFinalizeRunning {
				c.JSON(http.StatusAccepted, gin.H{"ok": true, "job_id": jobID, "phase": "FINALIZE", "dispatch_status": "stock_preparation_queued", "finalization_status": state, "idempotent": true})
				return
			}
		}
		bindings := append([]map[string]interface{}(nil), req.RuntimeAssets...)
		var bindingErr error
		bindings, bindingErr = appendRuntimeAssetBindings(bindings, req.RuntimePayload["runtime_assets"])
		if bindingErr == nil {
			bindings, bindingErr = appendRuntimeAssetBindings(bindings, current["runtime_assets"])
		}
		if storedPayload, ok := current["runtime_payload"].(map[string]interface{}); ok && bindingErr == nil {
			bindings, bindingErr = appendRuntimeAssetBindings(bindings, storedPayload["runtime_assets"])
		}
		if bindingErr != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_runtime_assets", "message": bindingErr.Error()})
			return
		}
		if compiledPlanHasDriveStock(planJSON, bindings) {
			if h.assetService == nil || h.enqueuer == nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "stock_preparation_unavailable", "message": "Master stock window preparation is not configured"})
				return
			}
			request := map[string]interface{}{
				"idempotency_key": req.IdempotencyKey,
				"runtime_assets":  req.RuntimeAssets,
				"runtime_payload": req.RuntimePayload,
			}
			patch := map[string]interface{}{
				runtimeFinalizeRequestKey:          request,
				runtimeFinalizeStatusKey:           runtimeFinalizeQueued,
				runtimeFinalizeErrorKey:            nil,
				"runtime_finalize_idempotency_key": req.IdempotencyKey,
				"runtime_assets_pending":           true,
			}
			if _, err := specStore.MergeTaskSpecPayload(c.Request.Context(), task.ID, patch); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "runtime_payload_update_failed", "message": err.Error()})
				return
			}
			logFinalizeAccepted(jobID, req, current, false)
			c.JSON(http.StatusAccepted, gin.H{"ok": true, "job_id": jobID, "phase": "FINALIZE", "dispatch_status": "stock_preparation_queued", "finalization_status": runtimeFinalizeQueued, "idempotency_key": req.IdempotencyKey})
			return
		}
		patch, err := buildRuntimeFinalizePatch(c.Request.Context(), planJSON, req, current, nil)
		if err != nil {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_runtime_assets", "message": err.Error()})
			return
		}
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
		logFinalizeAccepted(jobID, req, current, false)
		c.JSON(http.StatusAccepted, gin.H{
			"ok": true, "job_id": jobID, "phase": "FINALIZE",
			"dispatch_status":   "prefetch_refresh_queued",
			"future_asset_plan": "refresh",
			"idempotency_key":   req.IdempotencyKey,
		})
	}
}

func logFinalizeAccepted(jobID string, req FinalizeJobRequest, payload map[string]interface{}, idempotent bool) {
	correlationID, _ := payload["correlation_id"].(string)
	project := intakeProjectLabel(SubmitJobRequest{RuntimePayload: req.RuntimePayload}, payload)
	pipelineLog(
		"API_V1_JOBS_ACCEPTED phase=FINALIZE job_id=%s correlation_id=%s project=%q idem_hash=%s idempotent=%t",
		jobID,
		correlationID,
		project,
		logHashShort(req.IdempotencyKey),
		idempotent,
	)
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
