package pipeline

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

// FinalizeJobRequest is the runtime half of the same job created by
// POST /api/v1/jobs/pre. runtime_payload is intentionally extensible so 77
// can add TTS, BGM and SFX fields without introducing parallel resolver/cache
// infrastructure. Raw overlays stay typed; finished composite output uses
// VisualReplacements.
type FinalizeJobRequest struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Overlays       []SubmitOverlay `json:"overlays,omitempty"`
	// RenderManifest is the completed manifest after generated media is
	// available. Its original PRE timeline must compile identically.
	RenderManifest     map[string]interface{}      `json:"render_manifest,omitempty"`
	VisualReplacements []FinalizeVisualReplacement `json:"visual_replacements,omitempty"`
	RuntimeAssets      []map[string]interface{}    `json:"runtime_assets,omitempty"`
	RuntimePayload     map[string]interface{}      `json:"runtime_payload,omitempty"`
}

// FinalizeVisualReplacement binds an already-composited MP4 asset declared
// in RenderManifest to an absolute window on the original timeline.
type FinalizeVisualReplacement struct {
	ReplacementID   string `json:"replacement_id"`
	AssetID         string `json:"asset_id"`
	SHA256          string `json:"sha256,omitempty"`
	TimelineStartUS int64  `json:"timeline_start_us"`
	TimelineEndUS   int64  `json:"timeline_end_us"`
	ProfileID       string `json:"profile_id"`
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
		if hasCompositeOverlay(req.Overlays) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "overlay_requires_prepared_video", "message": "composite overlays must be rendered into finished video assets before FINALIZE; submit them as visual_replacements with the completed render_manifest"})
			return
		}
		if len(req.Overlays) > 0 {
			if details := validateSubmitOverlays(req.Overlays); len(details) > 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": "overlay windows are invalid", "details": details})
				return
			}
		}
		if len(req.VisualReplacements) > 0 {
			if len(req.RenderManifest) == 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "render_manifest_required", "message": "visual_replacements require the completed render_manifest"})
				return
			}
			if details := validateFinalizeVisualReplacements(req.VisualReplacements); len(details) > 0 {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": "prepared visual replacement windows are invalid", "details": details})
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
		if len(req.Overlays) > 0 {
			if compiled, isCompiledPlan := current[contract.PayloadKeyCompiledRenderPlanJSON].(string); isCompiledPlan && strings.TrimSpace(compiled) != "" {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "use_visual_replacements", "message": "PRE task uses a compiled V2 plan; finalize it with visual_replacements and the completed render_manifest so the plan is recompiled before dispatch"})
				return
			}
		}
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
		if len(req.VisualReplacements) > 0 {
			currentPlanJSON, ok := current[contract.PayloadKeyCompiledRenderPlanJSON].(string)
			if !ok || strings.TrimSpace(currentPlanJSON) == "" {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "compiled_plan_required", "message": "PRE task must contain a compiled V2 plan before prepared replacements can be finalized"})
				return
			}
			if err := validateCompletedManifestAgainstPlan(currentPlanJSON, req.RenderManifest); err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_render_manifest_update", "message": err.Error()})
				return
			}
			rawReplacements, err := jsonValue(req.VisualReplacements)
			if err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": err.Error()})
				return
			}
			replacements, err := contract.ParseVisualReplacements(rawReplacements)
			if err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_payload", "message": err.Error()})
				return
			}
			planJSON, planSHA, err := contract.CompileRenderPlanV2JSONWithReplacements(req.RenderManifest, replacements)
			if err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"ok": false, "error": "invalid_visual_replacements", "message": err.Error()})
				return
			}
			patch["render_manifest"] = req.RenderManifest
			patch["visual_replacements"] = rawReplacements
			patch[contract.PayloadKeyCompiledRenderPlanJSON] = string(planJSON)
			patch[contract.PayloadKeyCompiledRenderPlanSHA] = planSHA
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

func hasCompositeOverlay(overlays []SubmitOverlay) bool {
	for _, overlay := range overlays {
		if strings.EqualFold(strings.TrimSpace(overlay.Mode), "composite") {
			return true
		}
	}
	return false
}

func validateFinalizeVisualReplacements(items []FinalizeVisualReplacement) []gin.H {
	seen := make(map[string]struct{}, len(items))
	details := make([]gin.H, 0)
	for i, item := range items {
		path := fmt.Sprintf("visual_replacements.%d", i)
		id := strings.TrimSpace(item.ReplacementID)
		if id == "" {
			details = append(details, gin.H{"path": path + ".replacement_id", "issue": "required"})
		} else if _, exists := seen[id]; exists {
			details = append(details, gin.H{"path": path + ".replacement_id", "issue": "duplicate"})
		}
		seen[id] = struct{}{}
		if strings.TrimSpace(item.AssetID) == "" {
			details = append(details, gin.H{"path": path + ".asset_id", "issue": "required"})
		}
		if item.TimelineStartUS < 0 || item.TimelineEndUS <= item.TimelineStartUS {
			details = append(details, gin.H{"path": path, "issue": "invalid_timeline_window"})
		}
		if strings.TrimSpace(item.ProfileID) == "" {
			details = append(details, gin.H{"path": path + ".profile_id", "issue": "required"})
		}
	}
	return details
}

// validateCompletedManifestAgainstPlan pins the editorial timeline from PRE while
// allowing FINALIZE to add the already-rendered media assets referenced by
// visual_replacements. The strict manifest/compiler performs full validation.
func validateCompletedManifestAgainstPlan(previousPlanJSON string, completed map[string]interface{}) error {
	previous, err := contract.DecodeCompiledRenderPlanV2([]byte(previousPlanJSON))
	if err != nil {
		return fmt.Errorf("PRE compiled plan is invalid: %w", err)
	}
	baseJSON, _, err := contract.CompileRenderPlanV2JSONWithReplacements(completed, nil)
	if err != nil {
		return fmt.Errorf("completed render_manifest is invalid: %w", err)
	}
	base, err := contract.DecodeCompiledRenderPlanV2(baseJSON)
	if err != nil {
		return fmt.Errorf("completed render_manifest produced an invalid base plan: %w", err)
	}
	// Asset additions change the manifest identity hash. Compare the rendered
	// timeline/output/audio contract separately, then require all PRE assets to
	// remain byte-for-byte equivalent in the completed manifest.
	previous.TimelineSHA256, base.TimelineSHA256 = "", ""
	previous.FinalAudio.TimelineSHA256, base.FinalAudio.TimelineSHA256 = "", ""
	previousAssets, baseAssets := previous.Assets, base.Assets
	previous.Assets, base.Assets = nil, nil
	if !reflect.DeepEqual(previous, base) {
		return fmt.Errorf("completed render_manifest changes the PRE video timeline, output or final audio")
	}
	byID := make(map[string]contract.AssetRefV2, len(baseAssets))
	for _, asset := range baseAssets {
		byID[asset.AssetID] = asset
	}
	for _, asset := range previousAssets {
		if completedAsset, ok := byID[asset.AssetID]; !ok || completedAsset != asset {
			return fmt.Errorf("completed render_manifest changes or removes PRE asset %q", asset.AssetID)
		}
	}
	return nil
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
