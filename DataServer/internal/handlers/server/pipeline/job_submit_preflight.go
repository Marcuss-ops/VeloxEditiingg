// Package pipeline / job_submit_preflight.go — the intake pre-flight helpers
// shared by the single-job surface (job_submit.go) and the batch envelope
// (batch_submit.go) through the transport-neutral core
// (job_submit_core.go):
//
//   - idempotencyKeyErrorEnvelope: canonical 400 envelope for a rejected
//     idempotency_key (byte-shape violations from ValidateIdempotencyKey).
//   - assetPreflightEnvelope: the registry/blob + verified-media gate.
//   - deliveryPlanDestinationEnvelope: the P0 #2 delivery-destination
//     existence pre-flight (3-state batch status, fail-closed).
//
// Each helper RETURNS its (status, body) envelope instead of writing to a
// gin.Context, so a caller that owns no HTTP response (one batch item) gets the
// identical outcome without simulating HTTP. The third return value is the
// "stop the intake" flag: false means the pre-flight passed.
package pipeline

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/deliverystore"
	"velox-shared/assetref"

	"github.com/gin-gonic/gin"
)

// idempotencyKeyErrorEnvelope builds the canonical 400 envelope for a
// rejected idempotency_key. A typed *IdempotencyKeyError carries
// machine-readable reason + diagnostics so the API envelope is
// actionable. 400 because idempotency_key is a request-level
// byte-shape issue, distinct from the 422 semantic issues that
// follow.
func idempotencyKeyErrorEnvelope(vErr *IdempotencyKeyError) (int, gin.H) {
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
	return http.StatusBadRequest, gin.H{
		"ok":      false,
		"error":   vErr.Code,
		"message": vErr.Message,
		"details": details,
	}
}

// checkAssetPreflight validates canonical velox-asset references against the
// Master registry and either a final blob or a registered external source
// before enqueue. It enforces the Fase C2 fail-closed media gate: a media
// asset MUST carry verified registry metadata (ensured through the canonical
// one-time probe, which may persist the metadata row) before the job is
// admitted. External sources such as Drive are not downloaded here; the agent
// asset route resolves the saved source reference at execution time.
func assetPreflightEnvelope(ctx context.Context, h *Handlers, req SubmitJobRequest) (int, gin.H, bool) {
	payload, err := projectWorkerPayload(&req)
	if err != nil {
		// The canonical projection is run again by the enqueue path. Do not
		// turn a projection implementation detail into an asset error here.
		return 0, nil, false
	}
	requirements := collectAssetPreflightRequirements(payload)
	if len(requirements) == 0 {
		return 0, nil, false
	}
	if h == nil || h.assetService == nil {
		// The production composition always wires AssetService. Lightweight
		// pipeline profiles and legacy test harnesses may intentionally omit
		// the optional asset registry; their existing enqueue validation remains
		// authoritative. Production readiness owns the fail-closed wiring gate.
		return 0, nil, false
	}
	report, err := h.assetService.Preflight(ctx, requirements)
	if err != nil {
		return http.StatusInternalServerError, gin.H{
			"ok":      false,
			"error":   "asset_preflight_unavailable",
			"message": err.Error(),
		}, true
	}
	var details []gin.H
	for _, item := range report.Items {
		if item.Metadata && item.MediaMetadata && item.BlobResolvable && item.SHA256Valid && item.SizeValid {
			continue
		}
		details = append(details, gin.H{
			"asset_id":        item.AssetID,
			"issue":           item.Issue,
			"metadata":        item.Metadata,
			"media_metadata":  item.MediaMetadata,
			"blob_resolvable": item.BlobResolvable,
			"sha256_valid":    item.SHA256Valid,
			"size_valid":      item.SizeValid,
		})
	}
	if len(details) == 0 {
		return 0, nil, false
	}
	return http.StatusUnprocessableEntity, gin.H{
		"ok":      false,
		"error":   "asset_preflight_failed",
		"message": "one or more assets are unavailable or failed integrity validation",
		"summary": report,
		"details": details,
	}, true
}

func collectAssetPreflightRequirements(payload map[string]interface{}) []voiceoverassets.AssetPreflightRequirement {
	byID := make(map[string]voiceoverassets.AssetPreflightRequirement)
	var walk func(any, string, int64)
	walk = func(value any, inheritedSHA string, inheritedSize int64) {
		switch typed := value.(type) {
		case map[string]interface{}:
			sha := inheritedSHA
			if candidate, ok := typed["sha256"].(string); ok && strings.TrimSpace(candidate) != "" {
				sha = candidate
			}
			size := inheritedSize
			if candidate, ok := typed["size_bytes"].(float64); ok && candidate > 0 {
				size = int64(candidate)
			}
			if candidate, ok := typed["size_bytes"].(int64); ok && candidate > 0 {
				size = candidate
			}
			for key, child := range typed {
				text, ok := child.(string)
				if !ok {
					continue
				}
				id, ok := assetref.WireAssetID(text)
				if !ok || !assetref.IsLocalWire(text) {
					continue
				}
				if key == "asset_id" || key == "id" || strings.EqualFold(key, "url") || strings.EqualFold(key, "source_url") || strings.EqualFold(key, "uri") {
					mergeAssetPreflightRequirement(byID, voiceoverassets.AssetPreflightRequirement{AssetID: id, SHA256: sha, SizeBytes: size})
				}
			}
			for _, child := range typed {
				walk(child, sha, size)
			}
		case []interface{}:
			for _, child := range typed {
				walk(child, inheritedSHA, inheritedSize)
			}
		case []map[string]interface{}:
			for _, child := range typed {
				walk(child, inheritedSHA, inheritedSize)
			}
		}
	}
	walk(payload, "", 0)
	result := make([]voiceoverassets.AssetPreflightRequirement, 0, len(byID))
	for _, requirement := range byID {
		result = append(result, requirement)
	}
	return result
}

func mergeAssetPreflightRequirement(dst map[string]voiceoverassets.AssetPreflightRequirement, incoming voiceoverassets.AssetPreflightRequirement) {
	current, ok := dst[incoming.AssetID]
	if !ok {
		dst[incoming.AssetID] = incoming
		return
	}
	if current.SHA256 == "" {
		current.SHA256 = incoming.SHA256
	}
	if current.SizeBytes <= 0 {
		current.SizeBytes = incoming.SizeBytes
	}
	dst[incoming.AssetID] = current
}

// checkDeliveryPlanDestinations runs the P0 #2 delivery-destination
// existence pre-flight: every unique non-empty delivery_plan[N].destination_id
// MUST resolve to an enabled row in delivery_destinations.
//
// Runs AFTER ValidateSubmitJobRequest so we don't query the store for
// bodies that already failed shape rules, and BEFORE SSRF / quota so
// the rejection paths are order-stable: a missing destination is a
// 422 invalid_payload (this gate), not a 500 from inside
// AtomicForwardAndEnqueue (where validateDeliveryDestinationTx
// currently emits a plaintext "destination_id %q does not exist"
// error that WriteResolverError cannot map). Mirrors the policy of
// store.validateDeliveryDestinationTx so the handler-side and tx-side
// checks agree on existence + enabled=1.
//
// §0.3.4 item 4 split (NIT-2): the previous 2-state helper collapsed
// NOT_FOUND and DISABLED into a single `issue: invalid` detail. This
// pre-flight now uses the 3-state BatchDeliveryDestinationsStatus and
// emits a distinct target_error_code per bucket so operator dashboards
// can disambiguate:
//
//   - NOT_FOUND       → target_error_code=DESTINATION_NOT_FOUND
//     (id was unknown; producer must obtain a
//     valid opaque destination from InstaEdit,
//     never invent destinations).
//   - DISABLED        → target_error_code=BLOCKED_VELOX_DISABLED
//     (the locally provisioned opaque delivery
//     row is disabled; remediation belongs to
//     the InstaEdit-owned destination lifecycle).
//   - ENABLED         → no detail, enqueue proceeds.
//
// Catalog-side publishability is owned by InstaEdit. Velox only
// validates the opaque delivery row supplied by the submitter.
//
// Aggregates ALL destination-existence violations into a single 422
// with details[].path = "delivery_plan.N.destination_id" (same path
// shape used by enqueue's *validationError). Fail-closed on store
// failure (500 store_failure).
//
// Returns (status, body, true) when the intake must stop because the
// destination pre-flight failed (500 store_failure or 422 invalid_payload);
// (0, nil, false) when the plan is empty or all destinations resolved to
// ENABLED.
const localFallbackDestinationID = "local-fallback"

func deliveryPlanDestinationEnvelope(ctx context.Context, h *Handlers, req *SubmitJobRequest) (int, gin.H, bool) {
	if req == nil || len(req.DeliveryPlan) == 0 {
		return 0, nil, false
	}
	if h.store == nil {
		return http.StatusInternalServerError, gin.H{
			"ok":      false,
			"error":   "store_failure",
			"message": "delivery_plan validation requires a configured store",
		}, true
	}
	ids := make([]string, 0, len(req.DeliveryPlan))
	for _, d := range req.DeliveryPlan {
		if tid := strings.TrimSpace(d.DestinationID); tid != "" {
			ids = append(ids, tid)
		}
	}
	if len(ids) == 0 {
		return 0, nil, false
	}
	statuses, qerr := h.store.Delivery().BatchDeliveryDestinationsStatus(ctx, ids)
	if qerr != nil {
		return http.StatusInternalServerError, gin.H{
			"ok":      false,
			"error":   "store_failure",
			"message": "failed to resolve delivery destinations: " + qerr.Error(),
		}, true
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
		switch statuses[tid] {
		case deliverystore.DeliveryDestinationNotFound:
			if deliveryPlanRequestsLocalFallback(d.Metadata) {
				req.DeliveryPlan[i].DestinationID = localFallbackDestinationID
				continue
			}
			destDetails = append(destDetails, gin.H{
				"path":              fmt.Sprintf("delivery_plan.%d.destination_id", i),
				"issue":             "destination_not_found",
				"target_error_code": "DESTINATION_NOT_FOUND",
				"status":            deliverystore.DeliveryDestinationNotFound.String(),
			})
		case deliverystore.DeliveryDestinationDisabled:
			if deliveryPlanRequestsLocalFallback(d.Metadata) {
				req.DeliveryPlan[i].DestinationID = localFallbackDestinationID
				continue
			}
			destDetails = append(destDetails, gin.H{
				"path":              fmt.Sprintf("delivery_plan.%d.destination_id", i),
				"issue":             "destination_disabled",
				"target_error_code": BlockedCodeVeloxDisabled,
				"status":            deliverystore.DeliveryDestinationDisabled.String(),
			})
		}
	}
	if len(destDetails) > 0 {
		return http.StatusUnprocessableEntity, gin.H{
			"ok":      false,
			"error":   "invalid_payload",
			"message": fmt.Sprintf("request body has %d validation failure(s) (see details)", len(destDetails)),
			"details": destDetails,
		}, true
	}
	return 0, nil, false
}

// deliveryPlanRequestsLocalFallback is opt-in per delivery entry. This keeps
// ordinary publishing fail-closed while allowing benchmark/smoke jobs to
// retain a verified local artifact when an external Drive destination is not
// provisioned.
func deliveryPlanRequestsLocalFallback(metadata any) bool {
	m, ok := metadata.(map[string]interface{})
	if !ok {
		return false
	}
	v, ok := m["local_fallback"].(bool)
	return ok && v
}
