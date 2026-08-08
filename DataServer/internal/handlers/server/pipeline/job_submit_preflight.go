// Package pipeline / job_submit_preflight.go — the inline validation
// helpers extracted from the SubmitJob handler in job_submit.go:
//
//   - writeIdempotencyKeyError: canonical 400 envelope for a rejected
//     idempotency_key (byte-shape violations from ValidateIdempotencyKey).
//   - checkDeliveryPlanDestinations: the P0 #2 delivery-destination
//     existence pre-flight (3-state batch status, fail-closed).
package pipeline

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

// writeIdempotencyKeyError writes the canonical 400 envelope for a
// rejected idempotency_key. A typed *IdempotencyKeyError carries
// machine-readable reason + diagnostics so the API envelope is
// actionable. 400 because idempotency_key is a request-level
// byte-shape issue, distinct from the 422 semantic issues that
// follow.
func writeIdempotencyKeyError(c *gin.Context, vErr *IdempotencyKeyError) {
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
// Returns true when the handler must return early because a response
// was already written (500 store_failure or 422 invalid_payload);
// false when the plan is empty or all destinations resolved to ENABLED.
func checkDeliveryPlanDestinations(c *gin.Context, h *Handlers, req SubmitJobRequest) bool {
	if len(req.DeliveryPlan) == 0 {
		return false
	}
	if h.store == nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":      false,
			"error":   "store_failure",
			"message": "delivery_plan validation requires a configured store",
		})
		return true
	}
	ids := make([]string, 0, len(req.DeliveryPlan))
	for _, d := range req.DeliveryPlan {
		if tid := strings.TrimSpace(d.DestinationID); tid != "" {
			ids = append(ids, tid)
		}
	}
	if len(ids) == 0 {
		return false
	}
	statuses, qerr := h.store.BatchDeliveryDestinationsStatus(c.Request.Context(), ids)
	if qerr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":      false,
			"error":   "store_failure",
			"message": "failed to resolve delivery destinations: " + qerr.Error(),
		})
		return true
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
		case store.DeliveryDestinationNotFound:
			destDetails = append(destDetails, gin.H{
				"path":              fmt.Sprintf("delivery_plan.%d.destination_id", i),
				"issue":             "destination_not_found",
				"target_error_code": "DESTINATION_NOT_FOUND",
				"status":            store.DeliveryDestinationNotFound.String(),
			})
		case store.DeliveryDestinationDisabled:
			destDetails = append(destDetails, gin.H{
				"path":              fmt.Sprintf("delivery_plan.%d.destination_id", i),
				"issue":             "destination_disabled",
				"target_error_code": BlockedCodeVeloxDisabled,
				"status":            store.DeliveryDestinationDisabled.String(),
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
		return true
	}
	return false
}
