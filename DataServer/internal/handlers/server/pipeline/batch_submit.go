package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
)

// MaxSubmitJobBatchIDBytes bounds the stable batch identity used in
// responses, logs, and operator lookups.
const MaxSubmitJobBatchIDBytes = 128

// ValidateSubmitJobBatchRequest validates batch-envelope constraints.
// Per-item structural validation remains owned by ValidateSubmitJobRequest so
// the batch and single-job endpoints cannot drift apart. Item idempotency
// uniqueness is checked in the batch loop to preserve per-item isolation.
func ValidateSubmitJobBatchRequest(req SubmitJobBatchRequest) (*SubmitJobValidationError, bool) {
	var details []gin.H
	batchID := strings.TrimSpace(req.BatchID)
	if batchID == "" {
		details = append(details, gin.H{
			"path":  "batch_id",
			"issue": "required",
		})
	} else {
		if !utf8.ValidString(batchID) {
			details = append(details, gin.H{
				"path":     "batch_id",
				"issue":    "invalid_utf8",
				"expected": "valid UTF-8 string",
			})
		}
		if len(batchID) > MaxSubmitJobBatchIDBytes {
			details = append(details, gin.H{
				"path":     "batch_id",
				"issue":    "max_length",
				"max":      MaxSubmitJobBatchIDBytes,
				"observed": len(batchID),
			})
		}
		if strings.ContainsAny(batchID, "./\\\\:%") {
			details = append(details, gin.H{
				"path":     "batch_id",
				"issue":    "reserved_separator",
				"expected": "identifier without '.', '/', '\\\\', ':', or '%'",
			})
		}
		if offset, bad := hasControlOrSeparatorByte(batchID); bad && (batchID[offset] <= 0x20 || batchID[offset] == 0x7f) {
			details = append(details, gin.H{
				"path":        "batch_id",
				"issue":       "invalid_character",
				"byte_offset": offset,
				"expected":    "identifier without whitespace or control characters",
			})
		}
	}
	if len(req.Items) == 0 {
		details = append(details, gin.H{
			"path":  "items",
			"issue": "empty",
		})
	} else if len(req.Items) > MaxSubmitJobBatchItems {
		details = append(details, gin.H{
			"path":     "items",
			"issue":    "max_items",
			"max":      MaxSubmitJobBatchItems,
			"observed": len(req.Items),
		})
	}
	if len(details) == 0 {
		return nil, false
	}
	return &SubmitJobValidationError{
		Code:    "invalid_payload",
		Reason:  "batch_validation_failed",
		Message: fmt.Sprintf("batch request has %d validation failure(s) (see details)", len(details)),
		Details: details,
	}, true
}

// SubmitJobBatch handles POST /api/v1/jobs/batch. The endpoint is deliberately
// item-isolated: a malformed or failed item is reported in place while other
// items continue through the canonical single-job submission path.
func (h *Handlers) SubmitJobBatch() gin.HandlerFunc {
	return func(c *gin.Context) {
		var batch SubmitJobBatchRequest
		if err := decodeStrictJSON(c.Request.Body, &batch); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"ok":      false,
				"error":   "invalid_json",
				"message": "request body must be valid UTF-8 JSON without unknown fields and contain exactly one value: " + err.Error(),
			})
			return
		}
		if validationErr, bad := ValidateSubmitJobBatchRequest(batch); bad {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"ok":      false,
				"error":   validationErr.Code,
				"message": validationErr.Message,
				"details": validationErr.Details,
			})
			return
		}

		identity := batchIntakeIdentity(c)

		results := make([]SubmitJobBatchItemResult, len(batch.Items))
		seenKeys := make(map[string]int, len(batch.Items))
		dedupe := newBatchDedupeWindow()
		var totalScenes int
		var totalDuration float64
		for index, item := range batch.Items {
			if err := NormalizeCanonicalRecipe(&item); err != nil {
				results[index] = SubmitJobBatchItemResult{Index: index, IdempotencyKey: item.IdempotencyKey, Status: "rejected", Errors: []string{err.Error()}}
				dedupe.recordRejected()
				continue
			}
			batch.Items[index] = item
			result := SubmitJobBatchItemResult{
				Index:          index,
				IdempotencyKey: item.IdempotencyKey,
			}
			if keyError, invalid := ValidateIdempotencyKey(item.IdempotencyKey); invalid {
				result.Status = "rejected"
				result.Errors = []string{keyError.Code + ": " + keyError.Reason}
				results[index] = result
				dedupe.recordRejected()
				continue
			}
			key := strings.TrimSpace(item.IdempotencyKey)
			item.IdempotencyKey = key
			result.IdempotencyKey = key
			if previous, exists := seenKeys[key]; exists {
				result.Status = "rejected"
				result.Errors = []string{fmt.Sprintf("duplicate_idempotency_key: duplicates items.%d", previous)}
				results[index] = result
				dedupe.recordConflict()
				continue
			}
			seenKeys[key] = index

			// In-batch plan dedupe: an item whose render plan is fingerprint-
			// identical to an already-ACCEPTED item of this batch reuses that
			// item's job (re-publication of the same render) instead of
			// enqueuing a byte-identical render. Publications/delivery plans
			// are excluded from the fingerprint, so a variant that differs
			// only by destination still dedupes. A FAILED enqueue never
			// becomes an anchor (recordAccepted runs only on accepted items).
			fingerprint, fpErr := BatchItemFingerprint(item)
			if fpErr == nil {
				if anchorIndex, anchorJob, hit := dedupe.lookup(fingerprint); hit {
					anchor := anchorIndex
					result.Status = "dedup"
					result.JobID = anchorJob
					result.DedupedOf = &anchor
					results[index] = result
					dedupe.recordDedup()
					continue
				}
			}
			// A fingerprint computation failure is non-fatal: the item still
			// goes through the canonical single-job path un-deduped.

			result = h.submitBatchItem(c.Request.Context(), identity, index, item)
			results[index] = result
			switch result.Status {
			case "accepted":
				// Usage reflects work actually admitted. Rejected, conflicted,
				// and in-batch deduplicated items must not inflate the audit
				// aggregate.
				totalScenes += len(item.Scenes)
				for _, scene := range item.Scenes {
					totalDuration += scene.DurationSeconds
				}
				// Anchor registration only after a confirmed fresh enqueue: a
				// failed or rejected enqueue must never become a dedupe anchor.
				// Single-threaded loop: the lookup above missed this fingerprint,
				// so this item is unconditionally the anchor for it.
				if fpErr == nil && result.JobID != "" {
					dedupe.recordAccepted(index, fingerprint, result.JobID)
				}
			case "rejected":
				dedupe.recordRejected()
			case "conflict":
				dedupe.recordConflict()
			case "failed":
				dedupe.recordFailed()
			}
		}

		// The single-job child contexts receive their own usage stats. Mirror
		// the aggregate on the parent so the outer M2M audit middleware records
		// the complete batch rather than the last child (or zero values).
		SetUsageStats(c, totalScenes, totalDuration)
		c.JSON(http.StatusOK, SubmitJobBatchResponse{
			BatchID: batch.BatchID,
			Items:   results,
			Summary: SubmitJobBatchSummary{
				Total:    len(results),
				Accepted: dedupe.accepted,
				Deduped:  dedupe.deduped,
				Rejected: dedupe.rejected,
				Conflict: dedupe.conflict,
				Failed:   dedupe.failed,
			},
		})
	}
}

// batchIntakeIdentity derives the intake identity shared by every item of one
// batch envelope: the quota key and client id the M2M middleware resolved for
// the ENVELOPE request, and the `batch` intake source (the plain single-job
// endpoint stamps `canonical`). Building it once per envelope — instead of
// cloning a gin.Context per item — is what lets the items share the canonical
// intake core directly.
func batchIntakeIdentity(c *gin.Context) intakeIdentity {
	return intakeIdentity{
		ClientID:     ClientIDFromContext(c),
		IntakeSource: creatorflow.IntakeSourceBatch,
		Quota:        KeyFromContext(c),
	}
}

// submitBatchItem runs ONE batch item through the canonical single-job intake
// core (job_submit_core.go) and projects the transport-neutral outcome into the
// batch item result.
//
// It deliberately does NOT invoke the single-job HTTP handler: before this
// change each item was re-marshalled to JSON, dispatched through a cloned
// gin.Context with a synthetic ResponseWriter, and its JSON response parsed
// back into this result. The batch envelope now shares the intake
// implementation directly, so there is no HTTP round-trip to keep in sync and
// the item result is built from the core's typed envelope.
func (h *Handlers) submitBatchItem(ctx context.Context, identity intakeIdentity, index int, item SubmitJobRequest) SubmitJobBatchItemResult {
	result := SubmitJobBatchItemResult{
		Index:          index,
		IdempotencyKey: item.IdempotencyKey,
	}
	return batchItemResultFromOutcome(result, h.submitJobCore(ctx, item, identity))
}

// batchItemResultFromOutcome projects the canonical intake outcome into the
// batch item result.
//
// Errors are read from the TYPED envelope the core produced, so the batch
// classification is exactly the single-job classification. The previous
// implementation parsed the serialized JSON body of a synthetic single-job
// response (which is why it could not see a typed detail object without
// re-unmarshalling it).
func batchItemResultFromOutcome(result SubmitJobBatchItemResult, out intakeResponse) SubmitJobBatchItemResult {
	if jobID, ok := out.Body["job_id"].(string); ok {
		result.JobID = jobID
	}
	switch {
	case out.Status >= 200 && out.Status < 300:
		result.Status = "accepted"
	case out.Status == http.StatusConflict:
		result.Status = "conflict"
	case out.Status >= 500:
		result.Status = "failed"
	default:
		result.Status = "rejected"
	}
	if code, ok := out.Body["error"].(string); ok && code != "" {
		message := code
		if detail, ok := out.Body["message"].(string); ok && detail != "" {
			message += ": " + detail
		}
		result.Errors = append(result.Errors, message)
	}
	result.Errors = append(result.Errors, batchDetailErrors(out.Body["details"])...)
	if result.Status != "accepted" && len(result.Errors) == 0 {
		result.Errors = []string{fmt.Sprintf("http_status_%d", out.Status)}
	}
	return result
}

// batchDetailErrors flattens an envelope `details` value into the batch item's
// human-readable error lines.
//
// `path: issue` covers the {path, issue} diagnostics carried by the validation,
// enqueue and pre-flight envelopes. Any other shape (e.g. the quota
// {reason, observed, cap} object) is rendered as its JSON form, which is what
// the previous JSON-parsing projection produced — object-shaped diagnostics are
// preserved, never silently dropped.
func batchDetailErrors(details any) []string {
	items := batchDetailList(details)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if field, ok := item.(map[string]interface{}); ok {
			path, _ := field["path"].(string)
			issue, _ := field["issue"].(string)
			if path != "" || issue != "" {
				out = append(out, path+": "+issue)
				continue
			}
		}
		if encoded, err := json.Marshal(item); err == nil {
			out = append(out, string(encoded))
		}
	}
	return out
}

// batchDetailList normalizes the envelope's `details` value to a slice. It
// mirrors the two wire shapes the historical parser accepted: an array of
// diagnostics, or a single object treated as a one-element list. A nil/absent
// value yields nothing, so the `"details":null` envelopes contribute no error
// line.
func batchDetailList(details any) []any {
	switch typed := details.(type) {
	case nil:
		return nil
	case []any:
		return typed
	case []gin.H:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, map[string]interface{}(item))
		}
		return out
	case []map[string]interface{}:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	case gin.H:
		return []any{map[string]interface{}(typed)}
	case map[string]interface{}:
		return []any{typed}
	default:
		return []any{typed}
	}
}
