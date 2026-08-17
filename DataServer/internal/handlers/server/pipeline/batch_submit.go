package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

		results := make([]SubmitJobBatchItemResult, len(batch.Items))
		seenKeys := make(map[string]int, len(batch.Items))
		var totalScenes int
		var totalDuration float64
		for index, item := range batch.Items {
			if err := NormalizeCanonicalRecipe(&item); err != nil {
				results[index] = SubmitJobBatchItemResult{Index: index, IdempotencyKey: item.IdempotencyKey, Status: "rejected", Errors: []string{err.Error()}}
				continue
			}
			batch.Items[index] = item
			totalScenes += len(item.Scenes)
			for _, scene := range item.Scenes {
				totalDuration += scene.DurationSeconds
			}
			result := SubmitJobBatchItemResult{
				Index:          index,
				IdempotencyKey: item.IdempotencyKey,
			}
			if keyError, invalid := ValidateIdempotencyKey(item.IdempotencyKey); invalid {
				result.Status = "rejected"
				result.Errors = []string{keyError.Code + ": " + keyError.Reason}
				results[index] = result
				continue
			}
			key := strings.TrimSpace(item.IdempotencyKey)
			item.IdempotencyKey = key
			result.IdempotencyKey = key
			if previous, exists := seenKeys[key]; exists {
				result.Status = "rejected"
				result.Errors = []string{fmt.Sprintf("duplicate_idempotency_key: duplicates items.%d", previous)}
				results[index] = result
				continue
			}
			seenKeys[key] = index

			result = h.submitBatchItem(c, index, item)
			results[index] = result
		}

		// The single-job child contexts receive their own usage stats. Mirror
		// the aggregate on the parent so the outer M2M audit middleware records
		// the complete batch rather than the last child (or zero values).
		SetUsageStats(c, totalScenes, totalDuration)
		c.JSON(http.StatusOK, SubmitJobBatchResponse{
			BatchID: batch.BatchID,
			Items:   results,
		})
	}
}

// submitBatchItem invokes the canonical single-job handler with an isolated
// request/recorder pair. Copying c.Keys preserves the authenticated M2M
// context and quota settings without sharing response state between items.
func (h *Handlers) submitBatchItem(parent *gin.Context, index int, item SubmitJobRequest) SubmitJobBatchItemResult {
	result := SubmitJobBatchItemResult{
		Index:          index,
		IdempotencyKey: item.IdempotencyKey,
	}
	body, err := json.Marshal(item)
	if err != nil {
		result.Status = "failed"
		result.Errors = []string{"item_serialization_failed: " + err.Error()}
		return result
	}

	recorder := httptest.NewRecorder()
	subContext, _ := gin.CreateTestContext(recorder)
	subContext.Request = parent.Request.Clone(parent.Request.Context())
	subContext.Request.Method = http.MethodPost
	subContext.Request.URL = parent.Request.URL
	subContext.Request.ContentLength = int64(len(body))
	subContext.Request.Header = parent.Request.Header.Clone()
	subContext.Request.Body = io.NopCloser(bytes.NewReader(body))
	if parent.Keys != nil {
		subContext.Keys = make(map[any]any, len(parent.Keys))
		for key, value := range parent.Keys {
			subContext.Keys[key] = value
		}
	}
	// The batch surface is a distinct intake source from the plain
	// single-job endpoint: stamp it so the canonical submitter records
	// `intake_source=batch` for every item.
	SetIntakeSource(subContext, creatorflow.IntakeSourceBatch)

	h.SubmitJob()(subContext)
	return batchItemResultFromResponse(result, recorder.Code, recorder.Body.Bytes())
}

func batchItemResultFromResponse(result SubmitJobBatchItemResult, statusCode int, body []byte) SubmitJobBatchItemResult {
	var payload struct {
		Error   string          `json:"error"`
		Message string          `json:"message"`
		JobID   string          `json:"job_id"`
		Details json.RawMessage `json:"details"`
	}
	_ = json.Unmarshal(body, &payload)
	result.JobID = payload.JobID
	switch {
	case statusCode >= 200 && statusCode < 300:
		result.Status = "accepted"
	case statusCode == http.StatusConflict:
		result.Status = "conflict"
	case statusCode >= 500:
		result.Status = "failed"
	default:
		result.Status = "rejected"
	}
	if payload.Error != "" {
		message := payload.Error
		if payload.Message != "" {
			message += ": " + payload.Message
		}
		result.Errors = append(result.Errors, message)
	}
	var details []json.RawMessage
	if len(payload.Details) > 0 && string(payload.Details) != "null" {
		if payload.Details[0] == '[' {
			_ = json.Unmarshal(payload.Details, &details)
		} else {
			details = []json.RawMessage{payload.Details}
		}
	}
	for _, detail := range details {
		var field struct {
			Path  string `json:"path"`
			Issue string `json:"issue"`
		}
		if json.Unmarshal(detail, &field) == nil && (field.Path != "" || field.Issue != "") {
			result.Errors = append(result.Errors, field.Path+": "+field.Issue)
			continue
		}
		// Preserve object-shaped details such as quota/idempotency
		// diagnostics instead of silently dropping reason/observed/cap.
		result.Errors = append(result.Errors, string(detail))
	}
	if result.Status != "accepted" && len(result.Errors) == 0 {
		result.Errors = []string{fmt.Sprintf("http_status_%d", statusCode)}
	}
	return result
}
