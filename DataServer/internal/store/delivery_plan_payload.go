// Package store / delivery_plan_payload.go
//
// Canonical delivery-plan parser shared by the atomic Job+Task creator
// (internal/store/atomic_job_task.go::CreateJobWithTaskTx) and the
// finalize-side resolver (internal/deliveries/plan_resolver.go).
//
// Canonical rename note (YouTube → Delivery, PR-15.8):
//
//	YouTubeGroup       → DestinationGroupID   (was: youtube_group_id)
//	YouTubeChannelID   → ExternalDestinationID (was: youtube_channel; column on delivery_destinations)
//	YouTubeVideoID     → RemoteMediaID        (was: youtube_video_id; persisted on job_deliveries.remote_id)
//	YouTubeURL         → RemoteURL            (was: youtube_published_url)
//	YouTubeStatus      → DeliveryStatus       (was: youtube_publish_status)
//
// All five YouTube-prefixed field names are absent from active Go
// runtime code at this revision (verified by commit 7f8d3a4 +
// post-PR-15.8 grep): the per-row model owns `destination_id` +
// `metadata_json` and the durable delivery row owns `remote_id` +
// `last_remote_status`. Velox no longer `SELECT`s `youtube_channels`,
// `youtube_oauth_tokens`, or `youtube_groups` (those tables are
// dropped in migration 090 and the social_repo is the authoritative
// owner of platform metadata). Any future contributor reintroducing a
// YouTube-prefixed field into this struct must replace it with the
// canonical Destination-prefixed equivalent above AND open a new
// migration; do NOT add it as an additional name.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-shared/contract"
)

// =============================================================================
// Typed validation errors
// =============================================================================
//
// parseDeliveryPlanPayload + deliveryPlanEntryFromMap emit
// *DeliveryPlanValidationError (an exported analogue of
// enqueue.validationError, but kept in this package because store
// cannot import enqueue — the dep edge goes enqueue -> store). The
// typed error gives creatorflow.WriteResolverError a structured field
// path to surface as `details[0].path` on 422 invalid_payload,
// closing the same plaintext-error-classification pattern that P0
// (commit 72a455c) closed for destination-existence.
//
// The three getters Field() / Message() / Unwrap() mirror the
// enqueue.validationError API surface so the cross-package detection
// in creatorflow (errors.As + Field()) is symmetric. The infra
// supports future conversions of additional plaintext fmt.Errorf
// sites in this file to typed errors with the same plumbing.

// DeliveryPlanValidationError is the typed error returned by the
// canonical delivery-plan parser when a per-entry shape constraint
// fails (e.g. retry_budget < 0, priority < 0, destination_id
// missing, duplicate destination_id, disabled entry, metadata
// serialization failure). It mirrors enqueue.validationError's
// API surface (Field() / Message() / Unwrap()) so that
// creatorflow.WriteResolverError can classify it via
// DeliveryPlanValidationField(err) and emit a 422 invalid_payload
// envelope with `details[0].path = <Field()>` instead of falling
// through to the default 500 resolver_failure.
//
// The struct fields are intentionally unexported (with exported
// getters) so a future field rename cannot silently drift from
// the JSON-pointer-style path used by the HTTP envelope. Cross-
// package callers reach the field path only via Field() or via
// the package-level DeliveryPlanValidationField helper.
type DeliveryPlanValidationError struct {
	field   string
	message string
	wrapped error // optional underlying cause
}

// Error returns the canonical "field: message" formatting used by
// the enqueue.validationError twin. This is the value matched by
// the legacy strings.Contains checks in the existing
// parseDeliveryPlanPayload tests so the transition from
// fmt.Errorf to typed errors does not break test substring
// assertions (test contract is preserved verbatim).
func (e *DeliveryPlanValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.field + ": " + e.message
}

// Field returns the structured field path (e.g.
// "delivery_plan[0].retry_budget"). Exposed via a getter so the
// unexported `field` stays a private invariant while cross-package
// callers (creatorflow.WriteResolverError) can still reach the
// path via errors.As + .Field().
func (e *DeliveryPlanValidationError) Field() string {
	if e == nil {
		return ""
	}
	return e.field
}

// Message returns the human-readable rejection message WITHOUT the
// field-path prefix (use Error() if you want the field+message
// concatenation). Exposed via a getter for the same reason as
// Field.
func (e *DeliveryPlanValidationError) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

// Unwrap returns the underlying cause so errors.Is / errors.As can
// inspect the original error (e.g. a JSON marshal failure from
// metadata encoding). Without this, callers can only inspect the
// formatted message, which is fragile across message refactors.
func (e *DeliveryPlanValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.wrapped
}

// DeliveryPlanValidationField returns the structured field path
// (e.g. "delivery_plan[0].retry_budget") of the
// *DeliveryPlanValidationError wrapped inside err, or "" if err is
// not a DeliveryPlanValidationError. Exposed as a package-level
// helper so creatorflow.WriteResolverError can extract the field
// path without reaching into the unexported struct.
//
// Typical usage in creatorflow:
//
//	if got := store.DeliveryPlanValidationField(err); got != "" {
//	    // 422 + details[0].path = got
//	}
//
// This helper intentionally returns "" (not an error) on a
// non-DeliveryPlanValidationError input so callers can use it in
// expression position without short-circuiting their flow. The
// enqueue-side analogue (enqueue.ValidationErrorField) follows the
// same convention.
func DeliveryPlanValidationField(err error) string {
	if err == nil {
		return ""
	}
	var derr *DeliveryPlanValidationError
	if errors.As(err, &derr) && derr != nil {
		return derr.Field()
	}
	return ""
}

// NewDeliveryPlanValidationError builds a DeliveryPlanValidationError
// with the given field path and message. Exposed so cross-package
// callers (creatorflow's resolver_http_errors_test rig, future
// integration_test golden assertions) can construct instances for
// end-to-end classification testing without having to reach into
// the unexported struct fields. Production callers (the
// parseDeliveryPlanPayload + deliveryPlanEntryFromMap helpers)
// construct directly via struct literal; this constructor is the
// official constructor for tests + any future site that builds a
// typed error from outside the parser path.
func NewDeliveryPlanValidationError(field, message string) *DeliveryPlanValidationError {
	return &DeliveryPlanValidationError{field: field, message: message}
}

type deliveryPlanEntry struct {
	DestinationID string
	Priority      int
	RetryBudget   int
	MetadataJSON  string
}

func parseDeliveryPlanPayload(payload map[string]interface{}) ([]deliveryPlanEntry, error) {
	if payload == nil {
		return nil, nil
	}

	var entries []deliveryPlanEntry
	if raw, ok := payload["delivery_plan"]; ok && raw != nil {
		switch value := raw.(type) {
		case []interface{}:
			for i, item := range value {
				entryMap, ok := item.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("delivery_plan[%d] must be an object", i)
				}
				entry, err := deliveryPlanEntryFromMap(entryMap, i)
				if err != nil {
					return nil, err
				}
				entries = append(entries, entry)
			}
		case []map[string]interface{}:
			for i, entryMap := range value {
				entry, err := deliveryPlanEntryFromMap(entryMap, i)
				if err != nil {
					return nil, err
				}
				entries = append(entries, entry)
			}
		case map[string]interface{}:
			entry, err := deliveryPlanEntryFromMap(value, 0)
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		default:
			return nil, fmt.Errorf("delivery_plan must be an object or array of objects")
		}
	}

	if len(entries) == 0 {
		ids, err := deliveryDestinationIDs(payload)
		if err != nil {
			return nil, err
		}
		for i, id := range ids {
			entries = append(entries, deliveryPlanEntry{
				DestinationID: id,
				Priority:      i,
				RetryBudget:   contract.DefaultDeliveryRetryBudget,
				MetadataJSON:  "{}",
			})
		}
	}

	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		id := strings.TrimSpace(entries[i].DestinationID)
		if id == "" {
			return nil, fmt.Errorf("delivery plan entry %d: destination_id is required", i)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("delivery plan entry %d: duplicate destination_id %q", i, id)
		}
		seen[id] = struct{}{}
		entries[i].DestinationID = id
	}
	return entries, nil
}

func deliveryPlanEntryFromMap(value map[string]interface{}, index int) (deliveryPlanEntry, error) {
	id := deliveryPlanFirstString(value, "destination_id", "id")
	if id == "" {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].destination_id is required", index)
	}
	if enabled, ok := deliveryPlanBoolFromMap(value, "enabled"); ok && !enabled {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d] is disabled; omit it instead of creating a non-routable plan", index)
	}

	priority := deliveryPlanIntFromMap(value, "priority", index)
	if priority < 0 {
		return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].priority must be >= 0", index)
	}
	retryBudget := deliveryPlanIntFromMap(value, "retry_budget", contract.DefaultDeliveryRetryBudget)
	// retry_budget=0 is explicitly allowed per the canonical contract
	// (openapi.yaml:SubmitDeliveryPlanEntry.retry_budget.minimum is 0)
	// and round-trips verbatim into job_delivery_plans.retry_budget so
	// the worker terminal-fails on the first hard error — matching the
	// client's explicit "no retries" intent. Only negative values are
	// rejected at the parse layer.
	//
	// The rejection is emitted as a typed *DeliveryPlanValidationError
	// (not fmt.Errorf) so creatorflow.WriteResolverError can classify
	// it via DeliveryPlanValidationField(err) and return 422
	// invalid_payload with details[0].path="delivery_plan[N].retry_budget"
	// — closing the plaintext-error-classification pattern that P0
	// commit 72a455c closed for destination-existence. On the rare
	// path where the in-tx parser fires AFTER the handler-side
	// pre-check has already accepted the request (e.g. a future caller
	// that bypasses SubmitJob), this typed error keeps the API contract
	// stable: 422 invalid_payload with the canonical field path,
	// rather than a 500 resolver_failure downgrade.
	if retryBudget < 0 {
		return deliveryPlanEntry{}, &DeliveryPlanValidationError{
			field:   fmt.Sprintf("delivery_plan[%d].retry_budget", index),
			message: fmt.Sprintf("must be >= 0 (got %d)", retryBudget),
		}
	}

	metadataJSON := "{}"
	if metadata, ok := value["metadata"]; ok && metadata != nil {
		data, err := json.Marshal(metadata)
		if err != nil {
			return deliveryPlanEntry{}, fmt.Errorf("delivery_plan[%d].metadata: %w", index, err)
		}
		metadataJSON = string(data)
	}
	return deliveryPlanEntry{
		DestinationID: id,
		Priority:      priority,
		RetryBudget:   retryBudget,
		MetadataJSON:  metadataJSON,
	}, nil
}

func deliveryDestinationIDs(payload map[string]interface{}) ([]string, error) {
	for _, key := range []string{"delivery_destination_ids", "destination_ids"} {
		if raw, exists := payload[key]; exists && raw != nil {
			switch values := raw.(type) {
			case []string:
				return normalizeDeliveryDestinationIDs(values)
			case []interface{}:
				ids := make([]string, 0, len(values))
				for i, item := range values {
					id, ok := item.(string)
					if !ok || strings.TrimSpace(id) == "" {
						return nil, fmt.Errorf("%s[%d] must be a non-empty string", key, i)
					}
					ids = append(ids, id)
				}
				return normalizeDeliveryDestinationIDs(ids)
			default:
				return nil, fmt.Errorf("%s must be an array of strings", key)
			}
		}
	}
	if id := deliveryPlanFirstString(payload, "delivery_destination_id", "destination_id"); id != "" {
		return []string{id}, nil
	}
	return nil, nil
}

func normalizeDeliveryDestinationIDs(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for i, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("delivery destination id at index %d is empty", i)
		}
		out = append(out, value)
	}
	return out, nil
}
