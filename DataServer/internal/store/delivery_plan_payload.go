// Package store / delivery_plan_payload.go
//
// Parser bridge for the at-rest delivery_plan envelope. The shape
// rules + validation + cross-package field-path format now live in
// the canonical shared package:
//
//	shared/contract/deliveryplan
//
// This file owns the SQLite-persistence projection of the canonical
// Entry — specifically the metadata JSON-marshal into a string
// column — and re-exports the typed error + accessors so the
// existing creatorflow.WriteResolverError and store test rig keep
// working through the type alias.
//
// Canonical rename note: YouTube-prefixed delivery field names are
// retired. The durable delivery row owns the provider-neutral
// `remote_id` + `last_remote_status`, and platform metadata is owned
// by the social_repo. Do NOT reintroduce YouTube-prefixed fields
// into this struct.
// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

package store

import (
	"encoding/json"
	"errors"

	"velox-shared/contract/deliveryplan"
	"velox-shared/contract/domain"

	"google.golang.org/grpc/codes"
)

// =============================================================================
// Typed validation errors — TYPE ALIAS to the shared package.
// =============================================================================
//
// Parse bridges to deliveryplan.Parse which emits *ValidationError.
// The typed error stays in the shared package so the enqueue and
// store validators cannot drift on the cross-package field-path
// format; this file re-exports the alias under the historical
// store-side name so the existing creatorflow.WriteResolverError
// detection path (errors.As + .Field()) is unchanged on the wire.
//
// P0 history (commit 72a455c + 6a16296 + 44fc166 + 9692e1d:
// P0 contracts on POST /api/v1/jobs, retry_budget typed rejection,
// legacy-body-shape warning, tight assertion on details[0].path +
// details[0].issue): the typed-error contract was the P0 fix for
// the plaintext-error-classification downgrade (a 422 invalid_payload
// that fell through to a 500 resolver_failure because the validator
// emitted fmt.Errorf with a substring, not a typed shape). The
// shared package consolidates the fix; the alias below preserves
// the historical import path so the caller contract is stable.

// DeliveryPlanValidationError is the alias to the canonical typed
// validator rejection emitted by deliveryplan.Parse. The exported
// name is preserved verbatim so creatorflow.WriteResolverError and
// the resolver_http_errors_test rig continue to detect it through
// `errors.As(err, &store.DeliveryPlanValidationError{})`.
type DeliveryPlanValidationError = deliveryplan.ValidationError

// DeliveryPlanValidationField returns the structured field path of
// the *DeliveryPlanValidationError wrapped inside err, or "" if
// err is not a DeliveryPlanValidationError. Exposed as a
// package-level helper so creatorflow.WriteResolverError can
// extract the field path without reaching into the unexported
// struct (which is now in the shared package).
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

// NewDeliveryPlanValidationError builds a
// DeliveryPlanValidationError with the given field path and
// message. Exposed so cross-package callers (creatorflow's
// resolver_http_errors_test rig, future integration_test golden
// assertions) can construct instances for end-to-end
// classification testing without having to reach into the
// unexported struct fields. Production callers reach for
// deliveryplan.Parse + deliveryplan.NewValidationError directly;
// this re-export is the official constructor for tests + any
// future site that builds a typed error from outside the parser
// path.
func NewDeliveryPlanValidationError(field, message string) *DeliveryPlanValidationError {
	return deliveryplan.NewValidationError(field, message)
}

// =============================================================================
// SQLite-persistence projection
// =============================================================================

// deliveryPlanEntry is the at-rest shape persisted into
// job_delivery_plans. It mirrors a subset of the shared
// deliveryplan.Entry — specifically the columns durable at
// finalize time (destination_id, priority, retry_budget,
// metadata_json). Fields that are validator-only (Enabled,
// ExternalDestinationID, Platform) are deliberately NOT carried
// here; those belong to the wire-side contract and are re-applied
// downstream by the delivery runner.
type deliveryPlanEntry struct {
	DestinationID string
	PublicationID string
	Priority      int
	RetryBudget   int
	MetadataJSON  string
}

// parseDeliveryPlanPayload bridges the wire-shape payload map to the
// at-rest deliveryPlanEntry slice. The shape rules + cross-package
// field-path format come from deliveryplan.Parse; this function
// only owns the JSON-marshal of the canonical Entry.Metadata into
// the SQLite string column.
//
// On error, the deliverable is a typed *DeliveryPlanValidationError
// (alias for deliveryplan.ValidationError) carrying the
// "delivery_plan.N.<field>" path. The HTTP envelope layer
// (creatorflow.WriteResolverError) detects this typed error via
// errors.As + .Field() and emits 422 invalid_payload with
// details[0].path. Without the typed-error contract the error
// falls through to the default 500 resolver_failure branch — the
// P0 (commit 72a455c) downgrade that this typed surface closed.
func parseDeliveryPlanPayload(payload map[string]interface{}) ([]deliveryPlanEntry, error) {
	if payload == nil {
		return nil, nil
	}
	if renderOnly, ok := payload["render_only"].(bool); ok && renderOnly {
		// Render-only is an explicit no-delivery contract. Keep the
		// parser authoritative for normal jobs, but do not manufacture a
		// delivery-target error for this intentional control-plane mode.
		return nil, nil
	}

	entries, err := deliveryplan.Parse(payload)
	if err != nil {
		// Surface as-is — the typed error already carries the
		// canonical field path. The bridge here is 1:1.
		return nil, err
	}

	out := make([]deliveryPlanEntry, 0, len(entries))
	for _, e := range entries {
		metadataJSON, merr := marshalEntryMetadata(e.Metadata)
		if merr != nil {
			// Metadata marshal is the only failure mode that's
			// specific to the persistence projection; outside
			// the canonical validator. Emit as a typed error so
			// the chain (errors.Is + Unwrap) sees the marshal
			// failure cause, not just a substring.
			return nil, &domain.DomainError{
				Code:        domain.CodeInvalidPayload,
				Field:       "delivery_plan.metadata",
				Issue:       "serialization",
				Retryable:   false,
				PublicText:  "delivery plan metadata could not be serialized",
				Cause:       merr,
				HTTPStatus:  422,
				GRPCCode:    codes.InvalidArgument,
				FailureCode: domain.FailureInvalidPayload,
				MetricCode:  domain.MetricInvalidPayload,
				AuditAction: domain.AuditDeliveryPlanRejected,
				Component:   domain.ComponentEnqueue,
				Phase:       domain.PhaseValidation,
			}
		}
		out = append(out, deliveryPlanEntry{
			DestinationID: e.DestinationID,
			PublicationID: e.PublicationID,
			Priority:      e.Priority,
			RetryBudget:   e.RetryBudget,
			MetadataJSON:  metadataJSON,
		})
	}
	return out, nil
}

// marshalEntryMetadata JSON-encodes the canonical Entry.Metadata
// map into the SQLite string column. Nil metadata maps to "{}"
// (the legacy alias behaviour pinned by the previous store
// parser; this preserves the no-metadata rejection path the
// parse-time tests relied on).
func marshalEntryMetadata(metadata map[string]interface{}) (string, error) {
	if metadata == nil {
		return "{}", nil
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
