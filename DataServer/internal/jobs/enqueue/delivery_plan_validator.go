package enqueue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"velox-shared/contract"

	"velox-server/internal/socialclient"
)

// delivery_plan_validator.go — Step 4/8 canonical-purity preflight.
//
// Gates every Job behind an explicit delivery_plan whose per-entry
// retry_budget is > 0. Without this gate, FinalizeVerified discovers
// the missing plan AFTER the render has burned its budget — see the
// diagnostic "Validate delivery plan at enqueue or pre-render".
//
// Mirrors the shape rules from store/parseDeliveryPlanPayload so the
// two stay in lockstep at enqueue and finalize. Intentionally
// self-contained (no store import) to keep the enqueue surface narrow
// and to avoid extending the public store API just for a gate.
//
// Canonical rename note (YouTube → Delivery, PR-15.8):
//
//	YouTubeGroup       → DestinationGroupID   (was: youtube_group_id)
//	YouTubeChannelID   → ExternalDestinationID (was: youtube_channel)
//	YouTubeVideoID     → RemoteMediaID        (was: youtube_video_id)
//	YouTubeURL         → RemoteURL            (was: youtube_published_url)
//	YouTubeStatus      → DeliveryStatus       (was: youtube_publish_status)
//
// The five YouTube-prefixed fields are absent from active Go runtime
// code at this revision. Velox does NOT `SELECT` `youtube_channels`,
// `youtube_oauth_tokens`, or `youtube_groups` anywhere (those tables
// are dropped); destination validation is delegated to the external
// Social API at `POST /internal/v1/destinations/:id/validate` (see
// the optional pre-flight loop in `validateDeliveryPlanRequires`).
//
// Allowed payload shapes (mirroring the store parser):
//   - delivery_plan: []map[string]interface{}{ ... }
//   - delivery_plan: []interface{}{ map[string]interface{}{ ... } }
//   - delivery_plan: map[string]interface{}{ ... }  // single destination
//
// Legacy fallback (kept for backward-compat with consumers that pre-date
// the canonical-purity cut-over, AND because FinalizeVerified honors it):
//   - delivery_destination_ids / destination_ids: []string (retry_budget=5)
//   - delivery_destination_id / destination_id: string  (retry_budget=5)
//
// Rejected (with *validationError):
//   - delivery_plan absent + no legacy fallback
//   - delivery_plan present but empty after snapshot
//   - per-entry retry_budget <= 0
//   - per-entry enabled == false  (same semantics as store parser)
//   - per-entry destination_id missing, empty, or duplicated
//   - per-entry priority < 0
//   - per-entry external_destination_id present AND Social API pre-flight
//     returns ErrPermanent / ErrAuth (4xx) — bad destination, hard fail
//   - per-entry external_destination_id present AND Social API pre-flight
//     returns ErrTransient / ErrNotConfigured (network / not built) —
//     SOFT fail: logged as a warning, enqueue continues (the runner's
//     retry_budget can still recover via FinalizeVerified)
//   - delivery_plan of wrong root type (string, int, etc.)



// DestinationValidator is the minimal contract the enqueue-layer
// validator needs from the Social API boundary. Production wires
// `*socialclient.Client` here; tests can wire a hand-rolled stub.
//
// The contract is intentionally narrow: one method, ctx-aware,
// single-attempt. The validator applies the hard/soft classification
// policy ON TOP of this sentinel so the socialclient stays unaware of
// enqueue semantics.
type DestinationValidator interface {
	ValidateDestination(ctx context.Context, socialDestID string) error
}

// noopDestinationValidator is the default validator used when no
// *socialclient.Client has been wired in (legacy consumers, dev
// mode without a Social API configured). It short-circuits the
// per-entry pre-flight loop and skips any Social API call so the
// existing happy-path unit tests still pass without DI plumbing.
type noopDestinationValidator struct{}

func (noopDestinationValidator) ValidateDestination(ctx context.Context, socialDestID string) error {
	return nil
}

// deliveryPlanShape carries an optional ExternalDestinationID (canonical,
// opaque-mode identifier post-Residuo 4) + Platform per entry. Both are
// sourced from the operator-set delivery_plan payload and are
// FORWARD-ONLY: Velox does NOT validate `platform` semantics (the
// social_repo is the authoritative owner of platform semantics).
// ExternalDestinationID is used solely to delegate the destination
// validation step to `POST /internal/v1/destinations/:id/validate` when
// an opaque id is present.
//
// Mapping rationale:
//   - DestinationID → the Velox-side row in `delivery_destinations`
//   - ExternalDestinationID → the social_repo-side opaque identifier
//     (canonical). Optional; missing means "do not pre-flight"
//     (legacy / drive destinations).
//   - Platform → the target platform string (e.g. "youtube",
//     "tiktok", "instagram"); forwarded to the social_repo verbatim.
//
// Residuo 5 (this commit): the deprecated typed alias for the opaque
// identifier is removed from the shape. The single canonical typed slot
// is `ExternalDestinationID`. The legacy `social_destination_id` JSON
// payload key remains accepted as a back-compat read in shapeFromMap;
// both keys funnel into the canonical ExternalDestinationID slot.
type deliveryPlanShape struct {
	DestinationID         string
	Priority              int
	RetryBudget           int
	Enabled               bool
	ExternalDestinationID string
	Platform              string
}

// validateDeliveryPlanRequires is the canonical-purity preflight.
// Must be called from PrepareJobAndTask before the Job+TaskSpec is
// handed to the atomic creator; on error, the Job is NOT queued.
//
// The optional `validator` parameter performs a per-entry pre-flight
// against the external Social API (`POST /internal/v1/destinations/:id/validate`).
// Plug it in via Enqueuer.WithSocialValidator at the composition root;
// pass `nil` (or the bundled `noopDestinationValidator{}`) for the
// legacy paths that bypass the social_repo boundary (Drive-only,
// pre-rollout dev mode).
//
// Sentinel handling on the per-entry pre-flight loop:
//
//	nil                       → OK, proceed.
//	ErrPermanent / ErrAuth    → HARD fail: bad / unauthorized destination,
//	                            enqueue is rejected with a validationError.
//	ErrTransient / ErrRateLimit / ErrNotConfigured
//	                          → SOFT warn: log and continue; the runner's
//	                            per-destination retry_budget will re-resolve
//	                            at FinalizeVerified.
func validateDeliveryPlanRequires(ctx context.Context, payloadMap map[string]interface{}, validator DestinationValidator) error {
	if payloadMap == nil {
		return &validationError{
			field:   "delivery_plan",
			message: "is required for canonical-purity enqueue (no Job is scheduled without one)",
		}
	}

	entries, err := extractDeliveryPlanShape(payloadMap)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return &validationError{
			field:   "delivery_plan",
			message: "is required for canonical-purity enqueue (provide delivery_plan or delivery_destination_ids)",
		}
	}

	if validator == nil {
		validator = noopDestinationValidator{}
	}

	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		id := strings.TrimSpace(e.DestinationID)
		if id == "" {
			return &validationError{
				field:   fmt.Sprintf("delivery_plan[%d].destination_id", i),
				message: "is required",
			}
		}
		if _, dup := seen[id]; dup {
			return &validationError{
				field:   fmt.Sprintf("delivery_plan[%d].destination_id", i),
				message: fmt.Sprintf("duplicate destination_id %q", id),
			}
		}
		seen[id] = struct{}{}
		if !e.Enabled {
			return &validationError{
				field:   fmt.Sprintf("delivery_plan[%d]", i),
				message: "is disabled; omit it instead of creating a non-routable plan",
			}
		}
		if e.RetryBudget <= 0 {
			return &validationError{
				field:   fmt.Sprintf("delivery_plan[%d].retry_budget", i),
				message: "must be > 0",
			}
		}
		if e.Priority < 0 {
			return &validationError{
				field:   fmt.Sprintf("delivery_plan[%d].priority", i),
				message: "must be >= 0",
			}
		}
		// Per-entry pre-flight against the social_repo. Empty
		// external_destination_id (canonical, post-Residuo-4 rename)
		// means "no Social API routing for this entry" (legacy
		// Drive-only destinations) and the loop skips the validation
		// entirely. Residuo 5: the deprecated typed alias has been
		// dropped; the canonical slot covers both the
		// `external_destination_id` and the legacy `social_destination_id`
		// JSON payload keys via shapeFromMap's firstStringField fallback.
		socialDestID := strings.TrimSpace(e.ExternalDestinationID)
		if socialDestID != "" {
			if perr := validator.ValidateDestination(ctx, socialDestID); perr != nil {
				switch {
				case errors.Is(perr, socialclient.ErrPermanent),
					errors.Is(perr, socialclient.ErrAuth):
					return &validationError{
						field: msgExternalDestinationIDAt(i),
						message: fmt.Sprintf("social destination %q rejected by social_repo (%v); enqueue refused to keep the job from becoming visibly un-routable",
							socialDestID, perr),
						wrapped: perr,
					}
				default:
					// Soft: ErrTransient / ErrRateLimit / ErrNotConfigured.
					// Log a warning and continue. The DeliveryRunner's
					// retry_budget at finalize is the recovery path.
					log.Printf("[PREFLIGHT][enqueue] external_destination_id=%q for destination_id=%q skipped: %v (soft: enqueue continues; runner will re-attempt at finalize)",
						socialDestID, id, perr)
				}
			}
		}
	}
	return nil
}

// validateDeliveryPlanShapeOnly is the pure payload-shape validator,
// preserved for the canonical-purity gate on paths where the Social
// API boundary is intentionally NOT exercised (dev mode without a
// configured social_repo, legacy consumers, fuzz harnesses). It is
// the same loop validateDeliveryPlanRequires runs minus the
// per-entry pre-flight. Callers that want both shape + pre-flight
// must route through validateDeliveryPlanRequires.
func validateDeliveryPlanShapeOnly(payloadMap map[string]interface{}) error {
	return validateDeliveryPlanRequires(context.Background(), payloadMap, nil)
}

// extractDeliveryPlanShape walks the same shape rules as
// store.parseDeliveryPlanPayload but returns a flat slice of validated
// shapes without committing to any storage representation. The legacy
// fallback is honored because FinalizeVerified honors it too — rejecting
// here would break consumers that the gate is supposed to PROTECT, not
// block.
func extractDeliveryPlanShape(payloadMap map[string]interface{}) ([]deliveryPlanShape, error) {
	if raw, present := payloadMap["delivery_plan"]; present && raw != nil {
		switch value := raw.(type) {
		case []interface{}:
			out := make([]deliveryPlanShape, 0, len(value))
			for i, item := range value {
				m, ok := item.(map[string]interface{})
				if !ok {
					return nil, &validationError{
						field:   fmt.Sprintf("delivery_plan[%d]", i),
						message: "must be an object",
					}
				}
				out = append(out, shapeFromMap(m))
			}
			return out, nil
		case []map[string]interface{}:
			out := make([]deliveryPlanShape, 0, len(value))
			for _, item := range value {
				out = append(out, shapeFromMap(item))
			}
			return out, nil
		case map[string]interface{}:
			return []deliveryPlanShape{shapeFromMap(value)}, nil
		default:
			return nil, &validationError{
				field:   "delivery_plan",
				message: "must be an object or array of objects",
			}
		}
	}

	// Legacy fallback mirrors store.deliveryDestinationIDs.
	ids, err := extractLegacyDestinationIDs(payloadMap)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]deliveryPlanShape, 0, len(ids))
	for i, id := range ids {
		out = append(out, deliveryPlanShape{
			DestinationID: id,
			Priority:      i,
			RetryBudget:   contract.DefaultDeliveryRetryBudget,
			Enabled:       true,
		})
	}
	return out, nil
}

func extractLegacyDestinationIDs(payloadMap map[string]interface{}) ([]string, error) {
	for _, key := range []string{"delivery_destination_ids", "destination_ids"} {
		raw, exists := payloadMap[key]
		if !exists || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case []string:
			out := make([]string, 0, len(v))
			for i, s := range v {
				trimmed := strings.TrimSpace(s)
				if trimmed == "" {
					return nil, &validationError{
						field:   fmt.Sprintf("%s[%d]", key, i),
						message: "destination id is empty",
					}
				}
				out = append(out, trimmed)
			}
			return out, nil
		case []interface{}:
			out := make([]string, 0, len(v))
			for i, item := range v {
				s, ok := item.(string)
				if !ok {
					return nil, &validationError{
						field:   fmt.Sprintf("%s[%d]", key, i),
						message: "must be a non-empty string",
					}
				}
				trimmed := strings.TrimSpace(s)
				if trimmed == "" {
					return nil, &validationError{
						field:   fmt.Sprintf("%s[%d]", key, i),
						message: "destination id is empty",
					}
				}
				out = append(out, trimmed)
			}
			return out, nil
		default:
			return nil, &validationError{
				field:   key,
				message: "must be an array of strings",
			}
		}
	}
	if id := firstStringField(payloadMap, "delivery_destination_id", "destination_id"); id != "" {
		return []string{strings.TrimSpace(id)}, nil
	}
	return nil, nil
}

func firstStringField(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok {
			return v
		}
	}
	return ""
}

// msgExternalDestinationIDAt returns the canonical field path used in
// validationError messages for the social_repo opaque identifier at
// index i in the delivery_plan array. Renamed from the legacy
// `delivery_plan[0].social_destination_id` (Residuo 4 step 2 — validator).
func msgExternalDestinationIDAt(i int) string {
	return fmt.Sprintf("delivery_plan[%d].external_destination_id", i)
}

func shapeFromMap(m map[string]interface{}) deliveryPlanShape {
	// Canonical `external_destination_id` (Residuo 4) is the primary JSON
	// key. The legacy `social_destination_id` payload key is still
	// accepted for backward-compat reads of operator-submitted delivery_plan
	// payloads (the field has dropped from the TYPED shape in Residuo 5
	// but the wire-key acceptance is intentionally preserved so pre-rename
	// operator payloads keep working). Both keys funnel into the same
	// canonical ExternalDestinationID slot — no second typed field is
	// needed.
	externalDestID := firstStringField(m, "external_destination_id", "social_destination_id")
	return deliveryPlanShape{
		DestinationID:         firstStringField(m, "destination_id", "id"),
		Priority:              intFromAny(m["priority"]),
		RetryBudget:           intFromAny(m["retry_budget"]),
		Enabled:               boolFromAny(m["enabled"], true),
		ExternalDestinationID: externalDestID,
		Platform:              firstStringField(m, "platform"),
	}
}

func intFromAny(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int8:
		return int(x)
	case int16:
		return int(x)
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint:
		return int(x)
	case uint8:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case float32:
		return int(x)
	case float64:
		return int(x)
	default:
		return 0
	}
}

func boolFromAny(v interface{}, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
}
