package enqueue

import (
	"fmt"
	"strings"
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
//   - delivery_plan of wrong root type (string, int, etc.)

const defaultLegacyRetryBudget = 5

type deliveryPlanShape struct {
	DestinationID string
	Priority      int
	RetryBudget   int
	Enabled       bool
}

// validateDeliveryPlanRequires is the canonical-purity preflight.
// Must be called from PrepareJobAndTask before the Job+TaskSpec is
// handed to the atomic creator; on error, the Job is NOT queued.
func validateDeliveryPlanRequires(payloadMap map[string]interface{}) error {
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
	}
	return nil
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
			RetryBudget:   defaultLegacyRetryBudget,
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

func shapeFromMap(m map[string]interface{}) deliveryPlanShape {
	return deliveryPlanShape{
		DestinationID: firstStringField(m, "destination_id", "id"),
		Priority:      intFromAny(m["priority"]),
		RetryBudget:   intFromAny(m["retry_budget"]),
		Enabled:       boolFromAny(m["enabled"], true),
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
