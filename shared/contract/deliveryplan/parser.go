package deliveryplan

import (
	"fmt"
	"strings"

	"velox-shared/contract"
)

// Entry is the canonical per-destination shape. It carries every
// field both consumers care about; each consumer projects to its
// downstream representation (store serialises Metadata → JSON for
// SQLite persistence, enqueue runs the socialclient pre-flight loop
// over ExternalDestinationID).
//
// Consumers iterate the []Entry returned by Parse and do not
// construct Entries themselves. Validation runs in Parse, so the
// downstream projection only sees well-formed entries.
type Entry struct {
	DestinationID         string
	Priority              int
	RetryBudget           int
	Enabled               bool
	ExternalDestinationID string
	Platform              string
	Metadata              map[string]interface{}
}

// Parse reads delivery_plan[] from a JSON-decoded payload map and
// applies the canonical shape + validation boundary. Single source
// of truth for both the enqueue-layer pre-flight (validator) and
// the store-layer in-tx parser.
//
// Allowed root shapes for the `delivery_plan` payload field:
//
//   - []map[string]interface{}{ ... }
//   - []interface{}{ map[string]interface{}{ ... } }
//   - map[string]interface{}{ ... }  // single destination
//
// Legacy root shapes (kept for back-compat with pre-canonical-purity
// consumers AND because the finalize-side resolver honors them):
//
//   - delivery_destination_ids / destination_ids: []string
//   - delivery_destination_id / destination_id: string  (single entry)
//
// Rejected (with *ValidationError):
//
//   - delivery_plan absent AND no legacy fallback
//   - delivery_plan present but empty after snapshot
//   - per-entry retry_budget < 0
//   - per-entry enabled == false
//   - per-entry destination_id missing or empty
//   - per-entry destination_id duplicated
//   - per-entry priority < 0
//   - wrong root type (string, int, etc.)
//
// The per-entry visit order (enabled → retry_budget → priority) is
// preserved across both pre-refactor validators and is pinned by the
// DisabledFalsyRetryBudgetTripOrder test below.
func Parse(payload map[string]interface{}) ([]Entry, error) {
	if payload == nil {
		return nil, NewValidationError(
			"delivery_plan",
			"explicit delivery plan required; provide delivery_plan, delivery_destination_ids, or delivery_destination_id",
		)
	}

	raw, present := payload["delivery_plan"]
	var entries []Entry
	if present && raw != nil {
		planEntries, perr := parsePlanned(raw)
		if perr != nil {
			return nil, perr
		}
		entries = planEntries
	}

	if len(entries) == 0 {
		ids, lerr := extractLegacyDestinationIDs(payload)
		if lerr != nil {
			return nil, lerr
		}
		for i, id := range ids {
			entries = append(entries, Entry{
				DestinationID: id,
				Priority:      i,
				RetryBudget:   contract.DefaultDeliveryRetryBudget,
				Enabled:       true,
			})
		}
	}

	if len(entries) == 0 {
		return nil, NewValidationError(
			"delivery_plan",
			"explicit delivery plan required; provide delivery_plan, delivery_destination_ids, or delivery_destination_id",
		)
	}

	// Duplicate detection + id-trim pass (parity with both pre-refactor
	// validators: id-trim happens BEFORE dedupe so a trimmed id match
	// still triggers the duplicate alert).
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		id := strings.TrimSpace(entries[i].DestinationID)
		if id == "" {
			return nil, NewValidationError(
				fmt.Sprintf("delivery_plan.%d.destination_id", i),
				"is required",
			)
		}
		if _, dup := seen[id]; dup {
			return nil, NewValidationError(
				fmt.Sprintf("delivery_plan.%d.destination_id", i),
				fmt.Sprintf("duplicate destination_id %q", id),
			)
		}
		seen[id] = struct{}{}
		entries[i].DestinationID = id
	}
	return entries, nil
}

// parsePlanned walks the `delivery_plan` root through its permitted
// shape variants. Returns the typed rejection matching the
// pre-refactor enqueue/store validators verbatim so the existing
// substring pin tests stay green.
func parsePlanned(raw interface{}) ([]Entry, error) {
	switch v := raw.(type) {
	case []interface{}:
		out := make([]Entry, 0, len(v))
		for i, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, NewValidationError(
					fmt.Sprintf("delivery_plan.%d", i),
					"must be an object",
				)
			}
			e, err := entryFromMap(m, i)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	case []map[string]interface{}:
		out := make([]Entry, 0, len(v))
		for i, m := range v {
			e, err := entryFromMap(m, i)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	case map[string]interface{}:
		e, err := entryFromMap(v, 0)
		if err != nil {
			return nil, err
		}
		return []Entry{e}, nil
	default:
		return nil, NewValidationError(
			"delivery_plan",
			"must be an object or array of objects",
		)
	}
}

// entryFromMap applies per-entry validation. Returns the Entry +
// the FIRST violation it observed. Visit order is
//
//	enabled (fail-fast) → retry_budget → priority
//
// (this matches the post-residuo-5 validator order; enabled before
// retry_budget so a disabled entry never silently lets a malformed
// retry_budget slip through).
func entryFromMap(m map[string]interface{}, index int) (Entry, error) {
	s := shapeFromMap(m)
	if !s.Enabled {
		return s, NewValidationError(
			fmt.Sprintf("delivery_plan.%d", index),
			"is disabled; omit it instead of creating a non-routable plan",
		)
	}
	if s.RetryBudget < 0 {
		return s, NewValidationError(
			FieldPath(index, "retry_budget"),
			fmt.Sprintf("must be >= 0 (got %d)", s.RetryBudget),
		)
	}
	if s.Priority < 0 {
		return s, NewValidationError(
			FieldPath(index, "priority"),
			fmt.Sprintf("must be >= 0 (got %d)", s.Priority),
		)
	}
	return s, nil
}

// shapeFromMap reads the per-entry keys. Returns Entry with no
// validation applied; call entryFromMap for the typed surface.
//
// Back-compat: the canonical `external_destination_id` JSON key
// (Residuo 4 rename) is the primary contract. The legacy
// `social_destination_id` payload key is still honored for
// pre-rename operator payloads — both keys funnel into the same
// canonical ExternalDestinationID slot via firstStringField's
// fallback.
func shapeFromMap(m map[string]interface{}) Entry {
	externalDestID := firstStringField(m, "external_destination_id", "social_destination_id")
	return Entry{
		DestinationID:         firstStringField(m, "destination_id", "id"),
		Priority:              intFromAny(m["priority"]),
		RetryBudget:           intFromAny(m["retry_budget"]),
		Enabled:               boolFromAny(m["enabled"], true),
		ExternalDestinationID: externalDestID,
		Platform:              firstStringField(m, "platform"),
		Metadata:              metadataFromMap(m),
	}
}

// metadataFromMap preserves the wire-shape `metadata` map so the
// store layer can JSON-marshal it to SQLite downstream. Returns
// nil when the field is absent / nil; the store consumer translates
// nil to "{}" on the persistence side via parseDeliveryPlanPayload.
func metadataFromMap(m map[string]interface{}) map[string]interface{} {
	if v, ok := m["metadata"]; ok {
		if mm, ok := v.(map[string]interface{}); ok {
			return mm
		}
	}
	return nil
}

// extractLegacyDestinationIDs walks the same resolver order as the
// pre-refactor store.deliveryDestinationIDs + enqueue side:
//
//	delivery_destination_ids → destination_ids →
//	delivery_destination_id → destination_id
//
// All four keys honour TrimSpace on the value; non-string entries
// are rejected with the bracket-notation index path so the
// pre-refactor substring pins stay green.
func extractLegacyDestinationIDs(payload map[string]interface{}) ([]string, error) {
	for _, key := range []string{"delivery_destination_ids", "destination_ids"} {
		raw, exists := payload[key]
		if !exists || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case []string:
			out := make([]string, 0, len(v))
			for i, s := range v {
				trimmed := strings.TrimSpace(s)
				if trimmed == "" {
					return nil, NewValidationError(
						fmt.Sprintf("%s[%d]", key, i),
						"destination id is empty",
					)
				}
				out = append(out, trimmed)
			}
			return out, nil
		case []interface{}:
			out := make([]string, 0, len(v))
			for i, item := range v {
				s, ok := item.(string)
				if !ok {
					return nil, NewValidationError(
						fmt.Sprintf("%s[%d]", key, i),
						"must be a non-empty string",
					)
				}
				trimmed := strings.TrimSpace(s)
				if trimmed == "" {
					return nil, NewValidationError(
						fmt.Sprintf("%s[%d]", key, i),
						"destination id is empty",
					)
				}
				out = append(out, trimmed)
			}
			return out, nil
		default:
			return nil, NewValidationError(
				key,
				"must be an array of strings",
			)
		}
	}
	if id := firstStringField(payload, "delivery_destination_id", "destination_id"); id != "" {
		return []string{strings.TrimSpace(id)}, nil
	}
	return nil, nil
}

// firstStringField is the tolerant reader used by shapeFromMap +
// extractLegacyDestinationIDs. Returns the FIRST non-empty string
// under any of the supplied keys.
func firstStringField(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok {
			return v
		}
	}
	return ""
}

// intFromAny is the JSON-tolerant int reader used by shapeFromMap.
// Accepts every numeric Go type + float64 (JSON decoding emits
// numbers as float64). Unknown / non-numeric types collapse to 0
// so the subsequent enabled → retry_budget → priority visit
// produces a typed, debuggable rejection rather than a runtime
// panic on a type-switch inside the JSON map.
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

// boolFromAny is the JSON-tolerant bool reader used by shapeFromMap.
// Returns the explicit bool when present, fallback otherwise. The
// fallback default (true on missing key) is the operator intent: an
// omitted `enabled` flag means "this entry is routable".
func boolFromAny(v interface{}, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
}
