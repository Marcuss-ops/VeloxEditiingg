/*
Package deliveryplan canonical tests.

These tests reproduce, in lockstep, every shape + validation rule
that the previous two validators enforced:

  - DataServer/internal/jobs/enqueue/delivery_plan_validator_test.go
  - DataServer/internal/store/delivery_plan_payload_invalid_test.go

Moving them here is the single-source-of-truth payoff: future
changes to Parse + ValidationError + FieldPath are verified by ONE
test file, and the consumer-specific tests (enqueue pre-flight,
store typed-error alias integrity) only cover concerns that are
outside the parser's responsibility.

The convention is to test the parser via Parse and the typed error
via direct construction; testing internal helpers (intFromAny,
boolFromAny, firstStringField) is encouraged because the parser
sits at a JSON decode boundary where regression bites quietly.
*/
package deliveryplan

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// =====================================================================
// IntFromAny: every numeric type covered by Parse must parse
// correctly. Non-numeric inputs collapse to 0 (which then falls
// into the relax-retry_budget contract (<0) without tripping
// retry_budget.
// =====================================================================

func TestIntFromAny(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   interface{}
		want int
	}{
		{"nil", nil, 0},
		{"int_positive", int(7), 7},
		{"int_zero", int(0), 0},
		{"int_negative", int(-3), -3},
		{"int8", int8(8), 8},
		{"int16", int16(9), 9},
		{"int32", int32(10), 10},
		{"int64", int64(11), 11},
		{"uint", uint(12), 12},
		{"uint8", uint8(13), 13},
		{"uint16", uint16(14), 14},
		{"uint32", uint32(15), 15},
		{"uint64", uint64(16), 16},
		{"float32_whole_value", float32(17), 17},
		{"float32_truncates", float32(18.7), 18}, // int() truncation
		{"float64_whole_value", float64(19), 19},
		{"float64_truncates_negative", float64(-2.9), -2},
		{"bool_collapses_to_zero", true, 0}, // bool is not numeric
		{"string_collapses_to_zero", "35", 0},
		{"map_collapses_to_zero", map[string]interface{}{}, 0},
		{"slice_collapses_to_zero", []string{}, 0},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := intFromAny(c.in); got != c.want {
				t.Errorf("intFromAny(%v) = %d; want %d", c.in, got, c.want)
			}
		})
	}
}

// =====================================================================
// BoolFromAny: with explicit overrides and default fallback.
// =====================================================================

func TestBoolFromAny(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		in       interface{}
		fallback bool
		want     bool
	}{
		{"true_overrides_default_false", true, false, true},
		{"false_overrides_default_true", false, true, false},
		{"nil_uses_fallback", nil, true, true},
		{"nil_uses_fallback_false", nil, false, false},
		{"int_uses_fallback", int(1), true, true},
		{"string_uses_fallback", "true", true, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := boolFromAny(c.in, c.fallback); got != c.want {
				t.Errorf("boolFromAny(%v, %v) = %v; want %v", c.in, c.fallback, got, c.want)
			}
		})
	}
}

// =====================================================================
// Parse — golden paths.
// =====================================================================

func TestParse_HappyPaths(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   map[string]interface{}
	}{
		{
			name: "array_of_objects",
			in: map[string]interface{}{
				"delivery_plan": []map[string]interface{}{
					{"destination_id": "drive-main", "priority": 0, "retry_budget": 3},
				},
			},
		},
		{
			name: "array_of_interface",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3},
				},
			},
		},
		{
			name: "single_object",
			in: map[string]interface{}{
				"delivery_plan": map[string]interface{}{
					"destination_id": "drive-main", "retry_budget": 5,
				},
			},
		},
		{
			name: "legacy_ids_array_canonical_key",
			in: map[string]interface{}{
				"delivery_destination_ids": []string{"drive-main"},
			},
		},
		{
			name: "legacy_ids_array_alias_key",
			in: map[string]interface{}{
				"destination_ids": []string{"drive-main"},
			},
		},
		{
			name: "legacy_single_id_canonical_key",
			in: map[string]interface{}{
				"delivery_destination_id": "drive-main",
			},
		},
		{
			// retry_budget=0 is now ALLOWED per openapi.yaml:
			// SubmitDeliveryPlanEntry.retry_budget.minimum=0. The
			// explicit-zero contract round-trips downstream as
			// job_delivery_plans.retry_budget=0 so the worker
			// terminal-fails on the first hard error.
			name: "retry_budget_zero_explicit_accepted",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 0},
				},
			},
		},
		{
			// intFromAny coerces unrecognized JSON types (string, bool,
			// nil, …) to 0 via its default branch. Under the
			// relaxed contract (<0), the coerced 0 MUST be
			// accepted just like an explicit numeric 0.
			name: "retry_budget_string_invalid_coerces_to_zero_accepted",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": "abc"},
				},
			},
		},
		{
			name: "legacy_single_id_alias_key",
			in: map[string]interface{}{
				"destination_id": "drive-main",
			},
		},
		{
			name: "multi_destination_with_priority_and_enabled",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "priority": 0, "retry_budget": 5},
					map[string]interface{}{"destination_id": "video-main", "priority": 1, "retry_budget": 7, "enabled": true},
				},
			},
		},
		{
			// Creator-machine nested envelope back-compat (the
			// fix for the regression surfaced by
			// TestSubmitJobE2E_SuccessAndReplay + calendar
			// e2e suite): the Creator frontend wraps the entire
			// Job under a nested "payload" envelope that carries
			// `delivery_plan`. The legacy
			// store.parseDeliveryPlanPayload accepted this
			// nested form by inspection of the inner map. The
			// canonical Parse MUST honour the same nested form
			// so Creator-flow preflight gates don't regress to
			// a canonical "explicit delivery plan required"
			// rejection.
			name: "creator_nested_envelope_array",
			in: map[string]interface{}{
				"payload": map[string]interface{}{
					"delivery_plan": []interface{}{
						map[string]interface{}{"destination_id": "drive-main", "priority": 1, "retry_budget": 3},
					},
				},
			},
		},
		{
			// Same nested form, single object delivery_plan.
			name: "creator_nested_envelope_single_object",
			in: map[string]interface{}{
				"payload": map[string]interface{}{
					"delivery_plan": map[string]interface{}{
						"destination_id": "drive-main",
						"retry_budget":   5,
					},
				},
			},
		},
		{
			// Same nested form, legacy ids alias.
			name: "creator_nested_envelope_legacy_ids",
			in: map[string]interface{}{
				"payload": map[string]interface{}{
					"delivery_destination_ids": []string{"drive-main"},
				},
			},
		},
		{
			// Same nested form, legacy single-id alias.
			name: "creator_nested_envelope_legacy_single_id",
			in: map[string]interface{}{
				"payload": map[string]interface{}{
					"delivery_destination_id": "drive-main",
				},
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Parse(c.in); err != nil {
				t.Errorf("want no error; got %v", err)
			}
		})
	}
}

// =====================================================================
// Parse — every documented rejection mode.
// =====================================================================

func TestParse_RejectPaths(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		in        map[string]interface{}
		wantField string
		wantSub   string
	}{
		{
			name:      "nil_payload",
			in:        nil,
			wantField: "delivery_plan",
			wantSub:   "explicit delivery plan required",
		},
		{
			name:      "empty_payload",
			in:        map[string]interface{}{},
			wantField: "delivery_plan",
			wantSub:   "explicit delivery plan required",
		},
		{
			name: "empty_array",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{},
			},
			wantField: "delivery_plan",
			wantSub:   "explicit delivery plan required",
		},
		{
			name: "non_object_array_entry",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{"drive-main"},
			},
			wantField: "delivery_plan.0",
			wantSub:   "must be an object",
		},
		{
			name: "wrong_root_type_int",
			in: map[string]interface{}{
				"delivery_plan": 42,
			},
			wantField: "delivery_plan",
			wantSub:   "must be an object or array of objects",
		},
		{
			name: "wrong_root_type_string",
			in: map[string]interface{}{
				"delivery_plan": "drive-main",
			},
			wantField: "delivery_plan",
			wantSub:   "must be an object or array of objects",
		},
		{
			name: "missing_destination_id",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"retry_budget": 3},
				},
			},
			wantField: "delivery_plan.0.destination_id",
			wantSub:   "is required",
		},
		{
			name: "empty_destination_id",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "   ", "retry_budget": 3},
				},
			},
			wantField: "delivery_plan.0.destination_id",
			wantSub:   "is required",
		},
		{
			name: "duplicate_destination_id",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3},
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 5},
				},
			},
			wantField: "delivery_plan.1.destination_id",
			wantSub:   "duplicate",
		},
		{
			// Note: retry_budget=0 IS NOW ACCEPTED per openapi.yaml;
			// the rejection-table below only pins negative values.
			name: "retry_budget_negative",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": -3},
				},
			},
			wantField: "delivery_plan.0.retry_budget",
			wantSub:   "must be >= 0",
		},
		{
			name: "disabled_entry",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "enabled": false},
				},
			},
			wantField: "delivery_plan.0",
			wantSub:   "is disabled",
		},
		{
			name: "negative_priority",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": -1},
				},
			},
			wantField: "delivery_plan.0.priority",
			wantSub:   "must be >= 0",
		},
		{
			name: "legacy_ids_array_empty_first",
			in: map[string]interface{}{
				"delivery_destination_ids": []string{"", "valid"},
			},
			wantField: "delivery_destination_ids[0]",
			wantSub:   "destination id is empty",
		},
		{
			name: "legacy_ids_array_wrong_element_type",
			in: map[string]interface{}{
				"delivery_destination_ids": []interface{}{42},
			},
			wantField: "delivery_destination_ids[0]",
			wantSub:   "must be a non-empty string",
		},
		{
			name: "legacy_ids_array_wrong_root_type",
			in: map[string]interface{}{
				"delivery_destination_ids": "drive-main",
			},
			wantField: "delivery_destination_ids",
			wantSub:   "must be an array of strings",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(c.in)
			if err == nil {
				t.Fatalf("want error (field=%s sub=%s); got nil", c.wantField, c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantField) {
				t.Errorf("error %q does not contain field %q", err.Error(), c.wantField)
			}
			if c.wantSub != "" && !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error %q does not contain substring %q", err.Error(), c.wantSub)
			}
		})
	}
}

// =====================================================================
// Parse — visit order for combined rejections.
// =====================================================================

func TestParse_DisabledFalsyRetryBudgetTripOrder(t *testing.T) {
	t.Parallel()
	in := map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-main",
				"retry_budget":   0,
				"enabled":        false,
			},
		},
	}
	_, err := Parse(in)
	if err == nil {
		t.Fatal("want error; got nil")
	}
	// Per entryFromMap's visit order (enabled → retry_budget → priority),
	// enabled=false fails before retry_budget<=0.
	if !strings.Contains(err.Error(), "is disabled") {
		t.Errorf("want 'is disabled' to surface first; got %q", err.Error())
	}
}

// =====================================================================
// Legacy fallback — resolver order parity.
// =====================================================================

func TestExtractLegacyDestinationIDs_ResolverOrder(t *testing.T) {
	t.Parallel()

	t.Run("delivery_destination_ids_beats_destination_ids", func(t *testing.T) {
		t.Parallel()
		in := map[string]interface{}{
			"delivery_destination_ids": []string{"canonical"},
			"destination_ids":          []string{"alias"},
		}
		got, err := extractLegacyDestinationIDs(in)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !equalStrings(got, []string{"canonical"}) {
			t.Errorf("got %v; want [canonical] (delivery_destination_ids wins)", got)
		}
	})

	t.Run("delivery_destination_id_beats_destination_id", func(t *testing.T) {
		t.Parallel()
		in := map[string]interface{}{
			"delivery_destination_id": "primary-single",
			"destination_id":          "alias-single",
		}
		got, err := extractLegacyDestinationIDs(in)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !equalStrings(got, []string{"primary-single"}) {
			t.Errorf("got %v; want [primary-single]", got)
		}
	})

	t.Run("array_wins_over_single_when_both_present", func(t *testing.T) {
		t.Parallel()
		in := map[string]interface{}{
			"delivery_destination_ids": []string{"a", "b"},
			"delivery_destination_id":  "single",
		}
		got, err := extractLegacyDestinationIDs(in)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !equalStrings(got, []string{"a", "b"}) {
			t.Errorf("got %v; want [a b] (array present → array wins; single is fallback)", got)
		}
	})

	t.Run("empty_map_returns_nil_no_error", func(t *testing.T) {
		t.Parallel()
		got, err := extractLegacyDestinationIDs(map[string]interface{}{})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if got != nil {
			t.Errorf("empty map: got %v; want nil", got)
		}
	})

	t.Run("nil_map_returns_nil_no_error", func(t *testing.T) {
		t.Parallel()
		got, err := extractLegacyDestinationIDs(nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if got != nil {
			t.Errorf("nil map: got %v; want nil", got)
		}
	})

	t.Run("interface_slice_normalizes_strings", func(t *testing.T) {
		t.Parallel()
		in := map[string]interface{}{
			"delivery_destination_ids": []interface{}{"a", "b", "  c  "},
		}
		got, err := extractLegacyDestinationIDs(in)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !equalStrings(got, []string{"a", "b", "c"}) {
			t.Errorf("got %v; want [a b c] (interface slice normalized with trim)", got)
		}
	})
}

// =====================================================================
// shapeFromMap equivalents (replaces enqueue's TestShapeFromMap_*).
// =====================================================================

func TestShapeFromMap_DefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	t.Run("missing_destination_id_defaults_empty", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{})
		if s.DestinationID != "" {
			t.Errorf("destination_id = %q; want ''", s.DestinationID)
		}
		if s.RetryBudget != 0 {
			t.Errorf("retry_budget = %d; want 0", s.RetryBudget)
		}
		if s.Priority != 0 {
			t.Errorf("priority = %d; want 0", s.Priority)
		}
		if !s.Enabled {
			t.Errorf("enabled = false; want true (default for missing key)")
		}
	})

	t.Run("alias_id_key_resolved", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{"id": "alias-id"})
		if s.DestinationID != "alias-id" {
			t.Errorf("destination_id via id alias = %q; want alias-id", s.DestinationID)
		}
	})

	t.Run("all_explicit_fields_honored", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{
			"destination_id": "primary",
			"priority":       5,
			"retry_budget":   7,
			"enabled":        true,
		})
		if s.DestinationID != "primary" {
			t.Errorf("destination_id = %q; want primary", s.DestinationID)
		}
		if s.Priority != 5 {
			t.Errorf("priority = %d; want 5", s.Priority)
		}
		if s.RetryBudget != 7 {
			t.Errorf("retry_budget = %d; want 7", s.RetryBudget)
		}
		if !s.Enabled {
			t.Errorf("enabled = false; want true")
		}
	})
}

func TestShapeFromMap_ExternalDestinationIDAndPlatform(t *testing.T) {
	t.Parallel()

	t.Run("defaults_to_empty", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{"destination_id": "drive-main"})
		if s.ExternalDestinationID != "" {
			t.Errorf("external_destination_id = %q; want ''", s.ExternalDestinationID)
		}
		if s.Platform != "" {
			t.Errorf("platform = %q; want ''", s.Platform)
		}
	})

	t.Run("legacy_social_destination_id_key_feeds_canonical", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{
			"destination_id":        "social-amish",
			"social_destination_id": "social_dest_amish",
			"platform":              "youtube",
		})
		if s.ExternalDestinationID != "social_dest_amish" {
			t.Errorf("ExternalDestinationID = %q; want social_dest_amish (legacy JSON key back-compat read)", s.ExternalDestinationID)
		}
		if s.Platform != "youtube" {
			t.Errorf("platform = %q; want youtube", s.Platform)
		}
	})

	t.Run("canonical_external_destination_id_honored", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{
			"destination_id":          "social-amish",
			"external_destination_id": "social_dest_amish",
			"platform":                "youtube",
		})
		if s.ExternalDestinationID != "social_dest_amish" {
			t.Errorf("ExternalDestinationID = %q; want social_dest_amish", s.ExternalDestinationID)
		}
		if s.Platform != "youtube" {
			t.Errorf("Platform = %q; want youtube", s.Platform)
		}
	})

	t.Run("canonical_wins_over_legacy_key_when_both_present", func(t *testing.T) {
		t.Parallel()
		s := shapeFromMap(map[string]interface{}{
			"destination_id":          "social-amish",
			"external_destination_id": "canonical_id",
			"social_destination_id":   "legacy_id",
			"platform":                "youtube",
		})
		if s.ExternalDestinationID != "canonical_id" {
			t.Errorf("ExternalDestinationID = %q; want canonical_id (canonical wins over legacy JSON key when both present)", s.ExternalDestinationID)
		}
	})
}

// =====================================================================
// ValidationError contract: getters + factory + chain.
// =====================================================================

func TestValidationError_Getters(t *testing.T) {
	t.Parallel()
	e := &ValidationError{
		FieldPath: "delivery_plan.0.retry_budget",
		Msg:       "must be >= 0 (got -3)",
	}
	if e.Error() != "delivery_plan.0.retry_budget: must be >= 0 (got -3)" {
		t.Errorf("Error() = %q; want canonical field: message envelope", e.Error())
	}
	if e.Field() != "delivery_plan.0.retry_budget" {
		t.Errorf("Field() = %q; want delivery_plan.0.retry_budget", e.Field())
	}
	if e.Message() != "must be >= 0 (got -3)" {
		t.Errorf("Message() = %q; want must be >= 0 (got -3)", e.Message())
	}
	if e.Unwrap() != nil {
		t.Errorf("Unwrap() = %v; want nil (no cause set)", e.Unwrap())
	}
}

func TestValidationError_UnwrapChain(t *testing.T) {
	t.Parallel()
	cause := errors.New("social_repo: 5xx")
	wrapped := NewValidationErrorWrapped(
		"delivery_plan.0.external_destination_id",
		"social destination %q rejected by social_repo",
		cause,
	)
	if wrapped.Unwrap() != cause {
		t.Errorf("Unwrap() = %v; want %v", wrapped.Unwrap(), cause)
	}
	if !errors.Is(wrapped, cause) {
		t.Errorf("errors.Is must propagate the chain end to end")
	}
}

func TestValidationField_EmptyOnUntyped(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   error
	}{
		{name: "nil", in: nil},
		{name: "plaintext", in: errors.New("network baseline timeout")},
		{name: "wrapped plaintext (no error chain)", in: errors.New("creatorflow: Resolve atomic: wrapped")},
		{name: "wrapped-from-another-typed-error", in: fmt.Errorf("enqueue: %w", errors.New("social_repo: delivery_plan[N].destination_id unrecognized"))},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ValidationField(tc.in); got != "" {
				t.Errorf("ValidationField(%v) = %q; want \"\"", tc.in, got)
			}
		})
	}
}

func TestValidationField_UnwrapsThroughChain(t *testing.T) {
	t.Parallel()

	inner := &ValidationError{
		FieldPath: "delivery_plan.1.priority",
		Msg:       "must be >= 0",
	}

	// Plaintext %s-style concat does NOT chain Unwrap() — expected
	// to return "". Real callers must use %w to chain.
	plaintextWrapped := errors.New("enqueue prepare: " + inner.Error())
	if got := ValidationField(plaintextWrapped); got != "" {
		t.Errorf("plaintext wrap: ValidationField = %q, want \"\"", got)
	}

	// %w wrap chains Unwrap() — expected to recover the field.
	properlyWrapped := fmt.Errorf("enqueue prepare: %w", inner)
	if got := ValidationField(properlyWrapped); got != "delivery_plan.1.priority" {
		t.Errorf("%%w wrap: ValidationField = %q, want delivery_plan.1.priority", got)
	}
}

// =====================================================================
// helpers
// =====================================================================

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
