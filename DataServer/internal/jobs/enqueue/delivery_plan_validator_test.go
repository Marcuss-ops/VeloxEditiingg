// Package enqueue — delivery_plan_validator_test.go.
//
// Pure isolated unit tests for delivery_plan_validator.go. No DB,
// no migrations, no fixtures. The closest integration cousin is
// TestPrepareJobAndTask_RejectsMissingDeliveryPlan
// (enqueue_delivery_plan_test.go) which exercises the SAME validator
// indirectly through PrepareJobAndTask. This file drives the
// validator DIRECTLY so each rejection mode is observable in
// isolation — no scene normalization, no atomic creator wiring,
// no idempotency noise.
//
// The validator is the canonical-purity preflight (Step 4/8) so
// each rejection code below is a real production boundary; tests
// pin both the field path and the substring so downstream callers
// can rely on errors.Is / strings.Contains checks.
package enqueue

import (
	"strings"
	"testing"
)

// =====================================================================
// validateDeliveryPlanRequires — golden paths.
// =====================================================================

func TestValidateDeliveryPlanRequires_HappyPaths(t *testing.T) {
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
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if err := validateDeliveryPlanRequires(c.in); err != nil {
				t.Errorf("want no error; got %v", err)
			}
		})
	}
}

// =====================================================================
// validateDeliveryPlanRequires — every documented rejection mode.
// =====================================================================

func TestValidateDeliveryPlanRequires_RejectPaths(t *testing.T) {
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
			wantSub:   "is required for canonical-purity enqueue",
		},
		{
			name:      "empty_payload",
			in:        map[string]interface{}{},
			wantField: "delivery_plan",
			wantSub:   "is required for canonical-purity enqueue",
		},
		{
			name: "empty_array",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{},
			},
			wantField: "delivery_plan",
			wantSub:   "is required for canonical-purity enqueue",
		},
		{
			name: "non_object_array_entry",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{"drive-main"},
			},
			wantField: "delivery_plan[0]",
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
			wantField: "delivery_plan[0].destination_id",
			wantSub:   "is required",
		},
		{
			name: "empty_destination_id",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "   ", "retry_budget": 3},
				},
			},
			wantField: "delivery_plan[0].destination_id",
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
			wantField: "delivery_plan[1].destination_id",
			wantSub:   "duplicate",
		},
		{
			name: "retry_budget_zero",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 0},
				},
			},
			wantField: "delivery_plan[0].retry_budget",
			wantSub:   "must be > 0",
		},
		{
			name: "retry_budget_negative",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": -3},
				},
			},
			wantField: "delivery_plan[0].retry_budget",
			wantSub:   "must be > 0",
		},
		{
			name: "retry_budget_string_invalid",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": "abc"},
				},
			},
			wantField: "delivery_plan[0].retry_budget",
			wantSub:   "must be > 0",
		},
		{
			name: "disabled_entry",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "enabled": false},
				},
			},
			wantField: "delivery_plan[0]",
			wantSub:   "is disabled",
		},
		{
			name: "negative_priority",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": -1},
				},
			},
			wantField: "delivery_plan[0].priority",
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
			err := validateDeliveryPlanRequires(c.in)
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
// extractLegacyDestinationIDs: documented resolver order.
//
// The provider walks delivery_destination_ids → destination_ids → then
// single-key delivery_destination_id → destination_id. Mirrors the
// comment block at the top of delivery_plan_validator.go.
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
// intFromAny: every numeric type covered by extractDeliveryPlanShape
// must parse correctly. Non-numeric inputs collapse to 0 (which then
// fails the retry_budget > 0 gate downstream — pinned here as a
// pure contract test).
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
		{"bool_true_collapses_to_zero", true, 0}, // bool is not numeric
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
// boolFromAny: with explicit overrides and default fallback.
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
// shapeFromMap: integration of intFromAny + boolFromAny + firstStringField
// applied to a deliveryPlanShape. Pins the parser-level invariants of
// extractDeliveryPlanShape on the per-entry map.
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

// =====================================================================
// blackbox: an enabled=false with retry_budget<=0 must trip retry_budget
// first (the validator visits retry_budget after enabled), pinning the
// rejection order so callers can reason about which error surfaced.
// =====================================================================

func TestValidateDeliveryPlanRequires_DisabledFalsyRetryBudgetTripOrder(t *testing.T) {
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
	err := validateDeliveryPlanRequires(in)
	if err == nil {
		t.Fatal("want error; got nil")
	}
	// Per the validator's loop order (id → dup → enabled → retry → priority),
	// enabled=false fails before retry_budget<=0.
	if !strings.Contains(err.Error(), "is disabled") {
		t.Errorf("want 'is disabled' to surface first; got %q", err.Error())
	}
}
