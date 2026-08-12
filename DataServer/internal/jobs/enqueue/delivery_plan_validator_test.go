// Package enqueue — delivery_plan_validator_test.go.
//
// Pure isolated unit tests for the post-refactor enqueue-layer
// validator. The shape + validation boundaries themselves are
// owned by the canonical:
//
//	shared/contract/deliveryplan/parser_test.go
//
// This file covers the enqueue-LAYER concerns that the canonical
// parser does not cover:
//
//   1. validateDeliveryPlanRequires / validateDeliveryPlanShapeOnly
//      surfaces the SAME field paths +212 envelope errors as the
//      canonical parser (regression guard against bridging drift).
//   2. socialclient pre-flight loop classification on the
//      DestinationValidator interface:
//       * ErrPermanent / ErrAuth → HARD (typed error with wrapped
//         sentinel, errors.Is chain preserved).
//       * ErrTransient / ErrRateLimit → SOFT (log + continue; the
//         delivery runner consumes retry_budget later).
//       * ErrNotConfigured → HARD operational failure (the runner treats
//         provider-not-configured as terminal).
//       * Unknown errors → FAIL CLOSED (propagated, no silent retry).
//   3. Empty external_destination_id → loop skips pre-flight.
//   4. nil validator → rejects Social destinations fail-closed.
//   5. Per-entry call count + argument pinning.

package enqueue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"velox-server/internal/socialclient"
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
			// retry_budget=0 is now ALLOWED per openapi.yaml:
			// SubmitDeliveryPlanEntry.retry_budget.minimum=0.
			name: "retry_budget_zero_explicit_accepted",
			in: map[string]interface{}{
				"delivery_plan": []interface{}{
					map[string]interface{}{"destination_id": "drive-main", "retry_budget": 0},
				},
			},
		},
		{
			// intFromAny coerces unrecognized JSON types (string,
			// bool, nil, …) to 0. Under the relaxed contract (<0),
			// the coerced 0 MUST be accepted just like an explicit
			// numeric 0.
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
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if err := validateDeliveryPlanShapeOnly(c.in); err != nil {
				t.Errorf("want no error; got %v", err)
			}
		})
	}
}

// =====================================================================
// validateDeliveryPlanRequires — every documented rejection mode.
// =====================================================================
//
// After the delegation refactor, validateDeliveryPlanShapeOnly
// routes through deliveryplan.Parse. The error contract is the
// same, so substring pins below are unchanged. REGRESSION-GUARD:
// if a future contributor injects a new wrapping layer that
// re-formats the canonical "<field>: <message>" envelope, every
// row here would fail.

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
			err := validateDeliveryPlanShapeOnly(c.in)
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
// blackbox: an enabled=false with retry_budget<=0 must trip
// retry_budget first (the validator visits enabled → retry_budget
// → priority), pinning the rejection order across the canonical
// parser delegation.
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
	err := validateDeliveryPlanShapeOnly(in)
	if err == nil {
		t.Fatal("want error; got nil")
	}
	// Per the canonical parser's entryFromMap visit order
	// (enabled → retry_budget → priority), enabled=false fails
	// before retry_budget<=0.
	if !strings.Contains(err.Error(), "is disabled") {
		t.Errorf("want 'is disabled' to surface first; got %q", err.Error())
	}
}

// =====================================================================
// stubValidator: a hand-rolled DestinationValidator used to drive
// the per-entry pre-flight loop from unit tests without involving
// the real *socialclient.Client.
// =====================================================================

type stubValidator struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (s *stubValidator) ValidateDestination(ctx context.Context, socialDestID string) error {
	s.mu.Lock()
	s.calls = append(s.calls, socialDestID)
	s.mu.Unlock()
	return s.err
}

func (s *stubValidator) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// =====================================================================
// validateDeliveryPlanRequires — pre-flight loop. Pins the
// hard/soft classification of socialclient sentinels:
//
//	ErrPermanent | ErrAuth               → HARD fail (typed error)
//	ErrTransient | ErrRateLimit           → SOFT pass
//	ErrNotConfigured                      → HARD operational failure
//	unknown error                         → FAIL CLOSED
//	missing external_destination_id      → loop skips pre-flight
// =====================================================================

func TestValidateDeliveryPlanRequires_Preflight(t *testing.T) {
	t.Parallel()

	planWithSocial := map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id":          "velox-social-amish",
				"external_destination_id": "social_dest_amish",
				"platform":                "youtube",
				"retry_budget":            5,
			},
		},
	}
	planWithoutSocial := map[string]interface{}{
		"delivery_plan": []interface{}{
			map[string]interface{}{
				"destination_id": "drive-main",
				"retry_budget":   5,
			},
		},
	}

	t.Run("hard_fail_on_ErrPermanent", func(t *testing.T) {
		t.Parallel()
		// Wrap the sentinel with %w so errors.Is(err, socialclient.ErrPermanent)
		// returns true through the validator's *validationError chain. A
		// bare errors.New(...) with the same text would NOT satisfy
		// errors.Is and the validator would silently classify the
		// failure as soft.
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrPermanent)}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err == nil {
			t.Fatal("want error from hard fail; got nil")
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1", stub.callCount())
		}
		if !strings.Contains(err.Error(), "delivery_plan.0.external_destination_id") {
			t.Errorf("error %q does not contain delivery_plan.0.external_destination_id", err.Error())
		}
		if !strings.Contains(err.Error(), "social_dest_amish") {
			t.Errorf("error %q does not contain social_dest_amish", err.Error())
		}
		// Pin the errors.Is contract through the typed chain.
		if !errors.Is(err, socialclient.ErrPermanent) {
			t.Errorf("errors.Is must propagate ErrPermanent; got %v", err)
		}
		// Pin the errors.As contract so callers can read the
		// structured field path through the canonical surface.
		var verr *validationError
		if !errors.As(err, &verr) {
			t.Errorf("errors.As must surface *validationError; got %T", err)
		} else if verr.Field() != "delivery_plan.0.external_destination_id" {
			t.Errorf("validationError.Field() = %q; want %q", verr.Field(), "delivery_plan.0.external_destination_id")
		}
	})

	t.Run("hard_fail_on_ErrAuth", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrAuth)}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err == nil {
			t.Fatal("want error from hard fail; got nil")
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1", stub.callCount())
		}
		if !strings.Contains(err.Error(), "rejected by social_repo") {
			t.Errorf("error %q does not contain 'rejected by social_repo'", err.Error())
		}
		if !errors.Is(err, socialclient.ErrAuth) {
			t.Errorf("errors.Is must propagate ErrAuth; got %v", err)
		}
		var verr *validationError
		if !errors.As(err, &verr) {
			t.Errorf("errors.As must surface *validationError; got %T", err)
		} else if verr.Field() != "delivery_plan.0.external_destination_id" {
			t.Errorf("validationError.Field() = %q; want %q", verr.Field(), "delivery_plan.0.external_destination_id")
		}
	})

	t.Run("soft_pass_on_ErrTransient", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrTransient)}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err != nil {
			t.Errorf("soft pass on ErrTransient must NOT block enqueue; got %v", err)
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1 (soft path still calls validator)", stub.callCount())
		}
	})

	t.Run("soft_pass_on_ErrRateLimit", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrRateLimit)}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err != nil {
			t.Errorf("soft pass on ErrRateLimit must NOT block enqueue; got %v", err)
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1", stub.callCount())
		}
	})

	t.Run("hard_fail_on_ErrNotConfigured", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrNotConfigured)}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err == nil {
			t.Fatal("ErrNotConfigured must fail preflight; got nil")
		}
		if !strings.Contains(err.Error(), "non-retryable or unclassified error") {
			t.Errorf("error %q does not identify the operational failure", err.Error())
		}
		if !errors.Is(err, socialclient.ErrNotConfigured) {
			t.Errorf("errors.Is must preserve ErrNotConfigured; got %v", err)
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1", stub.callCount())
		}
	})

	t.Run("cancelled_ctx_does_not_block_enqueue", func(t *testing.T) {
		t.Parallel()
		// A cancelled ctx arriving at the validator must NOT block
		// enqueue: the socialclient returns ErrTransient (or
		// equivalent) from a cancelled HTTP request, and the
		// validator classifies ErrTransient as SOFT. The test
		// stubs a transient response to simulate the canceled-ctx
		// behaviour deterministically.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stub := &stubValidator{err: fmt.Errorf("wrapped: %w", socialclient.ErrTransient)}
		if err := validateDeliveryPlanRequires(ctx, planWithSocial, stub); err != nil {
			t.Errorf("cancelled ctx must still soft-pass (not block enqueue); got %v", err)
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1 (ctx-cancel must still flow through the validator)", stub.callCount())
		}
	})

	t.Run("skipped_for_empty_external_destination_id", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: nil}
		err := validateDeliveryPlanRequires(context.Background(), planWithoutSocial, stub)
		if err != nil {
			t.Errorf("legacy drive-only entry: want nil; got %v", err)
		}
		if stub.callCount() != 0 {
			t.Errorf("validator call count = %d; want 0 (pre-flight must skip empty external_destination_id)", stub.callCount())
		}
	})

	t.Run("nil_validator_rejects_social_destination", func(t *testing.T) {
		t.Parallel()
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, nil)
		if !errors.Is(err, socialclient.ErrNotConfigured) {
			t.Errorf("nil validator should fail closed with ErrNotConfigured; got %v", err)
		}
	})

	t.Run("validator_called_per_entry", func(t *testing.T) {
		t.Parallel()
		multiPlan := map[string]interface{}{
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id":          "a",
					"external_destination_id": "social_a",
					"retry_budget":            3,
				},
				map[string]interface{}{
					"destination_id":          "b",
					"external_destination_id": "social_b",
					"retry_budget":            3,
				},
				map[string]interface{}{
					"destination_id":          "c",
					"external_destination_id": "social_c",
					"retry_budget":            3,
				},
			},
		}
		stub := &stubValidator{err: nil}
		if err := validateDeliveryPlanRequires(context.Background(), multiPlan, stub); err != nil {
			t.Errorf("want nil; got %v", err)
		}
		if stub.callCount() != 3 {
			t.Errorf("validator call count = %d; want 3 (one per entry)", stub.callCount())
		}
		want := []string{"social_a", "social_b", "social_c"}
		for i, w := range want {
			if stub.calls[i] != w {
				t.Errorf("call[%d] = %q; want %q", i, stub.calls[i], w)
			}
		}
	})

	t.Run("unknown_error_fails_closed", func(t *testing.T) {
		t.Parallel()
		stub := &stubValidator{err: errors.New("validator contract is unavailable")}
		err := validateDeliveryPlanRequires(context.Background(), planWithSocial, stub)
		if err == nil {
			t.Fatal("unclassified validator error must fail closed; got nil")
		}
		if !strings.Contains(err.Error(), "non-retryable or unclassified error") {
			t.Errorf("error %q does not identify an unclassified preflight error", err.Error())
		}
		if !errors.Is(err, stub.err) {
			t.Errorf("errors.Is must preserve the original validator error; got %v", err)
		}
		if stub.callCount() != 1 {
			t.Errorf("validator call count = %d; want 1", stub.callCount())
		}
	})
}
