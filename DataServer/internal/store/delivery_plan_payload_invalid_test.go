package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"velox-shared/contract/deliveryplan"
)

// =============================================================================
// Cross-package type alias integrity
// =============================================================================
//
// After the shared/contract/deliveryplan extraction, this file
// asserts the SHAPE of the Bridge between the canonical shared
// package and the historical store-side identifier.
//
// The parser-shape boundary is exhausted in shared/contract/
// deliveryplan/parser_test.go. This file covers the
// store-specific concerns:
//
//   1. The store-side alias `DeliveryPlanValidationError ==
//      deliveryplan.ValidationError` resolves successfully via
//      errors.As + .Field().
//   2. The helper functions DeliveryPlanValidationField and
//      NewDeliveryPlanValidationError re-export the canonical
//      surface unchanged.
//   3. The Error() / Field() / Message() format remains
//      "field: message" so substring-matched call sites keep
//      working.
//   4. parseDeliveryPlanPayload delegates to deliveryplan.Parse and
//      forwards the typed error surface unchanged.
//
// Each test references the SHARED canonical surface to catch a
// future refactor that accidentally shadows the alias.

// TestDeliveryPlanValidationErrorTypeAliasIsCanonical pins that
// store.DeliveryPlanValidationError resolves to the shared
// deliveryplan.ValidationError through the type-alias identity.
// Without this assertion, a future contributor could replace the
// alias with a separate struct, breaking errors.As at the
// cross-package boundary.
func TestDeliveryPlanValidationErrorTypeAliasIsCanonical(t *testing.T) {
	t.Parallel()

	constructed := NewDeliveryPlanValidationError(
		"delivery_plan.0.retry_budget",
		"must be >= 0 (got -3)",
	)
	if constructed == nil {
		t.Fatal("NewDeliveryPlanValidationError returned nil")
	}

	// errors.As must surface the shared canonical type from a
	// pointer to the store-side alias. If the alias ever shadows
	// to a separate struct, this assertion fails.
	var canonical *deliveryplan.ValidationError
	if !errors.As(constructed, &canonical) {
		t.Errorf("errors.As must surface *deliveryplan.ValidationError from *DeliveryPlanValidationError; failed for %T", constructed)
	}
	// Pointer-equality: type aliases resolve at compile time, so
	// (*DeliveryPlanValidationError)(p) and
	// (*deliveryplan.ValidationError)(p) point at the same object.
	if (*DeliveryPlanValidationError)(constructed) != canonical {
		t.Errorf("alias pointer mismatch: store-side and shared-side pointers diverge")
	}
	// Getters reachable through the alias return the canonical
	// values unchanged.
	if canonical.Field() != "delivery_plan.0.retry_budget" {
		t.Errorf("Field() = %q; want delivery_plan.0.retry_budget", canonical.Field())
	}
	if canonical.Message() != "must be >= 0 (got -3)" {
		t.Errorf("Message() = %q; want %q", canonical.Message(), "must be >= 0 (got -3)")
	}
}

// TestDeliveryPlanValidationFieldReturnsEmptyOnUntyped pins the
// store-side helper's defensive behaviour: a non-DeliveryPlan-
// ValidationError input (plaintext error, nil, error from another
// package) MUST NOT panic and MUST return "". Without this row, a
// regression that returns the wrong zero value (e.g. "delivery_plan"
// as a fallback) would silently surface as a 422 with the wrong
// field path — the same class of bug as the original P0 #2
// destination-existence downgrade.
func TestDeliveryPlanValidationFieldReturnsEmptyOnUntyped(t *testing.T) {
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
			if got := DeliveryPlanValidationField(tc.in); got != "" {
				t.Errorf("DeliveryPlanValidationField(%v) = %q, want \"\"", tc.in, got)
			}
		})
	}
}

// TestDeliveryPlanValidationFieldUnwrapsThroughChain pins that
// errors.As + Unwrap() see through fmt.Errorf %w wrapping so a
// caller that does `fmt.Errorf("enqueue layer: %w", storeErr)` still
// recovers the structured field path. Without this row, a future
// refactor that introduces an extra wrapper layer would silently
// downgrade the response to 500.
func TestDeliveryPlanValidationFieldUnwrapsThroughChain(t *testing.T) {
	t.Parallel()

	inner := NewDeliveryPlanValidationError(
		"delivery_plan.1.priority",
		"must be >= 0",
	)

	// Plaintext %s-style concat does NOT chain Unwrap() — expected
	// to return "". Real callers must use %w to chain.
	plaintextWrapped := errors.New("enqueue prepare: " + inner.Error())
	if got := DeliveryPlanValidationField(plaintextWrapped); got != "" {
		t.Errorf("plaintext wrap: DeliveryPlanValidationField = %q, want \"\"", got)
	}

	// %w wrap chains Unwrap() — expected to recover the field.
	properlyWrapped := fmt.Errorf("enqueue prepare: %w", inner)
	if got := DeliveryPlanValidationField(properlyWrapped); got != "delivery_plan.1.priority" {
		t.Errorf("%%w wrap: DeliveryPlanValidationField = %q, want delivery_plan.1.priority", got)
	}
}

// TestDeliveryPlanValidationErrorStringFormat pins the typed
// error's Error() method contract: it emits "<field>: <message>"
// (matching the canonical "field: message" envelope style). This
// is a back-compat assertion for any caller that pattern-matches
// the message rather than reaching for the typed Field()/Message()
// getters (e.g. legacy logs, integration_test golden assertions).
func TestDeliveryPlanValidationErrorStringFormat(t *testing.T) {
	t.Parallel()

	derr := NewDeliveryPlanValidationError(
		"delivery_plan.0.retry_budget",
		fmt.Sprintf("must be >= 0 (got -7)"),
	)
	// The "<field>" half.
	if !strings.Contains(derr.Error(), "delivery_plan.0.retry_budget") {
		t.Errorf("err.Error() = %q, want substring %q", derr.Error(), "delivery_plan.0.retry_budget")
	}
	// The "<message>" half.
	if !strings.Contains(derr.Error(), "must be >= 0") {
		t.Errorf("err.Error() = %q, want substring %q", derr.Error(), "must be >= 0")
	}
	// The "(got N)" debug suffix preserved so an operator looking
	// at logs can see the offending value.
	if !strings.Contains(derr.Error(), "got -7") {
		t.Errorf("err.Error() = %q, want substring %q", derr.Error(), "got -7")
	}
	// Field() and Message() getters match the Error() split.
	if derr.Field() != "delivery_plan.0.retry_budget" {
		t.Errorf("Field() = %q, want delivery_plan.0.retry_budget", derr.Field())
	}
	if derr.Message() != "must be >= 0 (got -7)" {
		t.Errorf("Message() = %q, want %q", derr.Message(), "must be >= 0 (got -7)")
	}
}

// TestParseDeliveryPlanPayloadDelegatesToSharedParser pins the
// final integration: parseDeliveryPlanPayload must surface the
// SAME typed-error field paths as deliveryplan.Parse on the same
// input. Without this row, a regression that reverts the bridge to
// "fmt.Errorf at the store layer" would silently fork the typed
// surface between enqueue and store — defeating the single-source-
// of-truth consolidation.
func TestParseDeliveryPlanPayloadDelegatesToSharedParser(t *testing.T) {
	t.Parallel()

	_, err := parseDeliveryPlanPayload(map[string]interface{}{
		"delivery_plan": map[string]interface{}{
			"destination_id": "drive-main",
			"retry_budget":   -3,
		},
	})
	if err == nil {
		t.Fatalf("expected error on retry_budget=-3, got nil")
	}

	var derr *DeliveryPlanValidationError
	if !errors.As(err, &derr) {
		t.Fatalf("expected *DeliveryPlanValidationError via shared-alias, got %T (%v)", err, err)
	}
	if derr.Field() != "delivery_plan.0.retry_budget" {
		t.Errorf("Field() = %q, want delivery_plan.0.retry_budget", derr.Field())
	}
	if derr.Message() != "must be >= 0 (got -3)" {
		t.Errorf("Message() = %q, want %q", derr.Message(), "must be >= 0 (got -3)")
	}
	wantErr := "delivery_plan.0.retry_budget: must be >= 0 (got -3)"
	if derr.Error() != wantErr {
		t.Errorf("Error() = %q, want %q", derr.Error(), wantErr)
	}
	if got := DeliveryPlanValidationField(err); got != "delivery_plan.0.retry_budget" {
		t.Errorf("DeliveryPlanValidationField(err) = %q, want delivery_plan.0.retry_budget", got)
	}
}
