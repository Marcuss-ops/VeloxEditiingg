package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseDeliveryPlanPayloadRejectsInvalidPlans(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload map[string]interface{}
		want    string
	}{
		{
			name: "duplicate",
			payload: map[string]interface{}{
				"delivery_destination_ids": []string{"drive-main", "drive-main"},
			},
			want: "duplicate destination_id",
		},
		{
			name: "disabled",
			payload: map[string]interface{}{
				"delivery_plan": map[string]interface{}{
					"destination_id": "drive-main",
					"enabled":        false,
				},
			},
			want: "disabled",
		},
		{
			// retry_budget=0 is now ALLOWED (openapi.yaml minimum is 0).
			// The parse-layer rejection boundary moved from <=0 to <0.
			// Use -1 so the test keeps pinning the rejection path.
			// Substring "must be >= 0" matches both the legacy
			// fmt.Errorf shape ("delivery_plan[N].retry_budget must
			// be >= 0 (got N)") and the typed-error shape
			// ("delivery_plan[N].retry_budget: must be >= 0 (got N)")
			// — the typed Error() method returns "field: message".
			name: "retry budget",
			payload: map[string]interface{}{
				"delivery_plan": map[string]interface{}{
					"destination_id": "drive-main",
					"retry_budget":   -1,
				},
			},
			want: "must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseDeliveryPlanPayload(tt.payload)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

// TestParseDeliveryPlanPayloadEmitsTypedValidationError pins the
// P0 follow-up: the in-tx parser MUST emit a
// *DeliveryPlanValidationError (not a plaintext fmt.Errorf) on
// retry_budget < 0 so creatorflow.WriteResolverError can classify
// it via DeliveryPlanValidationField(err) and emit 422
// invalid_payload with details[0].path instead of falling through
// to the default 500 resolver_failure branch.
//
// Without this test, a future contributor could silently revert
// the typed-error contract by inlining `fmt.Errorf("delivery_plan[%d].retry_budget must be >= 0 ...")`
// and the regression would only surface as a 500 downgrade on a
// path that's rarely hit in practice (the handler-side pre-check
// shields most requests).
func TestParseDeliveryPlanPayloadEmitsTypedValidationError(t *testing.T) {
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
		t.Fatalf("expected *DeliveryPlanValidationError, got %T (%v)", err, err)
	}
	if derr.Field() != "delivery_plan[0].retry_budget" {
		t.Errorf("Field() = %q, want delivery_plan[0].retry_budget", derr.Field())
	}
	if derr.Message() != "must be >= 0 (got -3)" {
		t.Errorf("Message() = %q, want %q", derr.Message(), "must be >= 0 (got -3)")
	}
	wantErr := "delivery_plan[0].retry_budget: must be >= 0 (got -3)"
	if derr.Error() != wantErr {
		t.Errorf("Error() = %q, want %q", derr.Error(), wantErr)
	}
	if got := DeliveryPlanValidationField(err); got != "delivery_plan[0].retry_budget" {
		t.Errorf("DeliveryPlanValidationField(err) = %q, want delivery_plan[0].retry_budget", got)
	}
}

// TestDeliveryPlanValidationFieldReturnsEmptyOnUntyped pins the
// helper's defensive behavior: a non-DeliveryPlanValidationError
// input (plaintext error, nil, or wrapped typed error from another
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

	inner := &DeliveryPlanValidationError{
		field:   "delivery_plan[1].priority",
		message: "must be >= 0",
	}

	// Plaintext %s-style concat does NOT chain Unwrap() — expected
	// to return "". Real callers must use %w to chain.
	plaintextWrapped := errors.New("enqueue prepare: " + inner.Error())
	if got := DeliveryPlanValidationField(plaintextWrapped); got != "" {
		t.Errorf("plaintext wrap: DeliveryPlanValidationField = %q, want \"\"", got)
	}

	// %w wrap chains Unwrap() — expected to recover the field.
	properlyWrapped := fmt.Errorf("enqueue prepare: %w", inner)
	if got := DeliveryPlanValidationField(properlyWrapped); got != "delivery_plan[1].priority" {
		t.Errorf("%%w wrap: DeliveryPlanValidationField = %q, want delivery_plan[1].priority", got)
	}
}

// TestDeliveryPlanValidationErrorStringFormat pins the typed
// error's Error() method contract: it emits "<field>: <message>"
// (matching the enqueue.validationError twin and the canonical
// "field: message" envelope style). This is a back-compat
// assertion for any caller that pattern-matches the message
// rather than reaching for the typed Field()/Message() getters
// (e.g. legacy logs, integration_test golden assertions).
func TestDeliveryPlanValidationErrorStringFormat(t *testing.T) {
	t.Parallel()

	_, err := parseDeliveryPlanPayload(map[string]interface{}{
		"delivery_plan": map[string]interface{}{
			"destination_id": "drive-main",
			"retry_budget":   -7,
		},
	})
	if err == nil {
		t.Fatalf("expected error on retry_budget=-7, got nil")
	}
	// The "<field>" half of the envelope.
	if !strings.Contains(err.Error(), "delivery_plan[0].retry_budget") {
		t.Errorf("err.Error() = %q, want substring %q", err.Error(), "delivery_plan[0].retry_budget")
	}
	// The "<message>" half — the typed-error shape is
	// "<field>: must be >= 0 (got N)"; the substring "must be >=
	// 0" is index-independent so the test survives any future
	// refactor of the per-entry index wiring.
	if !strings.Contains(err.Error(), "must be >= 0") {
		t.Errorf("err.Error() = %q, want substring %q", err.Error(), "must be >= 0")
	}
	// The "(got N)" debug suffix is preserved verbatim so an
	// operator looking at logs can see the offending value.
	if !strings.Contains(err.Error(), "got -7") {
		t.Errorf("err.Error() = %q, want substring %q", err.Error(), "got -7")
	}
}
