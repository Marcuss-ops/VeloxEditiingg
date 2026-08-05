package deliveries

import (
	"fmt"
	"testing"

	"velox-shared/contract/domain"
)

func TestDomainErrorDrivesDeliveryClassification(t *testing.T) {
	err := domain.NewInvalidPayload("delivery_plan.0.priority", "out_of_range", "priority must be non-negative")
	wrapped := fmt.Errorf("delivery preflight: %w", err)

	if got := ClassifyError(wrapped); got != ErrorClassPermanent {
		t.Fatalf("ClassifyError=%d, want permanent", got)
	}
	if got := classifyErrorCode(wrapped); got != domain.FailureInvalidPayload {
		t.Fatalf("classifyErrorCode=%q, want %q", got, domain.FailureInvalidPayload)
	}
}
