package domain

import (
	"errors"
	"net/http"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestDomainErrorProjectsUniformly(t *testing.T) {
	cause := errors.New("validation cause")
	err := NewInvalidPayload("delivery_plan.0.retry_budget", "out_of_range", "must be >= 0")
	err.Cause = cause

	if !errors.Is(err, cause) {
		t.Fatal("domain error must preserve cause")
	}
	if err.HTTPCode() != http.StatusUnprocessableEntity {
		t.Fatalf("HTTP code=%d", err.HTTPCode())
	}
	if err.GRPCStatus().Code() != codes.InvalidArgument {
		t.Fatalf("gRPC code=%s", err.GRPCStatus().Code())
	}
	if FailureCodeOf(err) != FailureInvalidPayload {
		t.Fatalf("failure code=%q", FailureCodeOf(err))
	}
	if MetricCodeOf(err) != MetricInvalidPayload {
		t.Fatalf("metric code=%q", MetricCodeOf(err))
	}
	if AuditActionOf(err) != AuditDeliveryPlanRejected {
		t.Fatalf("audit action=%q", AuditActionOf(err))
	}
	if Retryable(err) {
		t.Fatal("validation error must not be retryable")
	}
}
