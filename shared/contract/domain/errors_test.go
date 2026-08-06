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
	mapping := MapError(err)
	if mapping.HTTPStatus != http.StatusUnprocessableEntity || mapping.GRPCCode != codes.InvalidArgument || mapping.Retryable {
		t.Fatalf("unexpected unified mapping: %+v", mapping)
	}
}

func TestDomainErrorRejectsTextualClassification(t *testing.T) {
	mapping := MapError(errors.New("database is closed"))
	if mapping.HTTPStatus != http.StatusInternalServerError || mapping.Retryable {
		t.Fatalf("untyped message must not be classified by text: %+v", mapping)
	}
}

func TestDomainErrorClassifiedProjections(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *DomainError
		want ErrorMapping
	}{
		{"infrastructure", NewInfrastructure(errors.New("db")), ErrorMapping{HTTPStatus: http.StatusServiceUnavailable, GRPCCode: codes.Unavailable, FailureCode: "INFRASTRUCTURE", MetricCode: "INFRASTRUCTURE", Retryable: true}},
		{"lease lost", NewLeaseLost(errors.New("cas")), ErrorMapping{HTTPStatus: http.StatusConflict, GRPCCode: codes.Aborted, FailureCode: "LEASE_LOST", MetricCode: "LEASE_LOST", Retryable: true}},
		{"stale report", NewStaleReport(errors.New("old")), ErrorMapping{HTTPStatus: http.StatusConflict, GRPCCode: codes.Aborted, FailureCode: "STALE_REPORT", MetricCode: "STALE_REPORT", Retryable: false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := MapError(tc.err); got != tc.want {
				t.Fatalf("MapError=%+v, want %+v", got, tc.want)
			}
		})
	}
}
