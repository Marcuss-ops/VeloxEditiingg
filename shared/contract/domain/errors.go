// Package domain contains leaf-level error contracts shared by HTTP, gRPC,
// workers, persistence, and observability adapters.
package domain

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CodeInvalidPayload              = "invalid_payload"
	CodeDeliveryTargetRequired      = "DELIVERY_TARGET_REQUIRED"
	CodeDeliveryDestinationRejected = "DELIVERY_TARGET_UNAVAILABLE"
	FailureInvalidPayload           = "INVALID_PAYLOAD"
	FailureDeliveryTarget           = "DELIVERY_TARGET_REQUIRED"
	FailureDestinationUnavailable   = "DELIVERY_TARGET_UNAVAILABLE"
	MetricInvalidPayload            = "INVALID_PAYLOAD"
	MetricDeliveryTarget            = "DELIVERY_TARGET_REQUIRED"
	MetricDestinationUnavailable    = "DELIVERY_TARGET_UNAVAILABLE"
	AuditDeliveryPlanRejected       = "DELIVERY_PLAN_REJECTED"
	ComponentEnqueue                = "enqueue"
	ComponentDelivery               = "delivery"
	PhaseValidation                 = "validation"
)

// DomainError is the canonical, machine-readable error contract. Every
// transport and stateful consumer should use these fields rather than parsing
// Error(). Code is the public/API code; FailureCode is persisted on job/task
// failures; MetricCode and AuditAction are deliberately low-cardinality.
type DomainError struct {
	Code        string
	Field       string
	Issue       string
	Retryable   bool
	PublicText  string
	Cause       error
	HTTPStatus  int
	GRPCCode    codes.Code
	FailureCode string
	MetricCode  string
	AuditAction string
	Component   string
	Phase       string
}

func (e *DomainError) Error() string {
	if e == nil {
		return ""
	}
	return e.PublicText
}

func (e *DomainError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// GRPCStatus lets grpc/status.FromError preserve the canonical code and
// details without a second string-based classification table.
func (e *DomainError) GRPCStatus() *status.Status {
	if e == nil {
		return status.New(codes.Unknown, "")
	}
	code := e.GRPCCode
	if code == codes.OK {
		code = codes.Unknown
	}
	return status.New(code, e.PublicText)
}

// HTTPCode returns the canonical HTTP status, defaulting safely to 500.
func (e *DomainError) HTTPCode() int {
	if e == nil || e.HTTPStatus == 0 {
		return http.StatusInternalServerError
	}
	return e.HTTPStatus
}

// RetryDecision is the sole retry projection for a DomainError.
func (e *DomainError) RetryDecision() bool { return e != nil && e.Retryable }

// Classification is the immutable cross-layer projection used by adapters
// that persist failures, emit metrics, or append audit events.
type Classification struct {
	Code        string
	FailureCode string
	MetricCode  string
	AuditAction string
	Retryable   bool
	Component   string
	Phase       string
}

func (e *DomainError) Classification() Classification {
	if e == nil {
		return Classification{}
	}
	return Classification{
		Code: e.Code, FailureCode: e.FailureCode, MetricCode: e.MetricCode,
		AuditAction: e.AuditAction, Retryable: e.Retryable,
		Component: e.Component, Phase: e.Phase,
	}
}

// AsDomainError extracts the canonical error through arbitrary %w wrappers.
func AsDomainError(err error) (*DomainError, bool) {
	if err == nil {
		return nil, false
	}
	var out *DomainError
	if errors.As(err, &out) && out != nil {
		return out, true
	}
	return nil, false
}

// NewInvalidPayload constructs the default non-retryable validation error.
func NewInvalidPayload(field, issue, text string) *DomainError {
	return &DomainError{
		Code: CodeInvalidPayload, Field: field, Issue: issue, Retryable: false,
		PublicText: text, Cause: nil, HTTPStatus: http.StatusUnprocessableEntity,
		GRPCCode: codes.InvalidArgument, FailureCode: FailureInvalidPayload,
		MetricCode: MetricInvalidPayload, AuditAction: AuditDeliveryPlanRejected,
		Component: ComponentEnqueue, Phase: PhaseValidation,
	}
}

// NewDeliveryTargetRequired constructs the canonical missing-destination
// classification. It is kept separate because this is a stable public API
// error rather than a generic invalid payload.
func NewDeliveryTargetRequired(text string, cause error) *DomainError {
	return &DomainError{
		Code: CodeDeliveryTargetRequired, Field: "delivery_plan", Issue: "required",
		Retryable: false, PublicText: text, Cause: cause,
		HTTPStatus: http.StatusUnprocessableEntity, GRPCCode: codes.InvalidArgument,
		FailureCode: FailureDeliveryTarget, MetricCode: MetricDeliveryTarget,
		AuditAction: AuditDeliveryPlanRejected, Component: ComponentEnqueue,
		Phase: PhaseValidation,
	}
}

// ContextCanceled reports whether a domain error is caused by caller
// cancellation. It is intentionally not used to classify delivery-plan
// validation errors, but is useful to retry adapters consuming this contract.
func ContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// GRPCError adapts any error carrying a DomainError into the canonical gRPC
// status. If no domain classification exists, it returns the original error
// so callers can preserve their existing internal-error policy.
func GRPCError(err error) error {
	if err == nil {
		return nil
	}
	if derr, ok := AsDomainError(err); ok {
		return derr.GRPCStatus().Err()
	}
	return err
}

// FailureCodeOf, MetricCodeOf and AuditActionOf are nil-safe projections for
// persistence and observability adapters. They intentionally return empty
// strings for untyped errors; callers must not infer classification from the
// human-readable Error() text.
func FailureCodeOf(err error) string {
	if derr, ok := AsDomainError(err); ok {
		return derr.FailureCode
	}
	return ""
}

func MetricCodeOf(err error) string {
	if derr, ok := AsDomainError(err); ok {
		return derr.MetricCode
	}
	return ""
}

func AuditActionOf(err error) string {
	if derr, ok := AsDomainError(err); ok {
		return derr.AuditAction
	}
	return ""
}

func Retryable(err error) bool {
	if derr, ok := AsDomainError(err); ok {
		return derr.RetryDecision()
	}
	return false
}
