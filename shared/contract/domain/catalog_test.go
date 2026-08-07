package domain

import (
	"encoding/json"
	"testing"
)

// Compile-time identity: VeloxError and DomainError are the same type, so the
// unified catalog keeps one name for new code and one source-compatible alias
// for existing callers without any conversion layer.
var (
	_ *VeloxError  = (*DomainError)(nil)
	_ *DomainError = (*VeloxError)(nil)
)

func TestErrorCodeTypedCatalog(t *testing.T) {
	for _, tc := range []struct {
		code ErrorCode
		want string
	}{
		{CodeInvalidPayload, "invalid_payload"},
		{CodeInfrastructure, "INFRASTRUCTURE"},
		{CodeLeaseLost, "LEASE_LOST"},
		{CodeStaleReport, "STALE_REPORT"},
		{CodeNotFound, "NOT_FOUND"},
		{CodeDeliveryTargetRequired, "DELIVERY_TARGET_REQUIRED"},
		{CodeDeliveryDestinationRejected, "DELIVERY_TARGET_UNAVAILABLE"},
	} {
		if string(tc.code) != tc.want {
			t.Fatalf("code %q: string form = %q, want %q", tc.code, string(tc.code), tc.want)
		}
	}

	// ErrorCode is a distinct type: a plain string must not compare equal to a
	// typed code without an explicit conversion (compile-time, not runtime).
	var raw string = "INFRASTRUCTURE"
	if ErrorCode(raw) != CodeInfrastructure {
		t.Fatal("ErrorCode(string) conversion failed")
	}
}

func TestVeloxErrorAliasCarriesCatalog(t *testing.T) {
	err := NewInfrastructure(nil)
	var v *VeloxError = err
	if v.Code != CodeInfrastructure {
		t.Fatalf("VeloxError.Code = %q, want %q", v.Code, CodeInfrastructure)
	}
	if !v.RetryDecision() {
		t.Fatal("VeloxError must preserve the retry projection")
	}
	got, ok := AsDomainError(err)
	if !ok || got != err {
		t.Fatal("AsDomainError must resolve the catalog through the alias")
	}
}

func TestDomainErrorCodeIsTypedAndSerializes(t *testing.T) {
	derr := NewInvalidPayload("delivery_plan.0.retry_budget", "out_of_range", "must be >= 0")
	if derr.Code != CodeInvalidPayload {
		t.Fatalf("Code = %q, want %q", derr.Code, CodeInvalidPayload)
	}
	data, mErr := json.Marshal(derr)
	if mErr != nil {
		t.Fatal(mErr)
	}
	var decoded struct {
		Code string `json:"Code"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Code != string(CodeInvalidPayload) {
		t.Fatalf("JSON code = %q, want %q", decoded.Code, string(CodeInvalidPayload))
	}
}

func TestClassificationCodeProjection(t *testing.T) {
	err := NewDeliveryTargetRequired("explicit destination required", nil)
	cls := err.Classification()
	if cls.Code != string(CodeDeliveryTargetRequired) {
		t.Fatalf("Classification.Code = %q, want %q", cls.Code, string(CodeDeliveryTargetRequired))
	}
	if cls.FailureCode != FailureDeliveryTarget || cls.MetricCode != MetricDeliveryTarget {
		t.Fatalf("Classification failure/metric projections mismatch: %+v", cls)
	}
}
