package alerts

import "testing"

type typedNilErrorMetrics struct{}

func (*typedNilErrorMetrics) RecordAlertEvaluationError(string, string, uint64) {}

func TestErrorMetricsConfiguredRejectsNilAndTypedNil(t *testing.T) {
	if ErrorMetricsConfigured(nil) {
		t.Fatal("nil sink must be unconfigured")
	}
	var typedNil *typedNilErrorMetrics
	if ErrorMetricsConfigured(typedNil) {
		t.Fatal("typed-nil sink must be unconfigured")
	}
	if !ErrorMetricsConfigured(&typedNilErrorMetrics{}) {
		t.Fatal("concrete sink must be configured")
	}
}
