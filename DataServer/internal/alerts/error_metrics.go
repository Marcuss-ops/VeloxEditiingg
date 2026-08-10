package alerts

import "reflect"

// ErrorMetrics receives low-cardinality alert-engine error observations.
// Implementations must never use worker IDs or raw error strings as labels.
type ErrorMetrics interface {
	RecordAlertEvaluationError(engine, category string, count uint64)
}

// ErrorMetricsConfigured reports whether a metric sink is usable, including
// interface values containing a typed-nil pointer.
func ErrorMetricsConfigured(sink ErrorMetrics) bool {
	if sink == nil {
		return false
	}
	value := reflect.ValueOf(sink)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return !value.IsNil()
	default:
		return true
	}
}
