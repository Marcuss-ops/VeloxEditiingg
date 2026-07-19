package store

// worker_value_decode.go owns the small set of value-coercion helpers
// shared by the worker snapshot file (store_worker_snapshot.go) and the
// runtime/metrics files. They are pure functions with no SQL or
// external dependencies, colocated here so any future new field type
// only needs a single paired edit in this file.

// clampPercent clamps a percent value to the [0, 100] closed interval.
// Used by reconcileWorkerRuntime to defend against malformed progress
// fields coming back from a misbehaving worker.
func clampPercent(v int64) int64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

// defaultString returns asString(v) if non-empty, otherwise fallback.
// Used to fill in default values for snapshot columns.
func defaultString(v any, fallback string) string {
	if s := asString(v); s != "" {
		return s
	}
	return fallback
}

// boolInt converts a Go bool to 0/1 for SQLite INTEGER columns.
func boolInt(v any) int {
	b, _ := v.(bool)
	if b {
		return 1
	}
	return 0
}

// int64Value coerces a Go numeric value to int64. Returns 0 for nil,
// strings, and unrecognised types — never errors, so callers can
// safely use it as part of an INSERT statement.
func int64Value(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	}
	return 0
}

// int64OrDefault returns int64Value(v) if non-zero, otherwise fallback.
func int64OrDefault(v any, fallback int64) int64 {
	if n := int64Value(v); n != 0 {
		return n
	}
	return fallback
}

// floatValue coerces a Go numeric value to float64. Returns 0 for nil
// and unrecognised types — same error-free contract as int64Value.
func floatValue(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

// floatOrMetric returns floatValue(primary) if non-zero, otherwise
// floatValue(fallback). Used by the snapshot upsert to prefer the
// heartbeat's top-level float field over a nested metrics-map entry.
func floatOrMetric(primary, fallback any) float64 {
	if primary != nil {
		if value := floatValue(primary); value != 0 {
			return value
		}
	}
	return floatValue(fallback)
}
