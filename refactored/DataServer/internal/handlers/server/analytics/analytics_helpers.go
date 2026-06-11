package analytics

import "velox-shared/payload"

func asStr(v any) string {
	return payload.AsString(v)
}

func asFloatFromAny(v any) float64 {
	return payload.AsFloat(v)
}

func extractFloat(data map[string]any, key string) float64 {
	return payload.FloatValue(data, key)
}

func parseIntDef(s string, def int) int {
	return payload.ParseIntDef(s, def)
}
