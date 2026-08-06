package config

import "strings"

// dataDirFromRaw returns VELOX_DATA_DIR as configured by the operator, or empty
// if the variable is not set. Callers must apply their own defaulting.
func dataDirFromRaw(raw RawConfig) string {
	return strings.TrimSpace(raw.Get("VELOX_DATA_DIR"))
}
