// Package store / query_helpers.go
//
// Shared query-construction helpers used across the SQLite writers so the
// timestamp serialization lives in exactly one place. The RFC3339 /
// RFC3339Nano layouts are the canonical DB wire format; new writers should
// call these instead of re-formatting time.Now().UTC() inline.
package store

import "time"

// nowRFC3339 returns the current UTC time formatted for a store timestamp
// column (second precision, RFC3339).
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// nowRFC3339Nano returns the current UTC time formatted at nanosecond
// precision (RFC3339Nano), used by columns that must preserve lease/fence
// ordering boundaries.
func nowRFC3339Nano() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
