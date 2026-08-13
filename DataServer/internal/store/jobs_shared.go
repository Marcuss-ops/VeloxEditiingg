package store

import "time"

// nowStrISO is the canonical UTC now-string for INSERT/UPDATE timestamps.
// Always UTC, RFC3339, shared across all job repository backends.
func nowStrISO() string {
	return nowRFC3339()
}

// parseTimeOrZero converts an RFC3339 string to time.Time, returning a
// zero time.Time for empty/invalid inputs. Shared across job repository
// backends.
func parseTimeOrZero(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
