// Package queue provides job queue management with SQLite persistence
package queue

import "time"

// NowISO returns current time in ISO format.
func NowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// NowUnix returns current time as Unix timestamp.
func NowUnix() int64 {
	return time.Now().Unix()
}
