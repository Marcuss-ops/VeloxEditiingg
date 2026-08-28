//go:build !linux

package worker

// prejobDiskFreeBytes returns -1 on platforms where the worker does not have
// a portable statfs implementation in this package. Callers treat -1 as
// "unknown" and continue using the normal runtime disk readiness gate.
func prejobDiskFreeBytes(string) (int64, error) { return -1, nil }
