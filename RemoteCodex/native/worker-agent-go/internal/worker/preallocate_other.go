//go:build !linux

package worker

import "os"

// preallocateFile is the portable fallback used on non-Linux platforms: a
// sparse truncate reserves the logical size without allocating physical
// blocks. Linux uses fallocate(2) instead (preallocate_linux.go).
func preallocateFile(f *os.File, size int64) error {
	return f.Truncate(size)
}
