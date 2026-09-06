// Package cache — pool.go: shared copy-buffer pool for streaming I/O.
//
// A4-3 follow-up: the canonical pool now lives in velox-shared/iopool so
// the master (DataServer) and the worker agent share one implementation.
// This wrapper is DEPRECATED and kept only so existing call sites keep
// compiling during the migration; new code must import velox-shared/iopool
// directly.
package cache

import "velox-shared/iopool"

// CopyBufferSize matches the shared pool's 1 MiB convention.
//
// Deprecated: use iopool.Size.
const CopyBufferSize = iopool.Size

// GetCopyBuffer returns a 1 MiB buffer from the shared pool.
//
// Deprecated: use iopool.Get.
func GetCopyBuffer() *[iopool.Size]byte {
	return iopool.Get()
}

// PutCopyBuffer returns a buffer obtained from GetCopyBuffer to the pool.
//
// Deprecated: use iopool.Put.
func PutCopyBuffer(buf *[iopool.Size]byte) {
	iopool.Put(buf)
}
