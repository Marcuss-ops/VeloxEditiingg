// Package cache — pool.go: shared copy-buffer pool for streaming I/O.
//
// A4-3 audit fix: the worker repeatedly hashes/copies large artifacts
// (publisher manifest, render-plan finalize, recover tooling) and each call
// site used to allocate a fresh 1 MiB buffer per invocation. This pool makes
// one process-wide set of bounded copy buffers available to every streaming
// path. Buffers are exactly copyBufferSize and are handed out via
// GetCopyBuffer / PutCopyBuffer; callers MUST NOT retain a reference past
// the PutCopyBuffer call and MUST NOT grow or reslice the buffer.
package cache

import "sync"

// copyBufferSize matches the 1 MiB convention already used by the streaming
// hash call sites (streamSHAAndSize, render-plan finalize hashing): well
// above syscall overhead, well below the page cache's effective size.
const copyBufferSize = 1 << 20

var copyBufferPool = sync.Pool{
	New: func() interface{} {
		b := &[copyBufferSize]byte{}
		return b
	},
}

// GetCopyBuffer returns a 1 MiB buffer from the pool. The buffer is returned
// as a pointer so the pool stores one word per entry; slice off the pointer
// for use: `buf := *cache.GetCopyBuffer()`.
func GetCopyBuffer() *[copyBufferSize]byte {
	return copyBufferPool.Get().(*[copyBufferSize]byte)
}

// PutCopyBuffer returns a buffer obtained from GetCopyBuffer to the pool.
// Safe to call with a nil pointer.
func PutCopyBuffer(b *[copyBufferSize]byte) {
	if b != nil {
		copyBufferPool.Put(b)
	}
}
